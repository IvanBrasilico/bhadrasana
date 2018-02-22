"""Módulo responsável pelas funções que aplicam os filtros/parâmetros
de risco cadastrados nos dados. Utiliza pandas para realizar filtragem
"""
import csv
import json
import os
import shutil
from collections import defaultdict

import pandas as pd

from ajna_commons.flask.log import logger
from sentinela.conf import ENCODE, tmpdir
from sentinela.models.models import (Filtro, PadraoRisco, ParametroRisco,
                                     ValorParametro)
from sentinela.utils.csv_handlers import (muda_titulos_lista, sanitizar,
                                          sanitizar_lista, unicode_sanitizar)


def equality(listaoriginal, nomecampo, listavalores):
    df = pd.DataFrame(listaoriginal[1:], columns=listaoriginal[0])
    df = df[df[nomecampo].isin(listavalores)]
    return df.values.tolist()


def startswith(listaoriginal, nomecampo, listavalores):
    df = pd.DataFrame(listaoriginal[1:], columns=listaoriginal[0])
    result = []
    for valor in listavalores:
        df_filtered = df[df[nomecampo].str.startswith(valor, na=False)]
        result.extend(df_filtered.values.tolist())
    return result


def contains(listaoriginal, nomecampo, listavalores):
    df = pd.DataFrame(listaoriginal[1:], columns=listaoriginal[0])
    result = []
    for valor in listavalores:
        df_filtered = df[df[nomecampo].str.contains(valor, na=False)]
        result.extend(df_filtered.values.tolist())
    return result


filter_functions = {
    Filtro.igual: equality,
    Filtro.comeca_com: startswith,
    Filtro.contem: contains
}


class GerenteRisco():
    """Classe que aplica parâmetros de risco e/ou junção em listas

    São fornecidos também metodos para facilitar o de/para entre
    o Banco de Dados e arquivos csv de parâmetros, para permitir que
    Usuário importe e exporte parâmetros de risco.

    Args:
        pre_processers: dict de funções para pré-processar lista. Função
        DEVE esperar uma lista como primeiro parâmetro

        pre_processers_params: se houver, será passado para função com mesmo
        'key' do pre_processer como kargs.

        Ex:

        gerente.pre_processers['mudatitulo'] = muda_titulos

        gerente.pre_processers_params['mudatitulo'] = {'de_para_dict': {}}

        Os atributos abaixo NÂO devem ser acessados diretamente. A classe
        os gerencia internamente.

        riscosativos: dict descreve "riscos" (compilado dos ParametrosRisco)

        padraorisco: PadraoRisco ativo
    """

    def __init__(self):
        self.pre_processers = {}
        self.pre_processers_params = {}
        self._riscosativos = {}
        self._padraorisco = None

    def importa_base(self, csv_folder, baseid, data, filename, remove=False):
        """Copia base para dest_path, processando se necessário
        Aceita arquivos .zip contendo arquivos sch
        e arquivos csv únicos
        No caso de CSV, retorna erro caso títulos não batam com importação
        anterior para a mesma "BASE" (dest_path) para evitar erros do usuário

        Args:
            csv_folder: destino do(s) arquivo(s) gerados
            base_id: id da base, será usada para identificar uma subpasta
            data: data do período inicial da base/extração. Será usada
            para gerar subpasta no formato YYYY/MM/DD
            filename: caminho completo do arquivo da base de origem
             (fonte externa/extração)
            remove: excluir o arquivo temporário após processamento
        Returns
            a tuple or a list of tuples. First items are created csvs
        """
        dest_path = os.path.join(csv_folder, baseid,
                                 data[:4], data[5:7], data[8:10])
        if os.path.exists(dest_path):
            raise FileExistsError(
                'Já houve importação de base para os parâmetros informados')
        else:
            os.makedirs(dest_path)
        if '.zip' in filename or os.path.isdir(filename):
            result = sch_processing(filename,
                                    dest_path=dest_path)
        else:
            # No caso de CSV, retornar erro caso títulos não batam com importação
            # anterior
            # Para sch zipados e lista de csv zipados, esta verificação é mais
            # custosa, mas também precisa ser implementada
            cabecalhos_antigos = self.get_headers_base(baseid, csv_folder)
            diferenca_cabecalhos = set()
            if cabecalhos_antigos:
                with open(filename, 'r', newline='') as file:
                    reader = csv.reader(file)
                    cabecalhos_novos = set(next(reader))
                    diferenca_cabecalhos = cabecalhos_novos ^ cabecalhos_antigos
                    if diferenca_cabecalhos:
                        raise ValueError('Erro na importação! ' +
                                         'Há base anterior com cabeçalhos diferentes. ' +
                                         str(diferenca_cabecalhos))
            dest_filename = os.path.join(dest_path,
                                         os.path.basename(filename))
            shutil.copyfile(filename,
                            os.path.join(dest_filename))
            result = [(dest_filename, 'single csv')]
            # result = csv_processing(tempfile, dest_path=dest_path)
        if not os.path.isdir(filename) and remove:
            os.remove(filename)
        return result

    def set_padraorisco(self, padraorisco):
        """Vincula o Gerente a um objeto padraoriscoOriginal
        Atenção: TODOS os parâmetros de risco ativos no Gerente serão
        zerados!!!
        TODOS os parâmetros de risco vinculados à padraoriscoOriginal serão
        adicionados aos riscos ativos!!!
        """
        self._padraorisco = padraorisco
        self._riscosativos = {}
        for parametro in self._padraorisco.parametros:
            self.add_risco(parametro)

    def cria_padraorisco(self, nomepadraorisco, session):
        padraorisco = session.query(PadraoRisco).filter(
            PadraoRisco.nome == nomepadraorisco).first()
        if not padraorisco:
            padraorisco = PadraoRisco(nomepadraorisco)
        self.set_padraorisco(padraorisco)

    def add_risco(self, parametrorisco, session=None):
        """Configura os parametros de risco ativos"""
        dict_filtros = defaultdict(list)
        for valor in parametrorisco.valores:
            dict_filtros[valor.tipo_filtro].append(valor.valor)
        self._riscosativos[parametrorisco.nome_campo] = dict_filtros
        if session and self._padraorisco:
            self._padraorisco.parametros.append(parametrorisco)
            session.merge(self._padraorisco)
            session.commit()

    def remove_risco(self, parametrorisco, session=None):
        """Configura os parametros de risco ativos"""
        self._riscosativos.pop(parametrorisco.nome_campo, None)
        if session and self._padraorisco:
            self._padraorisco.parametros.remove(parametrorisco)
            session.merge(self._padraorisco)
            session.commit()

    def clear_risco(self, session=None):
        """Zera os parametros de risco ativos"""
        self._riscosativos = {}
        if session and self._padraorisco:
            self._padraorisco.parametros.clear()
            session.merge(self._padraorisco)
            session.commit()

    def checa_depara(self, base):
        """Se tiver depara na base, adiciona aos pre_processers
        Os pre_processers são usados ao recuperar headers, importar,
        aplicar_risco, entre outras ações"""
        if base.deparas:
            de_para_dict = {depara.titulo_ant: depara.titulo_novo
                            for depara in base.deparas}
            self.pre_processers['mudartitulos'] = muda_titulos_lista
            self.pre_processers_params['mudartitulos'] = {
                'de_para_dict': de_para_dict, 'make_copy': False}

    def ativa_sanitizacao(self, norm_function=unicode_sanitizar):
        """Inclui função de sanitização nos pre_processers
        Os pre_processers são usados ao recuperar headers, importar,
        aplicar_risco, entre outras ações"""
        self.pre_processers['sanitizar'] = sanitizar_lista
        self.pre_processers_params['sanitizar'] = {
            'norm_function': norm_function}

    def load_csv(self, arquivo):
        with open(arquivo, 'r', encoding=ENCODE, newline='') as arq:
            reader = csv.reader(arq)
            lista = [linha for linha in reader]
        return lista

    def save_csv(self, lista, arquivo):
        try:
            os.remove(arquivo)  # Remove resultado antigo se houver
        except IOError:
            pass
        with open(arquivo, 'w', encoding=ENCODE, newline='') as csv_out:
            writer = csv.writer(csv_out)
            writer.writerows(lista)

    def strip_lines(self, lista):
        # Precaução: retirar espaços mortos de todo item
        # da lista para evitar erros de comparação
        for ind, linha in enumerate(lista):
            linha_striped = []
            for item in linha:
                if isinstance(item, str):
                    item = item.strip()
                linha_striped.append(item)
            lista[ind] = linha_striped
        return lista

    def pre_processa(self, lista):
        for key in self.pre_processers:
            lista = self.pre_processers[key](lista,
                                             **self.pre_processers_params[key])
        return lista

    def pre_processa_arquivos(self, lista_arquivos):
        print(lista_arquivos)
        if len(lista_arquivos) > 0:
            if (isinstance(lista_arquivos[0], list) or
                    isinstance(lista_arquivos[0], tuple)):
                alista = [linha[0] for linha in lista_arquivos]
        print(alista)
        for filename in alista:
            lista = self.load_csv(filename)
            lista = self.pre_processa(lista)
            self.save_csv(lista, filename)

    def aplica_risco(self, lista=None, arquivo=None, parametros_ativos=None):
        """Compara a linha de título da lista recebida com a lista de nomes
        de campo que possuem parâmetros de risco ativos. Após, chama para cada
        campo encontrado a função de filtragem. Somente um dos parâmetros
        precisa ser passado. Caso na lista do pipeline estejam cadastradas
        funções de pré-processamento, serão aplicadas.

        Args:
            lista (list): Lista a ser filtrada, primeira linha deve conter os
            nomes dos campos idênticos aos definidos no nome_campo
            do parâmetro de risco cadastrado.
            OU
            arquivo (str): Arquivo csv de onde carregar a lista a ser filtrada

        Returns:
            lista contendo os campos filtrados. 1ª linha com nomes de campo

        Obs: para um arquivo, quando a base for constituída de vários arquivos,
         utilizar :func:`aplica_juncao`

        """
        mensagem = 'Arquivo não fornecido!'
        if arquivo:
            mensagem = 'Lista não fornecida!'
            lista = self.load_csv(arquivo)
        if not lista:
            raise AttributeError('Erro! ' + mensagem)
        # Aplicar pre_processers
        logger.debug(lista[:10])
        lista = self.strip_lines(lista)
        lista = self.pre_processa(lista)
        logger.debug(lista[:10])
        headers = set(lista[0])
        # print('Ativos:', parametros_ativos)
        if parametros_ativos:
            riscos = set(parametros_ativos)
        else:
            riscos = set(list(self._riscosativos.keys()))
        aplicar = headers & riscos   # INTERSECTION OF SETS
        logger.debug(aplicar)
        result = []
        result.append(lista[0])
        # print(aplicar)
        # print(self._riscosativos)
        for campo in aplicar:
            dict_filtros = self._riscosativos.get(campo)
            for tipo_filtro, lista_filtros in dict_filtros.items():
                filter_function = filter_functions.get(tipo_filtro)
                if filter_function is None:
                    raise NotImplementedError('Função de filtro' +
                                              tipo_filtro.name +
                                              ' não implementada.')
                result_filter = filter_function(lista, campo, lista_filtros)
                # print('result_filter', result_filter)
                for linha in result_filter:
                    result.append(linha)
        return result

    def parametro_tocsv(self, campo, path=tmpdir, dbsession=None):
        """Salva os valores do parâmetro de risco em um arquivo csv
        no formato 'valor', 'tipo_filtro'"""
        lista = []
        lista.append(('valor', 'tipo_filtro'))
        dict_filtros = self._riscosativos.get(campo)
        if dbsession:
            risco = dbsession.query(ParametroRisco).filter(
                ParametroRisco.id == campo).first()
            risco_all = dbsession.query(ValorParametro).filter(
                ValorParametro.risco_id == campo).all()
            if risco:
                campo = risco.nome_campo
            if risco_all is None and dict_filtros is None:
                return False
            for valor in risco_all:
                filtro = str(valor.tipo_filtro)
                lista.append((valor.valor, filtro[filtro.find('.') + 1:]))
        elif dict_filtros:
            for tipo_filtro, lista_filtros in dict_filtros.items():
                for valor in lista_filtros:
                    lista.append((valor, tipo_filtro.name))
        filename = os.path.join(path, campo + '.csv')
        with open(filename,
                  'w', encoding=ENCODE, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(lista)
        return filename

    def parametros_tocsv(self, path=tmpdir):
        """Salva os parâmetros adicionados a um gerente em um arquivo csv
        Ver também: :py:func:`parametros_fromcsv`
        """
        for campo in self._riscosativos:
            self.parametro_tocsv(campo, path=path)

    def parametros_fromcsv(self, campo, session=None, padraorisco=None,
                           lista=None, path=tmpdir):
        """
        Abre um arquivo csv, recupera parâmetros configurados nele,
        adiciona à configuração do gerente e **também adiciona ao Banco de
        Dados ativo** caso não existam nele ainda. Para isso é preciso
        passar a session como parâmetro, senão cria apenas na memória
        Pode receber uma lista no lugar de um arquivo csv (como implementado
        em import_named_csv)

        Ver também:

        :py:func:`parametros_tocsv`
        :py:func:`import_named_csv`

        Args:
            campo: nome do campo a ser filtrado e deve ser também
            o nome do arquivo .csv
            session: a sessão com o banco de dados
            lista: passar uma lista pré-prenchida para usar a função com outros
            tipos de fontes/arquivos. Se passada uma lista, função não
            abrirá arquivo .csv, usará os valores da função

        O arquivo .csv ou a lista DEVEM estar no formato valor, tipo_filtro
        """
        if not lista:
            with open(os.path.join(path, campo + '.csv'),
                      'r', encoding=ENCODE, newline='') as f:
                reader = csv.reader(f)
                cabecalho = next(reader)
                lista = [linha for linha in reader]
        if session:
            parametro = session.query(ParametroRisco).filter(
                ParametroRisco.nome_campo == campo).first()
            if not parametro:
                parametro = ParametroRisco(campo, padraorisco=padraorisco)
                session.add(parametro)
                session.commit()
            for linha in lista:
                if parametro.id:
                    if len(linha) == 1:
                        ltipofiltro = Filtro.igual
                    else:
                        ltipofiltro = Filtro[linha[1].strip()]
                    valor = session.query(ValorParametro).filter(
                        ValorParametro.valor == linha[0],
                        ValorParametro.risco_id == parametro.id).first()
                    if not valor:
                        valor = ValorParametro(linha[0].strip(),
                                               ltipofiltro)
                        valor.risco_id = parametro.id
                        session.add(valor)
                    else:
                        valor.tipo_filtro = ltipofiltro
                        valor.risco_id = parametro.id
                        session.merge(valor)
                    parametro.valores.append(valor)
            session.merge(parametro)
            session.commit()
            self.add_risco(parametro)
        else:
            dict_filtros = defaultdict(list)
            for linha in lista:
                if len(linha) == 1:
                    ltipofiltro = Filtro.igual
                else:
                    ltipofiltro = Filtro[linha[1].strip()]
                dict_filtros[ltipofiltro].append(linha[0])
            self._riscosativos[campo] = dict_filtros

    def import_named_csv(self, arquivo, session=None, padraorisco=None,
                         filtro=Filtro.igual, tolist=False):
        """Abre um arquivo csv, cada coluna sendo um filtro.
        A primeira linha contém o campo a ser filtrado e as linhas
        seguintes os valores do filtro. Cria filtros na memória, e no
        Banco de Dados caso session seja informada.

        Args:
            arquivo: Nome e caminho do arquivo .csv
            session: sessão ativa com BD
            filtro: Tipo de filtro a ser assumido como padrão

        """
        with open(arquivo, 'r', encoding=ENCODE, newline='') as f:
            reader = csv.reader(f)
            cabecalho = next(reader)
            listas = defaultdict(list)
            for linha in reader:
                ind = 0
                for coluna in linha:
                    coluna = coluna.strip()
                    if coluna:
                        listas[cabecalho[ind].strip()].append(
                            [coluna, filtro.name])
                    ind += 1
        for key, value in listas.items():
            self.parametros_fromcsv(key, session, padraorisco, value)
        if tolist:
            return cabecalho

    def get_headers_base(self, baseorigemid, path):
        """Busca última base disponível no diretório de CSVs e
        traz todos os headers"""
        cabecalhos = []
        cabecalhos_nao_repetidos = set()
        caminho = os.path.join(path, str(baseorigemid))
        if not os.path.isdir(caminho):
            return set()
        ultimo_ano = sorted(os.listdir(caminho))
        logger.debug(ultimo_ano)
        if len(ultimo_ano) == 0:
            raise ValueError('Não há nenhuma base do tipo desejado '
                             'para consulta')
        ultimo_ano = ultimo_ano[-1]
        caminho = os.path.join(caminho, ultimo_ano)
        ultimo_mes = sorted(os.listdir(caminho))[-1]
        caminho = os.path.join(caminho, ultimo_mes)
        ultimo_dia = sorted(os.listdir(caminho))[-1]
        caminho = os.path.join(caminho, ultimo_dia)
        for arquivo in os.listdir(caminho):
            with open(os.path.join(caminho, arquivo),
                      'r', encoding=ENCODE, newline='') as f:
                reader = csv.reader(f)
                cabecalho = next(reader)
                cabecalhos.extend(cabecalho)
        for word in cabecalhos:
            new_word = sanitizar(word, norm_function=unicode_sanitizar)
            cabecalhos_nao_repetidos.add(new_word)
        return cabecalhos_nao_repetidos

    def aplica_juncao(self, visao, path=tmpdir, filtrar=False,
                      parametros_ativos=None):
        numero_juncoes = len(visao.tabelas)
        tabela = visao.tabelas[numero_juncoes - 1]
        filhofilename = os.path.join(path, tabela.csv)
        dffilho = pd.read_csv(filhofilename, encoding=ENCODE,
                              dtype=str)
        if hasattr(tabela, 'type'):
            how = tabela.type
        else:
            how = 'inner'
        # print(tabela.csv, tabela.estrangeiro, tabela.primario)
        # A primeira precisa ser "pulada", sempre é a junção 2 tabelas
        # de cada vez. Se numero_juncoes for >2, entrará aqui fazendo
        # a junção em cadeia desde o último até o primeiro filho
        for r in range(numero_juncoes - 2, 0, -1):
            paifilhofilename = os.path.join(path, visao.tabelas[r].csv)
            dfpaifilho = pd.read_csv(paifilhofilename, encoding=ENCODE,
                                     dtype=str)
            # print(tabela.csv, tabela.estrangeiro, tabela.primario)
            dffilho = dfpaifilho.merge(dffilho, how=how,
                                       left_on=tabela.primario,
                                       right_on=tabela.estrangeiro)
            tabela = visao.tabelas[r]
            paifilhofilename = os.path.join(path, tabela.csv)
            if hasattr(tabela, 'type'):
                how = tabela.type
            else:
                how = 'inner'
        csv_pai = visao.tabelas[0].csv
        paifilename = os.path.join(path, csv_pai)
        dfpai = pd.read_csv(paifilename, encoding=ENCODE, dtype=str)
        dfpai = dfpai.merge(dffilho, how=how,
                            left_on=tabela.primario,
                            right_on=tabela.estrangeiro)
        if visao.colunas:
            colunas = [coluna.nome for coluna in visao.colunas]
            result_df = dfpai[colunas]
            result_list = [colunas]
        else:
            result_df = dfpai
            result_list = [result_df.columns.tolist()]
        result_list.extend(result_df.values.tolist())
        if filtrar:
            return self.aplica_risco(result_list,
                                     parametros_ativos=parametros_ativos)
        return result_list

    @classmethod
    def csv_to_mongo(cls, db, tabela, path=None, arquivo=None, unique=[]):
        """Reads a csv file and inserts all contents into a MongoDB collection
        Creates collection if it not exists
        Args:
            db: MongoDBClient connection with database setted
            tabela: nome da coleção do MongoDB a utilizar (cria se não existe)
            path: caminho do(s) arquivo(s) csv OU
            arquivo: caminho e nome do arquivo csv
            unique: lista de campos que terão indice único (e 
            não serão reinseridos) - TODO: unique field on mongo archive
        """
        if path is None and arquivo is None:
            raise AtributeError('Nome ou caminho do(s) arquivo(s) deve ser'
                                'obrigatoriamente informado')
        if arquivo:
            lista_arquivos = [os.path.basename(arquivo)]
            path = os.path.dirname(arquivo)
        else:
            lista_arquivos = os.listdir(path)
        for arquivo in lista_arquivos:
            df = pd.read_csv(os.path.join(path, arquivo))
            data_json = json.loads(df.to_json(orient='records'))
            if tabela not in db.collection_names():
                db.create_collection(tabela)
            db[tabela].insert(data_json)
    
    def load_mongo(self, db, base):
        mongo_list = db[base.nome].find()
        result = [mongo_list[0].keys()]
        for linha in mongo_list:
            result.append(linha.values())

        return result
