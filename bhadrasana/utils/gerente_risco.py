"""Métodos e classes para aplicação de risco (filtros e junções).

Módulo responsável pelas funções que aplicam os filtros/parâmetros
de risco cadastrados nos dados.

Utiliza pandas para realizar filtragem de arquivos csv.
Utiliza MongoDB para arquivar diversos arquivos em base única
e Mongo agreggation framework para consultas.

"""
import csv
import json
import os
import shutil
from collections import OrderedDict, defaultdict

import pandas as pd
import pymongo

from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import (sanitizar, sanitizar_lista,
                                         unicode_sanitizar)
from bhadrasana.conf import ENCODE, tmpdir
from bhadrasana.models.models import (BaseOrigem, Filtro, PadraoRisco,
                                      ParametroRisco, ValorParametro, Visao)
from bhadrasana.utils.csv_handlers import muda_titulos_lista, sch_processing


class SemHeaders(Exception):
    """Exceção personalizada."""

    pass


def equality(listaoriginal, nomecampo, listavalores):
    """Realiza a filtragem dos riscos nas listas.

    Args:
        listaoriginal: Lista importada do CSV

        nomecampo: Nome do campo a ser buscado

        listavalores: Lista de valores a serem buscados

    Returns:
        Retorna uma lista com itens da listaoriginal que são iguais ao da
        listavalores

    """
    df = pd.DataFrame(listaoriginal[1:], columns=listaoriginal[0])
    df = df[df[nomecampo].isin(listavalores)]
    return df.values.tolist()


def startswith(listaoriginal, nomecampo, listavalores):
    """Realiza a filtragem dos riscos nas listas.

    Args:
        listaoriginal: Lista importada do CSV

        nomecampo: Nome do campo a ser buscado

        listavalores: Lista de valores a serem buscados

    Returns:
        Retorna uma lista com itens da listaoriginal que começam com os itens
        da listavalores

    """
    df = pd.DataFrame(listaoriginal[1:], columns=listaoriginal[0])
    result = []
    for valor in listavalores:
        df_filtered = df[df[nomecampo].str.startswith(valor, na=False)]
        result.extend(df_filtered.values.tolist())
    return result


def contains(listaoriginal, nomecampo, listavalores):
    """Realiza a filtragem dos riscos nas listas.

    Args:
        listaoriginal: Lista importada do CSV

        nomecampo: Nome do campo a ser buscado

        listavalores: Lista de valores a serem buscados

    Returns:
        Retorna uma lista com itens da listaoriginal que contenham os itens
        da listavalores

    """
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

# TODO: Estudar refatoração: dividir em classes, utilizar herança
# GerenteRisco->GerenteRiscoCSV
# GerenteRisco->GerenteRiscoMongo


class GerenteRisco():
    """Classe que aplica parâmetros de risco e/ou junção em listas.

    São fornecidos também metodos para facilitar o de/para entre
    o Banco de Dados e arquivos csv de parâmetros, para permitir que
    usuário importe e exporte parâmetros de risco.

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
        """Somente inicializa properties."""
        self.pre_processers = {}
        self.pre_processers_params = {}
        self._riscosativos = {}
        self._padraorisco = None

    def importa_base(self, csv_folder: str, baseid: int, data,
                     filename: str, remove=False):
        """Copia base para dest_path, processando se necessário.

        Aceita arquivos .zip contendo arquivos sch e arquivos csv únicos.
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

        Returns:
            Uma tupla ou lista de tuplas. Primeiros itens são CSVs criados

        """
        dest_path = os.path.join(csv_folder, str(baseid),
                                 data[:4], data[5:7], data[8:10])
        if os.path.exists(dest_path):
            raise FileExistsError(
                'Já houve importação de base para os parâmetros informados')
        else:
            os.makedirs(dest_path)
        try:
            if '.zip' in filename or os.path.isdir(filename):
                result = sch_processing(filename,
                                        dest_path=dest_path)
            else:
                # No caso de CSV, retornar erro caso títulos não batam
                # com importação anterior
                # Para sch zipados e lista de csv zipados, esta verificação
                # é mais custosa, mas também precisa ser implementada
                cabecalhos_antigos = self.get_headers_base(baseid, csv_folder)
                diferenca_cabecalhos = set()
                if cabecalhos_antigos:
                    with open(filename, 'r', newline='') as file:
                        reader = csv.reader(file)
                        cabecalhos_novos = set(next(reader))
                        diferenca_cabecalhos = (cabecalhos_novos ^
                                                cabecalhos_antigos)
                        if diferenca_cabecalhos:
                            raise ValueError(
                                'Erro na importação! Há base anterior' +
                                ' com cabeçalhos diferentes: ' +
                                str(diferenca_cabecalhos))
                dest_filename = os.path.join(dest_path,
                                             os.path.basename(filename))
                shutil.copyfile(filename,
                                os.path.join(dest_filename))
                result = [(dest_filename, 'single csv')]
                # result = csv_processing(tempfile, dest_path=dest_path)
            if not os.path.isdir(filename) and remove:
                os.remove(filename)
        except Exception as err:
            shutil.rmtree(dest_path)
            raise err
        return result

    def set_padraorisco(self, padraorisco):
        """Vincula o Gerente a um objeto padraoriscoOriginal.

        Atenção: **Todos** os parâmetros de risco ativos no Gerente serão
        zerados!!!

        **Todos** os parâmetros de risco vinculados à padraoriscoOriginal serão
        adicionados aos riscos ativos!!!
        """
        self._padraorisco = padraorisco
        self._riscosativos = {}
        if self._padraorisco:
            for parametro in self._padraorisco.parametros:
                self.add_risco(parametro)

    def cria_padraorisco(self, nomepadraorisco, session):
        """Cria um novo objeto PadraoRisco.

        Args:
            nomepadraorisco: Nome do padrão a ser criado

            session: Sessão do banco de dados
        """
        padraorisco = session.query(PadraoRisco).filter(
            PadraoRisco.nome == nomepadraorisco).first()
        if not padraorisco:
            padraorisco = PadraoRisco(nomepadraorisco)
        self.set_padraorisco(padraorisco)

    def add_risco(self, parametrorisco, session=None):
        """Configura os parametros de risco ativos.

        Args:
            parametrorisco: Nome do parâmetro a ser adicionado

            session: Sessão do banco de dados
        """
        dict_filtros = defaultdict(list)
        for valor in parametrorisco.valores:
            dict_filtros[valor.tipo_filtro].append(valor.valor.lower())
        self._riscosativos[parametrorisco.nome_campo.lower()] = dict_filtros
        if session and self._padraorisco:
            self._padraorisco.parametros.append(parametrorisco)
            session.merge(self._padraorisco)
            session.commit()

    def remove_risco(self, parametrorisco, session=None):
        """Configura os parametros de risco ativos.

        Args:
            parametrorisco: Nome do parâmetro a ser removido

            session: Sessão do banco de dados
        """
        self._riscosativos.pop(parametrorisco.nome_campo, None)
        if session and self._padraorisco:
            self._padraorisco.parametros.remove(parametrorisco)
            session.merge(self._padraorisco)
            session.commit()

    def clear_risco(self, session=None):
        """Zera os parametros de risco ativos.

        Args:
            session: Sessão do banco de dados
        """
        self._riscosativos = {}
        if session and self._padraorisco:
            self._padraorisco.parametros.clear()
            session.merge(self._padraorisco)
            session.commit()

    def checa_depara(self, base: BaseOrigem):
        """Se tiver depara na base, adiciona aos pre_processers.

        Os pre_processers são usados ao recuperar headers, importar,
        aplicar_risco, entre outras ações.
        """
        if base.deparas:
            de_para_dict = {depara.titulo_ant: depara.titulo_novo
                            for depara in base.deparas}
            self.pre_processers['mudartitulos'] = muda_titulos_lista
            self.pre_processers_params['mudartitulos'] = {
                'de_para_dict': de_para_dict, 'make_copy': False}

    def ativa_sanitizacao(self, norm_function=unicode_sanitizar):
        """Inclui função de sanitização nos pre_processers.

        Os pre_processers são usados ao recuperar headers, importar,
        aplicar_risco, entre outras ações.
        """
        self.pre_processers['sanitizar'] = sanitizar_lista
        self.pre_processers_params['sanitizar'] = {
            'norm_function': norm_function}

    def load_csv(self, arquivo):
        """Carrega arquivo csv em lista."""
        with open(arquivo, 'r', encoding=ENCODE, newline='') as arq:
            reader = csv.reader(arq)
            lista = [linha for linha in reader]
        return lista

    def save_csv(self, lista, arquivo):
        """Salva lista em arquivo csv. Exclui se existir."""
        try:
            os.remove(arquivo)  # Remove resultado antigo se houver
        except IOError:
            pass
        with open(arquivo, 'w', encoding=ENCODE, newline='') as csv_out:
            writer = csv.writer(csv_out)
            writer.writerows(lista)

    def strip_lines(self, lista):
        """Retira espaços adicionais entre palavras.

        Precaução: retirar espaços mortos de todo item
        da lista para evitar erros de comparação
        """
        for ind, linha in enumerate(lista):
            linha_striped = []
            for item in linha:
                if isinstance(item, str):
                    item = item.strip()
                linha_striped.append(item)
            lista[ind] = linha_striped
        return lista

    def pre_processa(self, lista):
        """Aplica funções de processamento de texto na lista."""
        for key in self.pre_processers:
            lista = self.pre_processers[key](lista,
                                             **self.pre_processers_params[key])
        return lista

    def pre_processa_arquivos(self, lista_arquivos):
        """Carrega uma lista de arquivos e pré processa texto.

        Carrega a lista de arquivos aplica as funções de pré processamento
        ativas. Salva novamente no mesmo arquivo.
        """
        # print(lista_arquivos)
        if len(lista_arquivos) > 0:
            if (isinstance(lista_arquivos[0], list) or
                    isinstance(lista_arquivos[0], tuple)):
                alista = [linha[0] for linha in lista_arquivos]
        # print(alista)
        for filename in alista:
            lista = self.load_csv(filename)
            lista = self.pre_processa(lista)
            self.save_csv(lista, filename)

    def aplica_risco(self, lista=None, arquivo=None, parametros_ativos=None):
        """Método de filtragem de lista ou dados de arquivo.

        Compara a linha de título da lista recebida com a lista de nomes
        de campo que possuem parâmetros de risco ativos. Após, chama para cada
        campo encontrado a função de filtragem. Somente um dos parâmetros
        precisa ser passado. Caso na lista do pipeline estejam cadastradas
        funções de pré-processamento, serão aplicadas.

        Args:
            lista (list): Lista a ser filtrada, primeira linha deve conter os
            nomes dos campos idênticos aos definidos no nome_campo
            do parâmetro de risco cadastrado.

            **OU**

            arquivo (str): Arquivo csv de onde carregar a lista a ser filtrada

            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo

        Obs:
            Para um arquivo, quando a base for constituída de vários arquivos,
            utilizar :func:`aplica_juncao`

        """
        mensagem = 'Arquivo não fornecido!'
        if arquivo:
            mensagem = 'Lista não fornecida!'
            lista = self.load_csv(arquivo)
        if not lista:
            raise AttributeError('Erro! ' + mensagem)
        # Aplicar pre_processers
        # logger.debug('Aplica risco. Lista ANTES do pre processamento')
        # logger.debug(lista[:10])
        lista = self.strip_lines(lista)
        lista = self.pre_processa(lista)
        # logger.debug('Aplica risco. Lista DEPOIS do pre processamento')
        # logger.debug(lista[:10])
        headers = set(lista[0])
        # print('Ativos:', parametros_ativos)
        if parametros_ativos:
            riscos = set([parametro.lower()
                          for parametro in parametros_ativos])
        else:
            riscos = set([key.lower() for key in self._riscosativos.keys()])
        aplicar = headers & riscos   # INTERSECTION OF SETS
        # logger.debug('Headers, riscos a aplicar, e intersecção: ')
        # logger.debug(headers)
        # logger.debug(riscos)
        # logger.debug(aplicar)
        result = []
        result.append(lista[0])
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
        """Salva parametro em arquivo.

        Salva os valores do parâmetro de risco em um arquivo csv
        no formato 'valor', 'tipo_filtro'.
        """
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
        sanitizar(campo)
        filename = os.path.join(path, campo + '.csv')
        with open(filename,
                  'w', encoding=ENCODE, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(lista)
        return filename

    def parametros_tocsv(self, path=tmpdir):
        """Salva os parâmetros adicionados a um gerente em um arquivo csv.

        Ver também: :py:func:`parametros_fromcsv`
        """
        for campo in self._riscosativos:
            self.parametro_tocsv(campo, path=path)

    def parametros_fromcsv(self, campo, session=None, padraorisco=None,
                           lista=None, path=tmpdir):
        """Carrega parâmetros de risco de um aquivo ou de uma lista.

        Abre um arquivo csv, recupera parâmetros configurados nele,
        adiciona à configuração do gerente e **também adiciona ao Banco de
        Dados ativo** caso não existam nele ainda. Para isso é preciso
        passar a session como parâmetro, senão cria apenas na memória
        Pode receber uma lista no lugar de um arquivo csv (como implementado
        em import_named_csv).

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

            *OU*

            path: caminho do arquivo .csv

        Obs:
            O arquivo .csv ou a lista DEVEM estar no formato valor, tipo_filtro

        """
        if not lista:
            with open(os.path.join(path, campo + '.csv'),
                      'r', encoding=ENCODE, newline='') as f:
                reader = csv.reader(f)
                next(reader)
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
        """Importa vários parâmetros nomeados de um arquivo único.

        Abre um arquivo csv, cada coluna sendo um filtro.
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
                        listas[
                            cabecalho[ind].strip().replace('/ ', '')].append(
                            [coluna, filtro.name])
                    ind += 1
        for key, value in listas.items():
            self.parametros_fromcsv(key, session, padraorisco, value)
        if tolist:
            return cabecalho

    def get_headers_base(self, baseorigemid, path, csvs=False):
        """Retorna lista de headers.

        Busca última base disponível no diretório de CSVs e
        traz todos os headers
        """
        lista_csv = []
        cabecalhos = []
        cabecalhos_nao_repetidos = set()
        caminho = os.path.join(path, str(baseorigemid))
        if not os.path.isdir(caminho):
            return set()
        # Procura ano, depois mes, depois dia, vai adicionando no caminho
        for r in range(3):
            ano_mes_dia = sorted(os.listdir(caminho))
            if len(ano_mes_dia) == 0:
                break
            caminho = os.path.join(caminho, ano_mes_dia[-1])
        for arquivo in os.listdir(caminho):
            lista_csv.append(arquivo[:-4])
            with open(os.path.join(caminho, arquivo),
                      'r', encoding=ENCODE, newline='') as f:
                reader = csv.reader(f)
                cabecalho = next(reader)
                cabecalhos.extend(cabecalho)
        for word in cabecalhos:
            new_word = sanitizar(word, norm_function=unicode_sanitizar)
            if new_word not in cabecalhos_nao_repetidos:
                cabecalhos_nao_repetidos.add(new_word)
        if csvs:
            return lista_csv
        return cabecalhos_nao_repetidos

    def aplica_juncao(self, visao, path=tmpdir, filtrar=False,
                      parametros_ativos=None):
        """Faz junção de arquivos diversos.

        Lê, um a um, os csvs configurados em visao.tabelas. Carrega em
        DataFrames e faz merge destes.

        Args:
            visao: objeto de Banco de Dados que espeficica as configurações
            (metadados) da base

            path: caminho da base de arquivos csv

            filtrar: aplica_risco após merge dos DataFrames

            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo.

        Obs:
            Quando a base for constituída de arquivo único, utilizar
            :func:`aplica_risco`

        """
        numero_juncoes = len(visao.tabelas)
        dffilho = None
        if numero_juncoes > 1:  # Caso apenas uma tabela esteja na visão,
            tabela = visao.tabelas[numero_juncoes - 1]  # não há junção
            filhofilename = os.path.join(path, tabela.csv_file)
            dffilho = pd.read_csv(filhofilename, encoding=ENCODE,
                                  dtype=str)
            if hasattr(tabela, 'type'):
                how = tabela.type
            else:
                how = 'inner'
        # A primeira precisa ser "pulada", sempre é a junção 2 tabelas
        # de cada vez. Se numero_juncoes for >2, entrará aqui fazendo
        # a junção em cadeia desde o último até o primeiro filho
        for r in range(numero_juncoes - 2, 0, -1):
            estrangeiro = tabela.estrangeiro.lower()
            if hasattr(tabela, 'type'):
                how = tabela.type
            else:
                how = 'inner'
            tabela = visao.tabelas[r]
            primario = tabela.primario.lower()
            paifilhofilename = os.path.join(path, tabela.csv_file)
            dfpaifilho = pd.read_csv(paifilhofilename, encoding=ENCODE,
                                     dtype=str)
            dffilho = dfpaifilho.merge(dffilho, how=how,
                                       left_on=primario,
                                       right_on=estrangeiro)
        csv_pai = visao.tabelas[0].csv_file
        paifilename = os.path.join(path, csv_pai)
        dfpai = pd.read_csv(paifilename, encoding=ENCODE, dtype=str)
        # print(tabela.csv_file, tabela.estrangeiro, tabela.primario)
        if dffilho is not None:
            dfpai = dfpai.merge(dffilho, how=how,
                                left_on=tabela.primario.lower(),
                                right_on=tabela.estrangeiro.lower())
        # print(dfpai)
        if visao.colunas:
            colunas = [coluna.nome.lower() for coluna in visao.colunas]
            result_df = dfpai[colunas]
            result_list = [colunas]
        else:
            result_df = dfpai
            result_list = [result_df.columns.tolist()]
        result_list.extend(result_df.values.tolist())
        # print(result_list)
        if filtrar:
            return self.aplica_risco(result_list,
                                     parametros_ativos=parametros_ativos)
        return result_list

    @classmethod
    def csv_to_mongo(cls, db, base, path=None, arquivo=None, unique=[]):
        """Insere conteúdo do arquivo csv em coleção MongoDB.

        Lê um arquivo CSV e insere todo seu conteúdo em uma coleção do
        MongoDB. Cria a coleção se não existir.

        Args:
            db: "MongoDBClient" conexão com o banco de dados selecionado

            base: Base Origem

            path: caminho do(s) arquivo(s) csv

            **OU**

            arquivo: caminho e nome do arquivo csv

            unique: lista de campos que terão indice único (e
            não serão reinseridos) - TODO: unique field on mongo archive
        """
        if path is None and arquivo is None:
            raise AttributeError('Nome ou caminho do(s) arquivo(s) deve ser'
                                 'obrigatoriamente informado')
        if arquivo:
            lista_arquivos = [os.path.basename(arquivo)]
            path = os.path.dirname(arquivo)
        else:
            lista_arquivos = os.listdir(path)
        for arquivo in lista_arquivos:
            df = pd.read_csv(os.path.join(path, arquivo),
                             encoding=ENCODE, dtype=str)
            data_json = json.loads(df.to_json(orient='records'))
            collection_name = base.nome + '.' + arquivo[:-4]
            if collection_name not in db.collection_names():
                db.create_collection(collection_name)
            # TODO: Estudar possibilidade de evitar inserções duplicadas,
            # em caso de evidência de inseração duplicada fazer upsert
            # É complicado, pois é necessário ter a metadata de cada tabela,
            # isto é, o campo que será considerado 'chave' para a inserção ou
            # update
            for linha in data_json:
                try:
                    db[collection_name].insert(linha)
                except pymongo.errors.DuplicateKeyError:
                    pass

    def load_mongo(self, db, base=None, collection_name=None,
                   parametros_ativos=None, limit=0, skip=0):
        """Recupera da base mongodb em um lista.

        Args:
            db: "MongoDBClient" conexão com o banco de dados selecionado.

            base: Base Origem

            **OU**

            collection_name: nome da coleção do MongoDB

            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo

        """
        if base is None and collection_name is None:
            raise AttributeError('Base Origem ou collection name devem ser'
                                 'obrigatoriamente informado')
        logger.debug(parametros_ativos)
        if parametros_ativos:
            riscos = set([parametro.lower()
                          for parametro in parametros_ativos])
        else:
            riscos = set([key.lower() for key in self._riscosativos.keys()])
        filtro = {}
        listadefiltros = []
        for campo in riscos:
            dict_filtros = self._riscosativos.get(campo)
            #  TODO: $or operator, filter operator
            for tipo_filtro, lista_filtros in dict_filtros.items():
                filtro = {campo: {'$in': lista_filtros}}
                listadefiltros.append(filtro)
        if listadefiltros:
            filtro = {'$or': listadefiltros}
        logger.debug(filtro)
        if collection_name:
            if collection_name.find('.csv') != -1:
                collection_name = collection_name[:-4]
            list_collections = [collection_name]
        else:
            list_collections = [name for name in
                                db.collection_names() if base.nome in name]
        result = OrderedDict()
        for collection_name in list_collections:
            mongo_list = db[collection_name].find(
                filtro).limit(limit).skip(skip)
            if mongo_list.count() == 0:
                filtro = {}
                mongo_list = db[collection_name].find(filtro)
            try:
                headers = [[key for key in mongo_list[0].keys()]]
                # number_of_headers = len(headers)
            except IndexError as err:
                logger.error('load_mongo retornou vazio. Collection name:')
                logger.error(collection_name)
                logger.error(err, exc_info=True)
                return []
            for linha in mongo_list:
                for key, value in linha.items():
                    if result.get(key) is None:
                        result[key] = []
                    result[key].append(value)
                # result.append(valores)
        # logger.debug('Result ')
        # logger.debug(result)
        headers = [key for key in result.keys()]
        comprimento = len(result[headers[0]])
        lista = [headers]
        for i in range(comprimento - 1):
            linha = []
            for key in headers:
                try:
                    linha.append(result[key][i])
                except IndexError:
                    continue
            lista.append(linha)
        return lista

    def aplica_juncao_mongo(self, db, visao,
                            parametros_ativos=None,
                            filtrar=False,
                            pandas=False,
                            limit=100,
                            skip=0):
        """Apenas um proxy para compatibilidade reversa.

        Proxy para compatibilidade e escolha.
        Repassa todos os parâmetros para o método utilizando
        pandas se param pandas=True ou aggregate se False.
        """
        if pandas:
            return self.aplica_juncao_mongo_pandas(db, visao,
                                                   parametros_ativos,
                                                   filtrar)
        return self.aplica_juncao_mongo_aggregate(db, visao,
                                                  parametros_ativos,
                                                  filtrar,
                                                  limit,
                                                  skip)

    def aplica_juncao_mongo_pandas(self, db, visao,
                                   parametros_ativos=None,
                                   filtrar=False):
        """Lê as coleções configuradas no mongo.

        Lê as coleções, carrega em DataFrames e faz merge destes.

        Args:
            db: MongoDB
            visao: objeto de Banco de Dados que espeficica as configurações
            (metadados) da base
            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados
            filtrar: aplica_risco após merge dos DataFrames

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo

        """
        base = visao.base
        numero_juncoes = len(visao.tabelas)
        dffilho = None
        if numero_juncoes > 1:   # Caso apenas uma tabela esteja na visão,
            tabela = visao.tabelas[numero_juncoes - 1]  # não há junção
            filhoname = base.nome + '.' + tabela.csv_file
            print(filhoname)
            lista = self.load_mongo(db, collection_name=filhoname)
            dffilho = pd.DataFrame(lista[1:], columns=lista[0])
            if hasattr(tabela, 'type'):
                how = tabela.type
            else:
                how = 'inner'
        # A primeira precisa ser "pulada", sempre é a junção 2 tabelas
        # de cada vez. Se numero_juncoes for >2, entrará aqui fazendo
        # a junção em cadeia desde o último até o primeiro filho
        for r in range(numero_juncoes - 2, 0, -1):
            tabela = visao.tabelas[r]
            paifilhoname = base.nome + '.' + tabela.csv_file
            if hasattr(tabela, 'type'):
                how = tabela.type
            else:
                how = 'inner'
            lista = self.load_mongo(db, collection_name=paifilhoname)
            dfpaifilho = pd.DataFrame(lista[1:], columns=lista[0])
            # print(tabela.csv_file, tabela.estrangeiro, tabela.primario)
            dffilho = dfpaifilho.merge(dffilho, how=how,
                                       left_on=tabela.primario.lower(),
                                       right_on=tabela.estrangeiro.lower())
        painame = base.nome + '.' + visao.tabelas[0].csv_file
        lista = self.load_mongo(db, collection_name=painame)
        dfpai = pd.DataFrame(lista[1:], columns=lista[0])
        if dffilho is not None:
            dfpai = dfpai.merge(dffilho, how=how,
                                left_on=tabela.primario.lower(),
                                right_on=tabela.estrangeiro.lower())
        if visao.colunas:
            colunas = [coluna.nome.lower() for coluna in visao.colunas]
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

    def aplica_juncao_mongo_aggregate(self, db, visao,
                                      parametros_ativos=None,
                                      filtrar=False,
                                      limit=100,
                                      skip=0):
        """Lê as coleções configuradas no mongo através de aggregates.

        Monta um pipeline MongoDB.
        Utiliza configurações da visao para montar um aggregate entre coleções
        MongoDB. Caso configurado, utiliza as colunas programadas para fazer
        'projection', isto é, trazer somente estas do Banco. Também aplica
        'match', filtrando os resultados.

        Args:
            db: MongoDB
            visao: objeto de Banco de Dados que espeficica as configurações
            (metadados) da base
            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados
            filtrar: aplica_risco na consulta

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo.

        """
        base = visao.base
        numero_juncoes = len(visao.tabelas)
        collection = None
        pipeline = []
        for r in range(1, numero_juncoes):
            tabela = visao.tabelas[r]
            paifilhoname = base.nome + '.' + tabela.csv
            pipeline.append({
                '$lookup':
                {'from': paifilhoname,
                 'localField': tabela.primario.lower(),
                 'foreignField': tabela.estrangeiro.lower(),
                 'as': tabela.csv
                 }})
            pipeline.append(
                {'$unwind': {'path': '$' + tabela.csv}}
            )
        painame = visao.tabelas[0].csv
        collection = db[base.nome + '.' + painame]
        tabelas = [tabela.csv for tabela in visao.tabelas]
        if visao.colunas:
            colunas = {'_id': 0}
            for coluna in visao.colunas:
                print(coluna.nome, tabelas)
                if coluna.nome not in tabelas:
                    colunas[coluna.nome.lower()] = 1
                for tabela in tabelas[1:]:
                    colunas[tabela + '.' + coluna.nome.lower()] = 1
            pipeline.append({'$project': colunas})
        if filtrar:
            filtro = []
            if parametros_ativos:
                riscos = set([parametro.lower()
                              for parametro in parametros_ativos])
            else:
                riscos = set([key.lower() for key
                              in self._riscosativos.keys()])
                for campo in riscos:
                    dict_filtros = self._riscosativos.get(campo)
                    for tipo_filtro, lista_filtros in dict_filtros.items():
                        print(tipo_filtro, lista_filtros)
                        # filter_function = filter_functions.get(tipo_filtro)
                        for valor in lista_filtros:
                            filtro.append({campo: valor})
                            for tabela in visao.tabelas:
                                filtro.append(
                                    {tabela.csv + '.' + campo: valor}
                                )
            if filtro:
                print('FILTRO', filtro)
                pipeline.append({'$match': {'$or': filtro}})
        pipeline.append({'$limit': skip + limit})
        pipeline.append({'$skip': skip})
        print('PIPELINE', pipeline)
        mongo_list = list(collection.aggregate(pipeline))
        # print(mongo_list)
        if mongo_list and len(mongo_list) > 0:
            first_line = mongo_list[0]
            result = []
            header = []
            for key, value in first_line.items():
                # print('VALUE', key, value, type(value))
                if isinstance(value, dict):
                    for sub_key in value.keys():
                        header.append(key + '.' + sub_key)
                else:
                    header.append(key)
            result.append(header)
            for linha in mongo_list:
                linha_lista = []
                for campo in header:
                    hieraquia = campo.split('.')
                    valor = linha
                    for nivel in hieraquia:
                        valor = valor.get(nivel)
                    linha_lista.append(valor)
                result.append(linha_lista)
            return result
        return None

    def aplica_risco_por_parametros(self, dbsession,
                                    base_csv: str,
                                    padraoid=0,
                                    visaoid=0,
                                    parametros_ativos: list=None):
        """Escolhe o método correto de acordo com parâmetros.

        Chama arquivo(s) com ou sem junção e filtro,
        de acordo com parâmetros passados.

        Ver :py:func:`aplica_risco` e :py:func:`aplica_juncao`

        Args:

            dbsession: conexão com o Banco de Dados

            base_csv: Arquivo csv de onde carregar a lista a ser filtrada

            padraoid: ID do PadraoRisco a utilizar como filtro

            visaoid: objeto Visao do Banco de Dados que espeficica as
             configurações (metadados) da base

            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            Lista contendo os campos filtrados. 1ª linha com nomes de campo.

        """
        padrao = dbsession.query(PadraoRisco).filter(
            PadraoRisco.id == padraoid
        ).first()
        self.set_padraorisco(padrao)
        if visaoid == '0':
            dir_content = os.listdir(base_csv)
            arquivo = os.path.join(base_csv, dir_content[0])
            lista_risco = self.load_csv(arquivo)
            if padrao is None:
                return lista_risco
            return self.aplica_risco(
                lista_risco,
                parametros_ativos=parametros_ativos
            )
        else:
            avisao = dbsession.query(Visao).filter(
                Visao.id == visaoid).one()
            return self.aplica_juncao(
                avisao, path=base_csv,
                filtrar=padrao is not None,
                parametros_ativos=parametros_ativos
            )


"""
db.getCollection('CARGA.Conhecimento').aggregate(
    [{'$lookup': {'from': 'CARGA.NCM',
                 'localField': 'conhecimento',
                 'foreignField': 'conhecimento',
                 'as': 'NCM'}},
    {'$unwind': {'path': '$NCM'}},
    {'$project':
     {'_id': 0, 'Conhecimento': 1,
      'Conhecimento.Conhecimento': 1,
      'NCM.Conhecimento': 1,
      'NCM': 1,
      'Conhecimento.NCM': 1,
      'NCM.NCM': 1,
      'CPFCNPJConsignatario': 1,
      'Conhecimento.CPFCNPJConsignatario': 1,
      'NCM.CPFCNPJConsignatario': 1,
      'DescricaoMercadoria': 1,
      'Conhecimento.DescricaoMercadoria': 1,
      'NCM.DescricaoMercadoria': 1,
      'IdentificacaoEmbarcador': 1,
      'Conhecimento.IdentificacaoEmbarcador': 1,
      'NCM.IdentificacaoEmbarcador': 1}}
      ]
"""
