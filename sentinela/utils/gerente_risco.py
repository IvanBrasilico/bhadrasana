"""Módulo responsável pelas funções que aplicam os filtros/parâmetros
de risco cadastrados nos dados. Utiliza pandas para realizar filtragem
"""
import csv
import json
import os
import shutil
from collections import defaultdict
from flask import flash

import pandas as pd
import pymongo

from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, sanitizar_lista, \
    unicode_sanitizar
from sentinela.conf import ENCODE, tmpdir
from sentinela.models.models import (Filtro, PadraoRisco, ParametroRisco,
                                     ValorParametro)
from sentinela.utils.csv_handlers import muda_titulos_lista, sch_processing


class SemHeaders(Exception):
    pass


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
        """Vincula o Gerente a um objeto padraoriscoOriginal
        Atenção: **Todos** os parâmetros de risco ativos no Gerente serão
        zerados!!!
        **Todos** os parâmetros de risco vinculados à padraoriscoOriginal serão
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
            dict_filtros[valor.tipo_filtro].append(valor.valor.lower())
        self._riscosativos[parametrorisco.nome_campo.lower()] = dict_filtros
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
            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

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
        sanitizar(campo)
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
            path: caminho do arquivo .csv

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
                        listas[
                            cabecalho[ind].strip().replace('/ ', '')].append(
                            [coluna, filtro.name])
                    ind += 1
        for key, value in listas.items():
            self.parametros_fromcsv(key, session, padraorisco, value)
        if tolist:
            return cabecalho

    def get_headers_base(self, baseorigemid, path, arquivo=False):
        """Busca última base disponível no diretório de CSVs e
        traz todos os headers"""
        cabecalhos = []
        arquivos = []
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
            arquivos.append(arquivo[:-4])
            with open(os.path.join(caminho, arquivo),
                      'r', encoding=ENCODE, newline='') as f:
                reader = csv.reader(f)
                cabecalho = next(reader)
                cabecalhos.extend(cabecalho)
        for word in cabecalhos:
            new_word = sanitizar(word, norm_function=unicode_sanitizar)
            if new_word not in cabecalhos_nao_repetidos:
                cabecalhos_nao_repetidos.add(new_word)
        if arquivo is True:
            return arquivos
        return cabecalhos_nao_repetidos

    def aplica_juncao(self, visao, path=tmpdir, filtrar=False,
                      parametros_ativos=None):
        """Lê, um a um, os csvs configurados em visao.tabelas. Carrega em
        DataFrames e faz merge destes.

        Args:
            visao: objeto de Banco de Dados que espeficica as configurações
            (metadados) da base
            path: caminho da base de arquivos csv
            filtrar: aplica_risco após merge dos DataFrames
            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            lista contendo os campos filtrados. 1ª linha com nomes de campo

        Obs: quando a base for constituída de arquivo único,
         utilizar :func:`aplica_risco`
        """
        numero_juncoes = len(visao.tabelas)
        dffilho = None
        if numero_juncoes > 1:   # Caso apenas uma tabela esteja na visão,
                                 # não há junção
            tabela = visao.tabelas[numero_juncoes - 1]
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
        if dffilho:
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
        print(result_list)
        if filtrar:
            return self.aplica_risco(result_list,
                                     parametros_ativos=parametros_ativos)
        return result_list

    @classmethod
    def csv_to_mongo(cls, db, base, path=None, arquivo=None, unique=[]):
        """Reads a csv file and inserts all contents into a MongoDB collection
        Creates collection if it not exists
        Args:
            db: MongoDBClient connection with database setted
            base: Base Origem
            path: caminho do(s) arquivo(s) csv OU
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
                   parametros_ativos=None):
        """Recupera da base mongodb em um lista
        Args:
            db: MongoDBClient connection with database setted
            base: Base Origem OU
            collection_name: nome da coleção do MongoDB
            parametros_ativos: subconjunto do parâmetros de risco a serem
            aplicados

        Returns:
            lista contendo os campos filtrados. 1ª linha com nomes de campo

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
        print(filtro)
        if collection_name:
            print(collection_name)
            if collection_name.find('.csv') != -1:
                collection_name = collection_name[:-4]
            list_collections = [collection_name]
        else:
            list_collections = [name for name in
                                db.collection_names() if base.nome in name]
        result = []
        for collection_name in list_collections:
            mongo_list = db[collection_name].find(filtro)
            if mongo_list.count() == 0:
                filtro = {}
                mongo_list = db[collection_name].find(filtro)
            try:
                result = [[key for key in mongo_list[0].keys()]]
            except IndexError as err:
                logger.error('load_mongo retornou vazio. Collection name:')
                logger.error(collection_name)
                logger.error(err, exc_info=True)
                return []
            for linha in mongo_list:
                result.append([value for value in linha.values()])
        logger.debug('Result ')
        logger.debug(result)
        return result

    def aplica_juncao_mongo(self, db, visao,
                            parametros_ativos=None,
                            filtrar=False):
        # TODO: Usar métodos próprios do MongoDB ao invés de DataFrames para
        # trazer dados já filtrados e melhorar desempenho
        base = visao.base
        numero_juncoes = len(visao.tabelas)
        dffilho = None
        if numero_juncoes > 1:   # Caso apenas uma tabela esteja na visão,
                                 # não há junção

            tabela = visao.tabelas[numero_juncoes - 1]
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
        if dffilho:
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
