"""Tests and documents use of the Web UI
Any client must make this type of request to Web UI
Made from Flask testing docs
http://flask.pocoo.org/docs/0.12/testing/
Use python bhadrasana/web_app_testing.py para transformar em
teste funcional

Alguns testes (importa_base, aplica_risco, arquiva_base),
gravam registros no Servidor Celery, portanto a saída
só será realmente avaliada nos testes funcionais

"""
import os
import unittest
from io import BytesIO

from pymongo import MongoClient

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from bhadrasana.models.models import (Base, BaseOrigem, Coluna, DePara,
                                      MySession, PadraoRisco, ParametroRisco,
                                      Tabela, ValorParametro, Visao)
from bhadrasana.views import configure_app


mysession = MySession(Base, test=True)
dbsession = mysession.session
engine = mysession.engine
conn = MongoClient(host=MONGODB_URI)
mongodb = conn[DATABASE]
app = configure_app(dbsession, None)
Base.metadata.create_all(engine)


class FlaskTestCase(unittest.TestCase):

    def setUp(self):
        # Ativar esta variável de ambiente na inicialização
        # do Servidor WEB para transformar em teste de integração
        self.http_server = os.environ.get('HTTP_SERVER')
        self.dbsession = dbsession
        self.engine = engine
        if self.http_server is not None:
            from webtest import TestApp
            self.app = TestApp(self.http_server)
        else:
            app.testing = True
            # app.config['WTF_CSRF_ENABLED'] = False
            self.app = app.test_client()

    def tearDown(self):
        self.logout()
        pass

    def get_token(self, url):
        if self.http_server is not None:
            response = self.app.get(url)
            self.csrf_token = str(response.html.find_all(
                attrs={'name': 'csrf_token'})[0])
            begin = self.csrf_token.find('value="') + 7
            end = self.csrf_token.find('"/>')
            self.csrf_token = self.csrf_token[begin: end]
        else:
            response = self.app.get(url, follow_redirects=True)
            csrf_token = response.data.decode()
            begin = csrf_token.find('csrf_token"') + 10
            end = csrf_token.find('username"') - 10
            csrf_token = csrf_token[begin: end]
            begin = csrf_token.find('value="') + 7
            end = csrf_token.find('/>')
            self.csrf_token = csrf_token[begin: end]
            return self.csrf_token

    def login(self, username, senha):
        self.get_token('/login')
        if self.http_server is not None:
            response = self.app.post('/login',
                                     params=dict(
                                         username=username,
                                         senha=senha,
                                         csrf_token=self.csrf_token)
                                     )
            return response
        else:
            return self.app.post('/login', data=dict(
                username=username,
                senha=senha,
                csrf_token=self.csrf_token
            ), follow_redirects=True)

    def logout(self):
        if self.http_server is not None:
            return self.app.get('/logout')
        else:
            return self.app.get('/logout', follow_redirects=True)

    # methods
    def data(self, rv):
        if self.http_server is not None:
            return str(rv.html).encode('utf_8')
        return rv.data

    def _post(self, url, data, follow_redirects=True):
        self.get_token(url)
        data['csrf_token'] = self.csrf_token
        print('TOKEN', self.csrf_token)
        if self.http_server is not None:
            rv = self.app.post(url, params=data)
        else:
            rv = self.app.post(url, data=data,
                               follow_redirects=follow_redirects)
        return rv

    def _get(self, url, follow_redirects=True):
        if self.http_server is not None:
            rv = self.app.get(url)
        else:
            rv = self.app.get(url,
                              follow_redirects=follow_redirects)
        return rv

    def _paramid(self, nome):
        parametro = self.dbsession.query(ParametroRisco).filter(
            ParametroRisco.nome_campo == nome).first()
        return str(parametro.id)

    def _valorid(self, nome):
        valor = self.dbsession.query(ValorParametro).filter(
            ValorParametro.valor == nome).first()
        return valor.id

    def _deparaid(self, nome):
        depara = self.dbsession.query(DePara).filter(
            DePara.titulo_ant == nome).first()
        return depara.id

    def _visaoid(self, nome):
        visao = self.dbsession.query(Visao).filter(
            Visao.nome == nome).first()
        return visao.id

    def _colunaid(self, nome):
        coluna = self.dbsession.query(Coluna).filter(
            Coluna.nome == nome).first()
        return coluna.id

    def _tabelaid(self, nome):
        tabela = self.dbsession.query(Tabela).filter(
            Tabela.csv == nome).first()
        return tabela.id

    def _excluibase(self):
        self.dbsession.query(BaseOrigem).filter(
            BaseOrigem.nome == 'baseteste').delete()
        self.dbsession.commit()

    def _excluipadrao(self):
        self.dbsession.query(PadraoRisco).filter(
            PadraoRisco.nome == 'padraoteste').delete()
        self.dbsession.commit()

    def _excluiparametros(self):
        self.dbsession.query(ParametroRisco).filter(
            ParametroRisco.nome_campo == 'z').delete()
        self.dbsession.commit()

    # Início dos testes...
    #
    def test_not_found(self):
        if self.http_server is None:
            rv = self.app.get('/non_ecsiste')
            assert b'404 Not Found' in rv.data

    # GET
    def test_1_home(self):
        rv = self._get('/', follow_redirects=True)
        data = self.data(rv)
        assert b'AJNA' in data
        assert b'input type="password"' in data
        self.login('ajna', 'ajna')
        rv = self._get('/', follow_redirects=True)
        data = self.data(rv)
        assert b'input type="password"' not in data
        assert b'AJNA' in data

    def test_1_0_adicionabase(self):
        rv = self._get('/adiciona_base/baseteste',
                       follow_redirects=True)
        data = self.data(rv)
        assert b'input type="password"' in data
        self.login('ajna', 'ajna')
        rv = self._get('/adiciona_base/baseteste',
                       follow_redirects=True)
        data = self.data(rv)
        assert b'baseteste' in data

    def test_1_1_0_editadepara(self):
        self.login('ajna', 'ajna')
        rv = self._get('/edita_depara?baseid=1')
        data = self.data(rv)
        assert b'baseteste' in data

    def test_1_1_2_adicionadepara(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/adiciona_depara?baseid=1&antigo=iguaria&novo=alimento'
        )
        data = self.data(rv)
        assert b'iguaria' in data

    def test_1_1_3_excluidepara(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/exclui_depara?=tituloid=1'
        )
        data = self.data(rv)
        assert b'iguaria' not in data

    def test_2_importabase(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/importa_base?baseid=1'
        )
        data = self.data(rv)
        assert b'AJNA' in data
        assert b'Submeter' in data

    def test_2_1_importabase_upload(self):
        # Aqui o teste é apenas se muda de tela.
        # Depois, ao aplicar risco, será validado se o arquivo
        # for realmente processado.
        file = {
            'file':
                (BytesIO(b'iguaria, esporte\n'
                         b'temaki, corrida\n'
                         b'churros, remo\n'),
                 'plan_test.csv'),
            'baseid': '1',
            'data': '2018-01-01'
        }
        self.login('ajna', 'ajna')
        rv = self._post('/importa_base', data=file, follow_redirects=True
                        )
        data = self.data(rv)
        assert b'Submeter' not in data
        assert b'Informe uma base' not in data

    def test_2_1_importabase_nofile(self):
        nofile = {
            'baseid': '1',
            'data': '2018-01-01'
        }
        self.login('ajna', 'ajna')
        rv = self._post('/importa_base', data=nofile, follow_redirects=True
                        )
        data = self.data(rv)
        assert b'Submeter' in data
        assert b'Selecionar arquivo' in data

    def test_2_1_importabase_wrongbase(self):
        wrongbase = {
            'file':
                (BytesIO(b'iguaria, esporte\n temaki, corrida'),
                 'plan_test.csv'),
            'baseid': '1000',
            'data': '2018-01-01'
        }
        self.login('ajna', 'ajna')
        rv = self._post('/importa_base', data=wrongbase, follow_redirects=True
                        )
        data = self.data(rv)
        print(data)
        assert b'Submeter' in data
        assert b'Informe uma base' in data

    def test_3_1_adicionapadraorisco(self):
        self.login('ajna', 'ajna')
        rv = self._get('/adiciona_padrao/padraoteste')
        data = self.data(rv)
        print(data)
        assert b'padraoteste' in data

    def test_3_2_padraorisco_vinculabase(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/vincula_base?padraoid=1&baseid=1',
            follow_redirects=True)
        data = self.data(rv)
        print(data)
        assert b'"selected">baseteste' in data

    """
    def test_3_3_padraorisco_excluibase(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/risco?&baseid=1&filename=plan_test.csv&acao=excluir',
            follow_redirects=True)
        data = self.data(rv)
        print(data)
        assert b'"center">baseteste' in data
    """
    def test_4_1_adicionaparametro(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/adiciona_parametro?padraoid=1&risco_novo=comida')
        data = self.data(rv)
        assert b'comida' in data

    def test_4_2_editarisco(self):
        self.login('ajna', 'ajna')
        param_id = self._paramid('comida')
        self.login('ajna', 'ajna')
        rv = self._get('/edita_risco?padraoid=1&riscoid=' +
                       param_id)
        data = self.data(rv)
        print(data)
        assert b'"center">comida' in data

    def test_4_3_adicionavalor(self):
        self.login('ajna', 'ajna')
        param_id = self._paramid('comida')
        rv = self._get(
            '/adiciona_valor?padraoid=1&riscoid=' + param_id +
            '&novo_valor=temaki&filtro=igual')
        data = self.data(rv)
        print(data)
        assert b'"center">comida' in data
        assert b'td>temaki' in data
        assert b'td>Filtro.igual' in data

    def test_4_4_valores(self):
        self.login('ajna', 'ajna')
        param_id = self._paramid('comida')
        rv = self._get('/valores?parametroid=' + param_id)
        data = self.data(rv)
        assert b'temaki' in data

    # importa csv
    def test_5_importacsv(self):
        self.login('ajna', 'ajna')
        param_id = self._paramid('comida')
        file = {
            'csv': (BytesIO(
                b'valor, tipo_filtro\nhot roll, igual\nchurros, igual'
            ), 'alimento.csv')
        }
        rv = self._post(
            '/importa_csv/1/' + param_id,
            data=file, follow_redirects=True)
        data = self.data(rv)
        assert b'hot roll' in data
        assert b'churros' in data

    def test_5_1_exportacsv(self):
        self.login('ajna', 'ajna')
        parametro = self._paramid('comida')
        rv = self._get(
            '/exporta_csv?padraoid=1&riscoid=' + str(parametro))
        data = self.data(rv)
        assert b'temaki' in data

    def test_6_aplica_risco(self):
        # Se este método der erro "No such file"
        # Significa que o importa_base não funcionou
        # Para funcionar é necessário habilitar um Servidor Celery
        self.login('ajna', 'ajna')
        rv = self._get('/risco?&filename=2018/01/01&acao=aplicar&baseid=1&padraoid=1&visaoid=0&\
                               &parametros_ativos=comida')
        data = self.data(rv)
        assert b'Lista de Riscos da Base 2018/01/01' in data

    def test_7_exclui_risco(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/risco?&baseid=1&filename=2018/01/01&acao=excluir')
        data = self.data(rv)
        print(data)
        # assert b'2018/01/01' not in data

    def test_7_1_excluivalor(self):
        self.login('ajna', 'ajna')
        valor_id = self._valorid('temaki')
        param_id = self._paramid('comida')
        rv = self._get('/exclui_valor?padraoid=1&\
                              &riscoid=' + param_id + '&valorid=' +
                       str(valor_id))
        data = self.data(rv)
        assert b'temaki' not in data
        assert b'churros' in data

    def test_7_2_excluidepara(self):
        self.login('ajna', 'ajna')
        depara = self._deparaid('iguaria')
        rv = self._get('/exclui_depara?baseid=1&\
                              &tituloid=' + str(depara))
        data = self.data(rv)
        assert data is not None
        # assert b'iguaria' not in data

    def test_8_excluiparametro(self):
        self.login('ajna', 'ajna')
        param_id = self._paramid('comida')
        rv = self._get('/exclui_parametro?padraoid=1&riscoid=' +
                       param_id)
        data = self.data(rv)
        assert b'comida' not in data

    def test_9_juncoes(self):
        self.login('ajna', 'ajna')
        rv = self._get('/juncoes?baseid=1&visaoid=1')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_9_0_adicionavisao_erros(self):
        self.login('ajna', 'ajna')
        rv = self._get('/adiciona_visao?visao_novo=visaotest')
        data = self.data(rv)
        assert b'>visaotest' not in data
        assert b'Selecionar Base' in data
        rv = self._get('/adiciona_visao?baseid=1&visao_novo=')
        data = self.data(rv)
        assert b'Informar nome' in data
        assert b'Selecionar Base' not in data

    def test_9_1_adicionavisao(self):
        self.login('ajna', 'ajna')
        rv = self._get('/adiciona_visao?baseid=1&visao_novo=visaotest')
        data = self.data(rv)
        print(data)
        assert b'>visaotest' in data

    def test_9_2_adicionacoluna(self):
        self.login('ajna', 'ajna')
        visaoid = self._visaoid('visaotest')
        rv = self._get('/adiciona_coluna?visaoid=' + str(visaoid) +
                       '&col_nova=colunatest')
        data = self.data(rv)
        assert b'colunatest' in data

    def test_9_3_adicionatabela(self):
        self.login('ajna', 'ajna')
        visaoid = self._visaoid('visaotest')
        rv = self._get('/adiciona_tabela?visaoid=' + str(visaoid) +
                       '&csv=tabelateste&primario=p&estrangeiro=e&\
                              &pai_id=1&descricao=d')
        data = self.data(rv)
        assert b'primario' in data
        assert b'tabelateste' in data

    def test_9_4_excluicoluna(self):
        self.login('ajna', 'ajna')
        visaoid = self._visaoid('visaotest')
        colunaid = self._colunaid('colunatest')
        rv = self._get('/exclui_coluna?visaoid=' + str(visaoid) + '&\
                              &colunaid=' + str(colunaid))
        data = self.data(rv)
        assert b'colunatest' not in data

    def test_9_5_excluitabela(self):
        self.login('ajna', 'ajna')
        visaoid = self._visaoid('visaotest')
        tabelaid = self._tabelaid('tabelateste')
        rv = self._get('/exclui_tabela?visaoid=' + str(visaoid) +
                       '&tabelaid=' + str(tabelaid))
        data = self.data(rv)
        assert b'tabelateste' not in data

    def test_9_6_excluivisao(self):
        self.login('ajna', 'ajna')
        visaoid = self._visaoid('visaotest')
        rv = self._get('/exclui_visao?visaoid=' + str(visaoid))
        data = self.data(rv)
        assert b'visaotest' not in data

    def test_arquivar(self):
        self.login('ajna', 'ajna')
        rv = self._get(
            '/risco?baseid=5&acao=arquivar')
        data = self.data(rv)
        print(data)
        assert data is not None
