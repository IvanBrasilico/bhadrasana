"""Tests and documents use of the Web UI
Any client must make this type of request to Web UI
Made from Flask testing docs
http://flask.pocoo.org/docs/0.12/testing/
"""
import os
import unittest
from io import BytesIO

import sentinela.app as app


class FlaskTestCase(unittest.TestCase):

    def setUp(self):
        # Ativar esta variável de ambiente na inicialização
        # do Servidor WEB para transformar em teste de integração
        self.http_server = os.environ.get('HTTP_SERVER')
        # TODO: Pesquisar método para testar mesmo com CSRF habilitado
        # https://gist.github.com/singingwolfboy/2fca1de64950d5dfed72

        if self.http_server is not None:
            from webtest import TestApp
            self.app = TestApp(self.http_server)
        else:
            app.app.testing = True
            app.app.config['WTF_CSRF_ENABLED'] = False
            self.app = app.app.test_client()
            app.DBUser.dbsession = None  # Bypass authentication
        rv = self.login('ajna', 'ajna')
        assert rv is not None

    def tearDown(self):
        rv = self.logout()
        assert rv is not None

    def login(self, username, senha):
        if self.http_server is not None:
            # First, get the CSRF Token
            response = self.app.get('/login')
            self.csrf_token = str(response.html.find_all(
                attrs={'name': 'csrf_token'})[0])
            begin = self.csrf_token.find('value="') + 7
            end = self.csrf_token.find('"/>')
            self.csrf_token = self.csrf_token[begin: end]
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
                senha=senha
            ), follow_redirects=True)

    def logout(self):
        if self.http_server is not None:
            return self.app.get('/logout',
                                params=dict(csrf_token=self.csrf_token))
        else:
            return self.app.get('/logout', follow_redirects=True)

    def test_not_found(self):
        if self.http_server is None:
            rv = self.app.get('/non_ecsiste')
            assert b'404 Not Found' in rv.data

    def data(self, rv):
        if self.http_server is not None:
            return str(rv.html).encode('utf_8')
        return rv.data

    def test_1_home(self):
        if self.http_server is not None:
            rv = self.app.get('/',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_2_importabase(self):
        if self.http_server is not None:
            rv = self.app.get('/importa_base',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/importa_base?baseid=4')
        data = self.data(rv)
        assert b'AJNA' in data
        
    def test_3_upload(self):
        file = {
            'file': (BytesIO(b'iguaria, esporte\n temaki, corrida'), 'plan_test.csv'),
            'baseid': '4',
            'data': '2018-01-01'
        }
        rv = self._post('/importa_base', data=file, follow_redirects=True
        )
        data = self.data(rv)
        print(data)
        assert b'clicar em submeter!' not in data
        assert b'Escolha Base' in data

    """def test_n_adicionapadraorisco(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_padrao/<>',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_padrao/<>')
        data = self.data(rv)
        assert b'Lista de Riscos' in data"""

    def test_4_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/risco?baseid=4',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/risco?baseid=4')
        data = self.data(rv)
        assert b'Lista de Riscos' in data

    def test_5_editarisco(self):
        if self.http_server is not None:
            rv = self.app.get('/edita_risco?padraoid=4&riscoid=26',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/edita_risco?padraoid=4&riscoid=26')
        data = self.data(rv)
        print(data)
        assert b'AJNA' in data

    def test_6_adicionaparametro(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_parametro?padraoid=4&\
                              &risco_novo=comida',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_parametro?padraoid=4&risco_novo=comida')
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def _paramid(self, nome):
        parametro = app.dbsession.query(app.ParametroRisco).filter(
            app.ParametroRisco.nome_campo == nome).first()
        return parametro.id

    def test_7_adicionavalor(self):
        parametro = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/adiciona_valor?padraoid=4&riscoid='+str(parametro)+'&\
                              &novo_valor=temaki&filtro=igual',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/adiciona_valor?padraoid=4&riscoid='+ str(parametro) +
                '&novo_valor=temaki&filtro=igual')
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    # importa csv
    def test_8_importacsv(self):
        parametro = self._paramid('comida')
        file = {
            'csv': (BytesIO(
                b'valor, tipo_filtro\nhot roll, igual\nchurros, igual'
                ), 'alimento.csv')
        }
        if self.http_server is not None:
            rv = self.app.get('/importa_csv/4/'+ str(parametro),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self._post(
                '/importa_csv/4/'+str(parametro)
                , data=file, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data

    def test_9_exportacsv(self):
        parametro = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/exporta_csv?padraoid=4&riscoid=' + str(parametro),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exporta_csv?padraoid=4&riscoid=' + str(parametro))
        data = self.data(rv)
        assert b'Redirecting...' in data

    # editar depara
    def test_a_editadepara(self):
        if self.http_server is not None:
            rv = self.app.get('/edita_depara?baseid=4',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/edita_depara?baseid=4')
        data = self.data(rv)
        print(data)
        assert b'AJNA' in data

    def test_b_adicionadepara(self):
        if self.http_server is not None:
            rv = self.app.get(
                '/adiciona_depara?baseid=4&\
                &antigo=iguaria&novo=alimento',
                params=dict(csrf_token=self.csrf_token)
            )
        else:
            rv = self.app.get(
                '/adiciona_depara?baseid=4&antigo=iguaria&novo=alimento'
            )
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def test_c_aplica_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/aplica_risco?&baseid=4&padraoid=4&visaoid=0&\
                               &parametroid=24&filename=2018/01/01&parametros_ativos=comida',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/aplica_risco?&baseid=4&padraoid=4&visaoid=0&\
                               &parametroid=24&filename=2018/01/01&parametros_ativos=comida')
        data = self.data(rv)
        assert b'Lista de Riscos da Base None' not in data

    def test_exclui_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/aplica_risco?&baseid=4&filename=2018/01/01&acao=excluir',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/aplica_risco?&baseid=4&filename=2018/01/01&acao=excluir')
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def _post(self, url, data, follow_redirects=True):
        if self.http_server is not None:
            data['csrf_token'] = self.csrf_token
            rv = self.app.post(url, params=data)
        else:
            rv = self.app.post(url, data=data,
                               follow_redirects=follow_redirects)
        return rv

    """def test_importacsv(self):
        if self.http_server is not None:
            rv = self.app.get('/importa_csv/1/1',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/importa_csv/4/26')
        data = self.data(rv)
        assert b'Redirecting...' in data
        rv = self._post(
            '/importa_csv/1/1', data={'file': ''}, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data
        file = {
            'csv': (BytesIO(b'FILE CONTENT'), 'test.csv')
        }
        rv = self._post(
            '/importa_csv/1/1', data=file, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data
        file = {
            'csv': (BytesIO(b'FILE CONTENT'), '')
        }
        rv = self._post(
            '/importa_csv/1/1', data=file, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data"""

    def test_excluiparametro(self):
        param = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/exclui_parametro?\
                              padraoid=4&riscoid=' + str(param),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_parametro?\
                              padraoid=4&riscoid=' + str(param))
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def _valorid(self, nome):
        valor = app.dbsession.query(app.ValorParametro).filter(
            app.ValorParametro.valor == nome).first()
        return valor.id

    def test_d_excluivalor(self):
        valor = self._valorid('temaki')
        if self.http_server is not None:
            rv = self.app.get('/exclui_valor?padraoid=4&\
                              &riscoid=26&valorid=' + str(valor),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_valor?padraoid=4&\
                              &riscoid=26&valorid=' + str(valor))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def _deparaid(self, nome):
        depara = app.dbsession.query(app.DePara).filter(
            app.DePara.titulo_ant == nome).first()
        return depara.id

    def test_e_excluidepara(self):
        depara = self._deparaid('iguaria')
        if self.http_server is not None:
            rv = self.app.get('/exclui_depara?baseid=3&\
                              &tituloid=' + str(depara),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_depara?baseid=4&\
                              &tituloid=' + str(depara))
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def test_navegabases(self):
        if self.http_server is not None:
            rv = self.app.get('/navega_bases?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/navega_bases?&selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_adicionafiltro(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_filtro?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_filtro?&selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala&filtro=None&valor=E-01')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_excluifiltro(self):
        if self.http_server is not None:
            rv = self.app.get('/exclui_filtro?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_filtro?&selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala&index=0')
        data = self.data(rv)
        assert b'Redirecting...' in data

    """def test_arvoreteste(self):
        if self.http_server is not None:
            rv = self.app.get('/arvore_teste',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/arvore_teste')
        data = self.data(rv)
        assert b'AJNA' in data"""

    def test_juncoes(self):
        if self.http_server is not None:
            rv = self.app.get('/juncoes?visaoid=1',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/juncoes?visaoid=1')
        data = self.data(rv)
        assert b'AJNA' in data

    """def test_adicionavisao(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_visao?visao_novo=visaotest',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_visao?visao_novo=visaotest')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def _visaoid(self, nome):
        visao = app.dbsession.query(app.Visao).filter(
            app.Visao.nome == nome).first()
        return visao.id

    def test_excluivisao(self):
        visaoid = self._visaoid('visaotest')
        if self.http_server is not None:
            rv = self.app.get('/exclui_visao?visaoid=' + str(visaoid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_visao?visaoid=' + str(visaoid))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_adicionacoluna(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_coluna?visaoid=1&col_nova=colunatest',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_coluna?visaoid=1&col_nova=colunatest')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def _colunaid(self, nome):
        coluna = app.dbsession.query(app.Coluna).filter(
            app.Coluna.nome == nome).first()
        return coluna.id

    def test_excluicoluna(self):
        colunaid = self._colunaid('colunatest')
        if self.http_server is not None:
            rv = self.app.get('/exclui_coluna?visaoid=1&\
                              &colunaid=' + str(colunaid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_coluna?visaoid=1&\
                              &colunaid=' + str(colunaid))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_adicionatabela(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_tabela?visaoid=3&csv=c&\
                              &primario=p&estrangeiro=e&pai_id=1&descricao=d',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_tabela?visaoid=3&csv=c&\
                              &primario=p&estrangeiro=e&pai_id=1&descricao=d')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def _tabelaid(self, nome):
        tabela = app.dbsession.query(app.Tabela).filter(
            app.Tabela.csv == nome).first()
        return tabela.id

    def test_excluitabela(self):
        tabelaid = self._tabelaid('c')
        if self.http_server is not None:
            rv = self.app.get('/exclui_tabela?visaoid=3&tabelaid=' +
                              str(tabelaid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_tabela?visaoid=3&tabelaid=' +
                              str(tabelaid))
        data = self.data(rv)
        assert b'Redirecting...' in data"""

    """def test_arvore(self):
        if self.http_server is not None:
            rv = self.app.get('/arvore?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala&instance_id=E-01',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/arvore?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala&instance_id=E-01')
        data = self.data(rv)
        assert b'AJNA' in data"""

    def test_consultabases(self):
        if self.http_server is not None:
            rv = self.app.get('/consulta_bases_executar?\
                              &selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/consulta_bases_executar?\
                              &selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala')
        data = self.data(rv)
        assert b'AJNA' in data
