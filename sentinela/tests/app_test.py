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
            app.login.DBUser.dbsession = None  # Bypass authentication
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

    # methods
    def data(self, rv):
        if self.http_server is not None:
            return str(rv.html).encode('utf_8')
        return rv.data

    def _post(self, url, data, follow_redirects=True):
        if self.http_server is not None:
            data['csrf_token'] = self.csrf_token
            rv = self.app.post(url, params=data)
        else:
            rv = self.app.post(url, data=data,
                               follow_redirects=follow_redirects)
        return rv

    def _paramid(self, nome):
        parametro = app.dbsession.query(app.ParametroRisco).filter(
            app.ParametroRisco.nome_campo == nome).first()
        return parametro.id

    def _valorid(self, nome):
        valor = app.dbsession.query(app.ValorParametro).filter(
            app.ValorParametro.valor == nome).first()
        return valor.id

    def _deparaid(self, nome):
        depara = app.dbsession.query(app.DePara).filter(
            app.DePara.titulo_ant == nome).first()
        return depara.id

    def _visaoid(self, nome):
        visao = app.dbsession.query(app.Visao).filter(
            app.Visao.nome == nome).first()
        return visao.id

    def _colunaid(self, nome):
        coluna = app.dbsession.query(app.Coluna).filter(
            app.Coluna.nome == nome).first()
        return coluna.id

    def _tabelaid(self, nome):
        tabela = app.dbsession.query(app.Tabela).filter(
            app.Tabela.csv == nome).first()
        return tabela.id

    def _excluibase(self):
        app.dbsession.query(app.BaseOrigem).filter(
            app.BaseOrigem.nome == 'baseteste').delete()
        app.dbsession.commit()

    def _excluipadrao(self):
        app.dbsession.query(app.PadraoRisco).filter(
            app.PadraoRisco.nome == 'padraoteste').delete()
        app.dbsession.commit()

    # adiciona base
    def test_adicionabase(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_base/baseteste',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_base/baseteste')
        data = self.data(rv)
        assert b'Redirecting...' in data
        self._excluibase()

    def test_1_home(self):
        if self.http_server is not None:
            rv = self.app.get('/',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_11_editadepara(self):
        if self.http_server is not None:
            rv = self.app.get('/edita_depara?baseid=4',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/edita_depara?baseid=4')
        data = self.data(rv)
        print(data)
        assert b'AJNA' in data

    def test_12_adicionadepara(self):
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
            'file':
                (BytesIO(b'iguaria, esporte\n temaki, corrida'),
                 'plan_test.csv'),
            'baseid': '4',
            'data': '2018-01-01'
        }
        rv = self._post('/importa_base', data=file, follow_redirects=True
                        )
        data = self.data(rv)
        print(data)
        assert b'clicar em submeter!' not in data
        # assert b'Escolha Base' in data

    def test_adicionapadraorisco(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_padrao/padraoteste',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_padrao/padraoteste')
        data = self.data(rv)
        print(data)
        self._excluipadrao()
        assert b'Redirecting...' in data

    def test_4_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/risco?baseid=4',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/risco?baseid=4')
        data = self.data(rv)
        assert b'Lista de Riscos' in data

    def test_41_editarisco(self):
        if self.http_server is not None:
            rv = self.app.get('/edita_risco?padraoid=4&riscoid=26',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/edita_risco?padraoid=4&riscoid=26')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_42_adicionaparametro(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_parametro?padraoid=4&\
                              &risco_novo=comida',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/adiciona_parametro?padraoid=4&risco_novo=comida&\
                &lista=lista, teste')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_43_adicionavalor(self):
        parametro = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/adiciona_valor?padraoid=4&riscoid=' + str(parametro) + '&\
                              &novo_valor=temaki&filtro=igual',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/adiciona_valor?padraoid=4&riscoid=' + str(parametro) +
                '&novo_valor=temaki&filtro=igual')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_44_valores(self):
        parametro = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/valores?parametroid=' + str(parametro),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/valores?parametroid=' + str(parametro))
        data = self.data(rv)
        assert b'temaki' in data

    # importa csv
    def test_5_importacsv(self):
        parametro = self._paramid('comida')
        file = {
            'csv': (BytesIO(
                b'valor, tipo_filtro\nhot roll, igual\nchurros, igual'
            ), 'alimento.csv')
        }
        if self.http_server is not None:
            rv = self.app.get('/importa_csv/4/' + str(parametro),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self._post(
                '/importa_csv/4/' + str(parametro),
                data=file, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data

    def test_51_exportacsv(self):
        parametro = self._paramid('comida')
        if self.http_server is not None:
            rv = self.app.get('/exporta_csv?padraoid=4&riscoid=' +
                              str(parametro),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/exporta_csv?padraoid=4&riscoid=' + str(parametro))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_6_aplica_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/aplica_risco?&baseid=4&padraoid=4&visaoid=0&\
                               &parametroid=24&filename=2018/01/01&\
                               &parametros_ativos=comida',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/aplica_risco?&filename=2018/01/01&acao=aplicar&baseid=4&padraoid=4&visaoid=0&\
                               &parametros_ativos=comida')
        data = self.data(rv)
        print(data)
        assert b'Lista de Riscos da Base None' not in data

    def test_7_exclui_risco(self):
        if self.http_server is not None:
            rv = self.app.get('/aplica_risco?&baseid=4&\
                              &filename=2018/01/01&acao=excluir',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/aplica_risco?&baseid=4&filename=2018/01/01&acao=excluir')
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    # testes dos erros
    def test_importacsv(self):
        if self.http_server is not None:
            rv = self.app.get('/importa_csv/4/26',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self._post(
                '/importa_csv/4/26', data={'': ''}, follow_redirects=False)
        data = self.data(rv)
        file = {
            'csv': (BytesIO(b'FILE CONTENT'), 'arq.csv')
        }
        rv = self._post(
            '/importa_csv/4/None', data=file, follow_redirects=False)
        data = self.data(rv)
        assert b'Redirecting..' in data

    def test_importabase(self):
        file = {
            'file':
                (BytesIO(b'iguaria, esporte\n temaki, corrida'),
                 'plan_test.csv'),
            'baseid': None,
            'data': '2018-01-01'
        }
        if self.http_server is not None:
            rv = self.app.get('/importa_base',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self._post('/importa_base',
                            data={'file': None},
                            follow_redirects=True
                            )
        data = self.data(rv)
        print(data)
        png = {
            'file': (BytesIO(b'FILE CONTENT'), 'arq.png')
        }
        rv = self._post('/importa_base', data=png, follow_redirects=True)
        data = self.data(rv)
        print(data)
        rv = self._post('/importa_base', data=file, follow_redirects=True)
        data = self.data(rv)
        print(data)
        # assert False

    def test_arquivar(self):
        if self.http_server is not None:
            rv = self.app.get('/aplica_risco?baseid=5&acao=arquivar',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get(
                '/aplica_risco?baseid=5&acao=arquivar')
        data = self.data(rv)
        print(data)

    def test_excluiparametro(self):
        param = self._paramid('comida')
        lista = [self._paramid('lista'), self._paramid('teste')]
        if self.http_server is not None:
            rv = self.app.get('/exclui_parametro?\
                              padraoid=4&riscoid=' + str(param),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_parametro?padraoid=4&\
                              &riscoid=' + str(param))
        data = self.data(rv)
        for i in lista:
            rv = self.app.get('/exclui_parametro?padraoid=4&\
                              &riscoid=' + str(i))
            data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def test_b_excluivalor(self):
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

    def test_c_excluidepara(self):
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

    def test_arvoreteste(self):
        if self.http_server is not None:
            rv = self.app.get('/arvore_teste',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/arvore_teste')
        data = self.data(rv)
        print(data)
        assert b'Escala' in data

    def test_d_juncoes(self):
        if self.http_server is not None:
            rv = self.app.get('/juncoes?baseid=1&visaoid=1',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/juncoes?baseid=1&visaoid=1')
        data = self.data(rv)
        assert b'AJNA' in data

    def test_e_adicionavisao(self):
        if self.http_server is not None:
            rv = self.app.get('/adiciona_visao?visao_novo=visaotest',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_visao?visao_novo=visaotest')
        data = self.data(rv)
        print(data)
        assert b'Redirecting...' in data

    def test_f_adicionacoluna(self):
        visaoid = self._visaoid('visaotest')
        if self.http_server is not None:
            rv = self.app.get('/adiciona_coluna?visaoid=' + str(visaoid) +
                              '&col_nova=colunatest',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_coluna?visaoid=' + str(visaoid) +
                              '&col_nova=colunatest')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_g_adicionatabela(self):
        visaoid = self._visaoid('visaotest')
        if self.http_server is not None:
            rv = self.app.get('/adiciona_tabela?visaoid=' + str(visaoid) +
                              '&csv=c&primario=p&estrangeiro=e&\
                              &pai_id=1&descricao=d',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/adiciona_tabela?visaoid=' + str(visaoid) +
                              '&csv=c&primario=p&estrangeiro=e&\
                              &pai_id=1&descricao=d')
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_h_excluicoluna(self):
        visaoid = self._visaoid('visaotest')
        colunaid = self._colunaid('colunatest')
        if self.http_server is not None:
            rv = self.app.get('/exclui_coluna?visaoid=' + str(visaoid) + '&\
                              &colunaid=' + str(colunaid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_coluna?visaoid=' + str(visaoid) + '&\
                              &colunaid=' + str(colunaid))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_i_excluitabela(self):
        visaoid = self._visaoid('visaotest')
        tabelaid = self._tabelaid('c')
        if self.http_server is not None:
            rv = self.app.get('/exclui_tabela?visaoid=' + str(visaoid) +
                              '&tabelaid=' + str(tabelaid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_tabela?visaoid=' + str(visaoid) +
                              '&tabelaid=' + str(tabelaid))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_j_excluivisao(self):
        visaoid = self._visaoid('visaotest')
        if self.http_server is not None:
            rv = self.app.get('/exclui_visao?visaoid=' + str(visaoid),
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/exclui_visao?visaoid=' + str(visaoid))
        data = self.data(rv)
        assert b'Redirecting...' in data

    def test_arvore(self):
        if self.http_server is not None:
            rv = self.app.get('/arvore?selected_module=carga&\
                              &selected_model=Escala&\
                              &selected_field=Escala&instance_id=E-01',
                              params=dict(csrf_token=self.csrf_token))
        else:
            rv = self.app.get('/arvore?selected_module=carga&\
                              &selected_model=Escala&selected_field=Escala&\
                              &instance_id=E-01')
        data = self.data(rv)
        print(data)
        assert b'odyniec' in data

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
