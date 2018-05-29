"""Histórias de Usuário e testes.

Este módulo contém os Histórias de Usuário e testes funcionais
que testam o comportamento descrito no Caso de Usuário

A classe SeleniumTestCase implementa um Servidor de Testes
para utilização e métodos reutilizáveis para os testes de cada
História de Usuário

Obs:

tox precisa ser configurado com passenv = * para selenium funcionar no venv tox
"""
import time

from flask_testing import LiveServerTestCase
from pymongo import MongoClient
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.flask.login import DBUser
# from selenium.webdriver.common.keys import Keys
from bhadrasana.models.models import (Base, BaseOrigem, Filtro, MySession,
                                      PadraoRisco, ParametroRisco, Tabela,
                                      ValorParametro, Visao)
from bhadrasana.views import configure_app

MAX_WAIT = 5


class AppSingleTon():
    """Singleton para evitar que aplicação seja instanciada mais de uma vez."""

    app = None

    @classmethod
    def create(cls, dbsession, mongodb):
        """Retorna aplicação existente. Chama configure_app na primeira vez."""
        if cls.app is None:
            cls.app = configure_app(dbsession, mongodb)
        return cls.app


def wait(function):
    """Decorador para espera do selenium."""
    def modified_function(*args, **kwargs):
        """Tenta executar função até MAX_WAIT."""
        start_time = time.time()
        while True:
            try:
                return function(*args, **kwargs)
            except (AssertionError, WebDriverException) as err:
                if time.time() - start_time > MAX_WAIT:
                    raise err
                time.sleep(0.5)
    return modified_function


def create_base_testes(dbsession):
    """Cria base para testes funcionais."""
    base1 = BaseOrigem('alimentos_e_esportes')
    dbsession.add(base1)
    dbsession.commit()
    risco1 = PadraoRisco('perigo', base1)
    dbsession.add(risco1)
    dbsession.commit()
    param1 = ParametroRisco('alimento', 'teste1', risco1)
    param2 = ParametroRisco('esporte', 'teste2', risco1)
    dbsession.add(param1)
    dbsession.add(param2)
    dbsession.commit()
    valor1 = ValorParametro('bacon', Filtro.igual, param1)
    valor2 = ValorParametro('base jump', Filtro.igual, param2)
    dbsession.add(valor1)
    dbsession.add(valor2)
    dbsession.commit()
    visao1 = Visao('viagens')
    dbsession.add(visao1)
    dbsession.commit()
    tabela1 = Tabela('viagens', 'viagem', '', 0, visao1.id)
    tabela2 = Tabela('alimentos', 'alimento', 'viagem', tabela1.id, visao1.id)
    tabela3 = Tabela('esportes', 'esporte', 'viagem', tabela1.id, visao1.id)
    dbsession.add(tabela1)
    dbsession.add(tabela2)
    dbsession.add(tabela3)
    dbsession.commit()


class SeleniumTestCase(LiveServerTestCase):
    """Classe base para testes funcionais."""

    def create_app(self):
        """Requisito do LiveServerTestCase. Retorna aplicação a ser testada.

        Está sendo criado um BD de testes, na memória.
        Deve-ser tomar cuidado ao modificar, pois a classe recria e APAGA
        o BD dbsession a cada teste.

        """
        mysession = MySession(Base, test=True)
        self.dbsession = mysession.session
        self.engine = mysession.engine
        conn = MongoClient(host=MONGODB_URI)
        self.mongodb = conn[DATABASE]
        app = AppSingleTon.create(self.dbsession, self.mongodb)
        DBUser.dbsession = None # Bypass authentication
        app.config.update(
            LIVESERVER_PORT=8942
        )
        return app

    def create_database(self):
        """Inicializa BD."""
        Base.metadata.create_all(self.engine)
        create_base_testes(self.dbsession)

    def drop_database(self):
        """**APAGA*** BD."""
        Base.metadata.drop_all(self.engine)

    def setUp(self):
        """Inicializa objetos."""
        self.create_database()
        try:
            self.driver = webdriver.Firefox()
        except Exception as err:
            print(err)

    def tearDown(self):
        """Limpa ambiente."""
        self.drop_database()
        self.driver.close()

    @wait
    def wait_fn(self, function):
        """Função que usa o decorador wait."""
        return function()

    @wait
    def wait_for_row_in_table(self, table_id, row_text):
        """Espera por uma tabela até row_text estar nela ou timeout."""
        table = self.driver.find_element_by_id(table_id)
        rows = table.find_elements_by_tag_name('tr')
        self.assertIn(row_text, [row.text for row in rows])

    def get_item_id(self, item_id):
        """Retorna item com id = item_id. Para uso opcional com wait_fn."""
        return self.driver.find_element_by_id(item_id)

    @wait
    def wait_to_be_logged_in(self):
        """Espera até opção "Sair"(logout) estar na tela ou timeout."""
        self.driver.find_element_by_link_text('Sair')

    @wait
    def wait_to_be_logged_out(self):
        """Espera até opção "Sair"(logout) não estar na tela ou timeout."""
        navbar = self.driver.find_element_by_css_selector('.navbar')
        print('NAVBAR TEXT', navbar.text)
        self.assertNotIn('Sair', navbar.text)
