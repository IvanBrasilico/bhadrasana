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
import unittest

from flask_testing import LiveServerTestCase
from pymongo import MongoClient
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from bhadrasana.views import configure_app
from bhadrasana.models.models import Base, MySession

MAX_WAIT = 10
# from selenium.webdriver.common.keys import Keys


class AppSingleTon():
    app = None

    @classmethod
    def create(cls, dbsession):
        if cls.app is None:
            cls.app = configure_app(dbsession, None)
        return cls.app


class SeleniumTestCase(LiveServerTestCase):

    def create_app(self):
        mysession = MySession(Base, test=True)
        dbsession = mysession.session
        self.engine = mysession.engine
        conn = MongoClient(host=MONGODB_URI)
        mongodb = conn[DATABASE]
        app = AppSingleTon.create(dbsession)
        return app

    def create_database(self):
        Base.metadata.create_all(self.engine)

    def drop_database(self):
        Base.metadata.drop_all(self.engine)

    def setUp(self):
        try:
            self.driver = webdriver.Firefox()
        except Exception as err:
            print(err)
        self.create_database()

    def tearDown(self):
        self.driver.close()
        self.drop_database()

    def wait_fn(self, function):
        start_time = time.time()
        while True:
            try:
                return function()
            except (AssertionError, WebDriverException) as err:
                if time.time() - start_time > MAX_WAIT:
                    raise err
                time.sleep(0.5)

    def test_server_is_up_and_running(self):

        response = request.urlopen(self.get_server_url())
        self.assertEqual(response.code, 200)

    def test_tela_de_login(self):
        def test_username():
            for names in ['username', 'senha']:
                element = driver.find_element_by_name(names)
                assert element is not None
                element.send_keys('ajna')

        driver = self.driver
        driver.get(self.get_server_url())
        self.wait_fn(test_username)
        driver.find_element_by_id('btnlogin').click()


if __name__ == '__name__':
    unittest.main()
