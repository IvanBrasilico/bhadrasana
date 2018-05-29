"""
Apenas testa se Servidor no ar.

Testa se Servidor no ar e se apresenta tela de login

"""
import unittest
from urllib import request

from flask import url_for

from selenium_test import SeleniumTestCase


class ServerUp(SeleniumTestCase):
    """Testa se SeleniumTestCase consegue iniciar Servidor e tela inicial."""

    def test_server_is_up_and_running(self):
        """Testar se página home responde."""
        response = request.urlopen(self.get_server_url())
        self.assertEqual(response.code, 200)

    def test_tela_de_login(self):
        """Ver se tela de login é exibida e se aceita entradas.

        Fazer login e depois logoout.

        """
        def test_username():
            for names in ['username', 'senha']:
                element = driver.find_element_by_name(names)
                assert element is not None
                element.send_keys('ajna')

        driver = self.driver
        driver.get(self.get_server_url())
        self.wait_fn(test_username)
        driver.find_element_by_id('btnlogin').click()
        self.wait_to_be_logged_in()
        assert url_for('index') in self.driver.current_url
        driver.find_element_by_link_text('Sair').click()
        self.wait_to_be_logged_out()


if __name__ == '__name__':
    unittest.main()
