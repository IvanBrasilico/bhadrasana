"""Testa se Servidor está no ar e testa formas de login."""
import unittest
from urllib import request

from selenium_test import SeleniumTestCase


class ServerUp(SeleniumTestCase):
    """Testa se SeleniumTestCase consegue iniciar Servidor e tela inicial."""

    def test_server_is_up_and_running(self):
        """Testar se página home responde."""
        response = request.urlopen(self.get_server_url())
        self.assertEqual(response.code, 200)


if __name__ == '__name__':
    unittest.main()
