"""
Apenas testa se Servidor no ar.

Testa se Servidor no ar e se apresenta tela de login

"""
from urllib import request

from selenium_test import SeleniumTestCase


class ServerUp(SeleniumTestCase):

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
