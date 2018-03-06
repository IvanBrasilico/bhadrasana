import os
import threading
import unittest
from selenium import webdriver
# from selenium.webdriver.common.keys import Keys

import sentinela.app as app


class SeleniumTestCase(unittest.TestCase):

    def setUp(self):
        """ Obter o geckodriver no site
        https://github.com/mozilla/geckodriver/releases
        docs
        http://selenium-python.readthedocs.io/locating-elements.html"""
        try:
            self.driver = webdriver.Firefox()
        except Exception as err:
            print(err)

        # skip these tests if the browser could not be started
        if self.driver:
            self.app = app.app
            self.app_context = self.app.app_context()
            self.app_context.push()

            # start the Flask server in a thread
            self.server_thread = threading.Thread(target=self.app.run,
                                                  kwargs={'debug': False})
            self.server_thread.start()

    def tearDown(self):
        self.driver.close()

    def test_login(self):
        driver = self.driver
        driver.get('http://localhost:5000/')
        driver.find_element_by_name('username').send_keys('ajna')
        driver.find_element_by_name('senha').send_keys('ajna')
        driver.find_element_by_id('btnlogin').click()

    def test_importa_aplica(self):
        driver = self.driver
        driver.get('http://localhost:5000/list_files')
        driver.find_element_by_id(
            'upload').send_keys(os.getcwd() + '/teste.csv')
        driver.find_element_by_name('btnenviar').click()
        driver.find_element_by_name('teste.csv').click()
        driver.implicity_wait(30)
        # o que é xpath?
        # driver.find_element_by_css_selector('p.content')


if __name__ == '__name__':
    unittest.main()
