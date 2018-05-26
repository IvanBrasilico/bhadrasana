import os
import threading
import unittest
import time

from flask_testing import LiveServerTestCase
from selenium import webdriver

import bhadrasana.app as app


# from selenium.webdriver.common.keys import Keys


class SeleniumTestCase(LiveServerTestCase):

    def create_app(self):
        # config_name = 'testing'
        # app = create_app(config_name)
        aapp = app.app
        aapp.config.update(
            DEBUG=False
        )
        return aapp

    def setUp(self):
        try:
            self.driver = webdriver.Firefox()
        except Exception as err:
            print(err)

    def tearDown(self):
        self.driver.close()

    def test_login(self):
        driver = self.driver
        driver.get(self.get_server_url())
        time.sleep(2)
        driver.find_element_by_name('username').send_keys('ajna')
        driver.find_element_by_name('senha').send_keys('ajna')
        driver.find_element_by_id('btnlogin').click()

    """
    
    def test_importa_aplica(self):
        driver = self.driver
        driver.get('http://localhost:5000/list_files')
        driver.find_element_by_id(
            'upload').send_keys(os.getcwd() + '/teste.csv')
        driver.find_element_by_name('btnenviar').click()
        driver.find_element_by_name('teste.csv').click()
        # driver.implicity_wait(30)
        # o que é xpath?
        # driver.find_element_by_css_selector('p.content')
    """


if __name__ == '__name__':
    unittest.main()
