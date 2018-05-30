"""História Importar Base.

COMO Usuário comum do sistema,
PRECISO iniciar meu trabalho importando dados
QUE são um ou mais arquivos csv (relacionados se vários)
    ou zips com arquivos sch (no caso da base CARGA)

"""
import csv
import os
import unittest

from flask import url_for
from selenium.webdriver.support.ui import Select
from selenium_test import SeleniumTestCase

from bhadrasana.conf import ENCODE, tmpdir


class ImportarBase(SeleniumTestCase):
    """Teste funcional/aceitação da tela Importa Base."""

    def test_tela_correta(self):
        """Testa se carrega a tela esperada."""
        self.driver.get(self.get_server_url() + '/importa_base')
        assert url_for('importa_base') in self.driver.current_url

    def test_upload(self):
        lista = [['alimento, esporte'],
                 ['temaki, corrida'],
                 ['couve, base jump'],
                 ['bacon, tai chi chuan'],
                 ['alface, motociclismo'],
                 ]
        planilha = os.path.join(tmpdir, 'plan_test.csv')

        with open(planilha, 'w', encoding=ENCODE, newline='') as out:
            writer = csv.writer(out, quotechar='"', quoting=csv.QUOTE_ALL)
            for row in lista:
                writer.writerow(row)
        driver = self.driver
        self.driver.get(self.get_server_url() + '/importa_base')
        input_file = driver.find_element_by_id('planilha')
        input_file.send_keys(planilha)
        select = Select(driver.find_element_by_name('baseid'))
        select.select_by_visible_text('alimentos_e_esportes')
        button = driver.find_element_by_name('btnimporta')
        button.click()
        self.wait_fn(lambda: self.assertIn(url_for('risco'), self.driver.current_url))


if __name__ == '__name__':
    unittest.main()
