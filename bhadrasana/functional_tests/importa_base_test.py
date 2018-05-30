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

    def set_planilha(self):
        """Cria planilha para testes."""
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
        input_file = driver.find_element_by_id('planilha')
        input_file.send_keys(planilha)

    def carrega_tela(self):
        """Usuário clica em importa_base e a tela esperada é carregada."""
        self.driver.get(self.get_server_url() + url_for('importa_base'))
        assert url_for('importa_base') in self.driver.current_url

    def test_upload(self):
        """Usuário informa arquivo e base e clica em importar.

        Sistema carrega a tela 'risco' em seguida se tudo OK.
        """
        driver = self.driver
        self.carrega_tela()
        self.set_planilha()
        select = Select(driver.find_element_by_name('baseid'))
        select.select_by_visible_text('alimentos_e_esportes')
        button = driver.find_element_by_name('btnimporta')
        button.click()
        self.wait_fn(
            lambda: self.assertIn(url_for('risco'),
                                  self.driver.current_url)
        )

    def test_upload_sem_base(self):
        """Usuário informa arquivo e clica em importar.

        Sistema informa que falta informar a base.
        """
        driver = self.driver
        self.carrega_tela()
        self.make_planilha()
        button = driver.find_element_by_name('btnimporta')
        button.click()

    def test_upload_sem_planilha(self):
        """Usuário informa arquivo e clica em importar.

        Sistema informa que falta informar a planilha.
        """
        driver = self.driver
        self.carrega_tela()
        select = Select(driver.find_element_by_name('baseid'))
        select.select_by_visible_text('alimentos_e_esportes')
        button = driver.find_element_by_name('btnimporta')
        button.click()

    def test_inclui_base(self):
        """Usuário vê que não tem opção para base que deseja importar.

        Então, este digita o nome da base e clica em "Inclui Base" para
        inserir esta base nas opções.

        O sistema valida se o campo estiver em branco.

        """
        # driver = self.driver
        self.carrega_tela()


if __name__ == '__name__':
    unittest.main()
