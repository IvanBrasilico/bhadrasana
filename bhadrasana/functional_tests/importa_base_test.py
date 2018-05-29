"""História Importar Base.

COMO Usuário comum do sistema,
PRECISO iniciar meu trabalho importando dados
QUE são um ou mais arquivos csv (relacionados se vários)
    ou zips com arquivos sch (no caso da base CARGA)

"""
import unittest
from io import BytesIO

from flask import url_for

from selenium_test import SeleniumTestCase, wait


class ImportarBase(SeleniumTestCase):
    """Teste funcional/aceitação da tela Importa Base."""

    @wait
    def test_tela_correta(self):
        """Testa se carrega a tela esperada."""
        self.driver.get(self.get_server_url() + '/importa_base')
        assert url_for('index') in self.driver.current_url

    def upload(self):
        file = {
            'file':
                (BytesIO(b'alimento, esporte\n' +
                         'temaki, corrida' +
                         'couve, base jump' +
                         'bacon, tai chi chuan' +
                         'alface, motociclismo'),
                 'plan_test.csv'),
            'baseid': '4',
            'data': '2018-01-01'
        }
        self.driver.post('/importa_base', data=file, follow_redirects=True)


if __name__ == '__name__':
    unittest.main()
