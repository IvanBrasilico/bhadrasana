"""História Importar Base.

COMO Usuário comum do sistema,
PRECISO iniciar meu trabalho importando dados
QUE são um ou mais arquivos csv (relacionados se vários)
    ou zips com arquivos sch (no caso da base CARGA)

"""
import unittest
from selenium_test import SeleniumTestCase


class ImportarBase(SeleniumTestCase):
    def test_tela_correta(self):
        pass


if __name__ == '__name__':
    unittest.main()
