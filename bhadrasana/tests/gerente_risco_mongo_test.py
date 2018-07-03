"""Unit tests para self.gerente_risco nos métodos que usam MongoDB.

Cria banco de dados com dados fabricados para testes no
método setUp. Limpa tudo no final no método tearDown. Chamar via pytest ou tox

Chamar python virasana/tests/integracao_test.py criará o Banco de Dados
SEM apagar tudo no final. Para inspeção visual do BD criado para testes.

"""
import unittest

# import pprint
from pymongo import MongoClient

from bhadrasana.models.models import Filtro
from bhadrasana.utils.gerente_risco import GerenteRisco


class TestCase(unittest.TestCase):
    def setUp(self):
        db = MongoClient()['unit_test']
        self.db = db
        # Cria documentos simulando registros importados do CARGA
        db['CARGA.Container'].insert(
            {'container': 'cheio',
             'conhecimento': '1',
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert(
            {'container': 'cheio2',
             'conhecimento': '2',
             'item': 1,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Conhecimento'].insert({'conhecimento': '1', 'tipo': 'mbl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': '2', 'tipo': 'bl'})
        db['CARGA.NCM'].insert({'conhecimento': '1', 'item': '1', 'ncm': '1'})
        db['CARGA.NCM'].insert({'conhecimento': '2', 'item': '1', 'ncm': '2'})
        db['CARGA.NCM'].insert({'conhecimento': '2', 'item': '2', 'ncm': '3'})
        self.gerente = GerenteRisco()
        self.carga = type('Base', (object, ),
                          {'nome': 'CARGA'})
        self.conhecimentos = type('Tabela', (object, ),
                                  {'csv_table': 'Conhecimento',
                                   'primario': 'conhecimento',
                                   'estrangeiro': 'conhecimento',
                                   })
        self.containers = type('Tabela', (object, ),
                               {'csv_table': 'Container',
                                'primario': 'conhecimento',
                                'estrangeiro': 'conhecimento',
                                })
        self.ncms = type('Tabela', (object, ),
                         {'csv_table': 'NCM',
                          'primario': 'conhecimento',
                          'estrangeiro': 'conhecimento',
                          })
        self.containers_conhecimento_ncms = type('Visao', (object, ),
                                                 {'nome': 'ncms_conhecimento',
                                                  'base': self.carga,
                                                  'tabelas':
                                                  [self.containers,
                                                   self.conhecimentos,
                                                   self.ncms],
                                                  'colunas': []
                                                  })

    def tearDown(self):
        db = self.db
        db['CARGA.NCM'].drop()
        db['CARGA.Conhecimento'].drop()
        db['CARGA.Container'].drop()
        db['CARGA.ContainerVazio'].drop()

    def test_gerente_load_mongo(self):
        lista = self.gerente.load_mongo(
            self.db, collection_name='CARGA.Container')
        assert len(lista) == 2

    def test_gerente_juncao1(self):
        # Teste com 1 tabela
        containers_visao = type('Visao', (object, ),
                                {'nome': 'containers',
                                 'base': self.carga,
                                 'tabelas': [self.containers],
                                 'colunas': []
                                 })
        lista = self.gerente.aplica_juncao_mongo(self.db,
                                                 containers_visao,
                                                 filtrar=False)
        assert len(lista) == 3

    def test_gerente_juncao2(self):
        # Teste com 2 tabelas
        containers_conhecimento = type('Visao', (object, ),
                                       {'nome': 'containers_conhecimento',
                                        'base': self.carga,
                                        'tabelas': [self.containers,
                                                    self.conhecimentos],
                                        'colunas': []
                                        })
        lista = self.gerente.aplica_juncao_mongo(self.db,
                                                 containers_conhecimento,
                                                 filtrar=False)
        assert len(lista) == 3

    def test_gerente_juncao3(self):
        # Teste com 3 tabelas
        lista = self.gerente.aplica_juncao_mongo(
            self.db,
            self.containers_conhecimento_ncms,
            filtrar=False)
        assert len(lista) == 4

    def test_gerente_juncao_campos(self):
        # Teste com 3 tabelas e lista de campos
        coluna1 = type('Visao', (object, ),
                       {'nome': 'conhecimento'})
        coluna2 = type('Visao', (object, ),
                       {'nome': 'container'})
        self.containers_conhecimento_ncms.colunas = [coluna1, coluna2]
        lista = self.gerente.aplica_juncao_mongo(
            self.db,
            self.containers_conhecimento_ncms,
            filtrar=False)
        # pprint.pprint(lista)
        assert len(lista) == 4
        assert len(lista[0]) == 4

    def test_gerente_juncao_filtro1(self):
        # Teste com 3 tabelas e filtro (risco)
        cheio = type('ValorParametro', (object, ),
                     {'tipo_filtro': Filtro.igual,
                      'valor': 'cheio'
                      })
        risco_cheio = type('ParametroRisco', (object, ),
                           {'nome_campo': 'container',
                            'valores': [cheio]}
                           )
        ncm = type('ValorParametro', (object, ),
                   {'tipo_filtro': Filtro.igual,
                    'valor': '3'
                    })
        risco_ncm = type('ParametroRisco', (object, ),
                         {'nome_campo': 'ncm',
                          'valores': [ncm]}
                         )
        self.gerente.add_risco(risco_cheio)
        self.gerente.add_risco(risco_ncm)
        lista = self.gerente.aplica_juncao_mongo(
            self.db,
            self.containers_conhecimento_ncms,
            filtrar=True)
        print('LISTA', lista)
        assert len(lista) == 3

    def test_gerente_juncao_filtro2(self):
        cheio = type('ValorParametro', (object, ),
                     {'tipo_filtro': Filtro.igual,
                      'valor': 'cheio'
                      })
        risco_cheio = type('ParametroRisco', (object, ),
                           {'nome_campo': 'container',
                            'valores': [cheio]}
                           )
        container_numero_cheio = type('PadraoRisco', (object, ),
                                      {'nome': 'dummy',
                                       'parametros': [risco_cheio]}
                                      )

        self.gerente.set_padraorisco(container_numero_cheio)
        lista = self.gerente.aplica_juncao_mongo(
            self.db,
            self.containers_conhecimento_ncms,
            filtrar=True)
        print('LISTA', lista)
        assert len(lista) == 2


# Chamar python bhadrasana/tests/gerente_risco_mongo_test.py criará o Banco
# SEM apagar tudo no final. Para inspeção visual do BD criado para testes.
if __name__ == '__main__':
    print('Criando banco unit_test e Dados...')
    testdb = TestCase()
    testdb.setUp()
