"""Unit tests para gerente_risco nos métodos que usam MongoDB.

Cria banco de dados com dados fabricados para testes no
método setUp. Limpa tudo no final no método tearDown. Chamar via pytest ou tox

Chamar python virasana/tests/integracao_test.py criará o Banco de Dados
SEM apagar tudo no final. Para inspeção visual do BD criado para testes.

"""
import unittest

# import pprint
from pymongo import MongoClient

from sentinela.models.models import Filtro
from sentinela.utils.gerente_risco import GerenteRisco


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

    def tearDown(self):
        db = self.db
        db['CARGA.NCM'].drop()
        db['CARGA.Conhecimento'].drop()
        db['CARGA.Container'].drop()
        db['CARGA.ContainerVazio'].drop()

    def test_gerente_juncao(self):
        gerente = GerenteRisco()
        carga = type('Base', (object, ),
                     {'nome': 'CARGA'})
        conhecimentos = type('Tabela', (object, ),
                             {'csv': 'Conhecimento',
                              'primario': 'conhecimento',
                              'estrangeiro': 'conhecimento',
                              })
        containers = type('Tabela', (object, ),
                          {'csv': 'Container',
                           'primario': 'conhecimento',
                           'estrangeiro': 'conhecimento',
                           })
        ncms = type('Tabela', (object, ),
                    {'csv': 'NCM',
                     'primario': 'conhecimento',
                     'estrangeiro': 'conhecimento',
                     })
        containers_visao = type('Visao', (object, ),
                                {'nome': 'containers',
                                 'base': carga,
                                 'tabelas': [containers],
                                 'colunas': []
                                 })
        containers_conhecimento = type('Visao', (object, ),
                                       {'nome': 'containers_conhecimento',
                                        'base': carga,
                                        'tabelas': [containers, conhecimentos],
                                        'colunas': []
                                        })
        containers_conhecimento_ncms = type('Visao', (object, ),
                                            {'nome': 'ncms_conhecimento',
                                             'base': carga,
                                             'tabelas': [containers,
                                                         conhecimentos,
                                                         ncms],
                                             'colunas': []
                                             })
        coluna1 = type('Visao', (object, ),
                       {'nome': 'conhecimento'})
        coluna2 = type('Visao', (object, ),
                       {'nome': 'container'})
        # Teste com 1 tabela
        lista = gerente.aplica_juncao_mongo(self.db,
                                            containers_visao,
                                            filtrar=False)
        # pprint.pprint(lista)
        assert len(lista) == 3
        # Teste com 2 tabelas
        lista = gerente.aplica_juncao_mongo(self.db,
                                            containers_conhecimento,
                                            filtrar=False)
        # pprint.pprint(lista)
        assert len(lista) == 3
        # Teste com 3 tabelas
        lista = gerente.aplica_juncao_mongo(self.db,
                                            containers_conhecimento_ncms,
                                            filtrar=False)
        assert len(lista) == 4
        # pprint.pprint(lista)
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
        gerente.add_risco(risco_cheio)
        gerente.add_risco(risco_ncm)
        lista = gerente.aplica_juncao_mongo(self.db,
                                            containers_conhecimento_ncms,
                                            filtrar=True)
        # pprint.pprint(lista)
        assert len(lista) == 3
        # Teste com 3 tabelas e lista de campos
        containers_conhecimento_ncms.colunas = [coluna1, coluna2]
        lista = gerente.aplica_juncao_mongo(self.db,
                                            containers_conhecimento_ncms,
                                            filtrar=False)
        # pprint.pprint(lista)
        assert len(lista) == 4
        assert len(lista[0]) == 4


# Chamar python bhadrasana/tests/gerente_risco_mongo_test.py criará o Banco
# SEM apagar tudo no final. Para inspeção visual do BD criado para testes.
if __name__ == '__main__':
    print('Criando banco unit_test e Dados...')
    testdb = TestCase()
    testdb.setUp()
