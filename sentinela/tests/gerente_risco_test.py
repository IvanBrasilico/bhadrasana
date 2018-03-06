"""Testes para o módulo gerente_risco"""
import csv
import datetime
import os
import shutil
import tempfile
import unittest
# from pymongo import MongoClient

from sentinela.conf import APP_PATH
from sentinela.models.models import Filtro
from sentinela.utils.gerente_risco import GerenteRisco

CSV_RISCO_TEST = 'sentinela/tests/sample/csv_risco_example.csv'
CSV_NAMEDRISCO_TEST = 'sentinela/tests/sample/csv_namedrisco_example.csv'
CSV_FOLDER_TEST = 'tests/CSV'
CSV_FOLDER_DEST = 'tests/DEST'

CSV_ALIMENTOS = os.path.join(
    APP_PATH, CSV_FOLDER_TEST, 'alimentoseesportes.csv')
CSV_ADITIVOS = os.path.join(
    APP_PATH, CSV_FOLDER_TEST, 'aditivoseaventuras.csv')


# SCH_VIAGENS é um zip
# contendo o conteúdo de csv alimentos e csv aditivos,
# separado por "tabelas":
# viagem: id
# alimento: viagem, alimento
# esporte: viagem, esporte
SAMPLES_DIR = os.path.join('sentinela', 'tests', 'sample')
SCH_VIAGENS = os.path.join(SAMPLES_DIR, 'viagens')


class TestGerenteRisco(unittest.TestCase):
    def setUp(self):
        with open(CSV_RISCO_TEST, 'r', newline='') as f:
            reader = csv.reader(f)
            self.lista = [linha for linha in reader]
        self.gerente = GerenteRisco()
        self.tmpdir = tempfile.mkdtemp()
        # Ensure the file is read/write by the creator only
        self.saved_umask = os.umask(0o077)

    def tearDown(self):
        os.umask(self.saved_umask)

    def test_aplica_igual(self):
        lista = self.lista
        gerente = self.gerente
        bacon = type('ValorParametro', (object, ),
                     {'tipo_filtro': Filtro.igual,
                      'valor': 'bacon'
                      })
        coxinha = type('ValorParametro', (object, ),
                       {'tipo_filtro': Filtro.igual,
                        'valor': 'coxinha'
                        })
        basejump = type('ValorParametro', (object, ),
                        {'tipo_filtro': Filtro.igual,
                         'valor': 'basejump'
                         })
        surf = type('ValorParametro', (object, ),
                    {'tipo_filtro': Filtro.igual,
                     'valor': 'surf'
                     })
        madrugada = type('ValorParametro', (object, ),
                         {'tipo_filtro': Filtro.igual,
                          'valor': 'madrugada'
                          })
        alimentos = type('ParametroRisco', (object, ),
                         {'nome_campo': 'alimento',
                          'valores': [bacon, coxinha]}
                         )
        esportes = type('ParametroRisco', (object, ),
                        {'nome_campo': 'esporte',
                         'valores': [surf, basejump]}
                        )
        horarios = type('ParametroRisco', (object, ),
                        {'nome_campo': 'horario',
                         'valores': [madrugada]}
                        )

        gerente.add_risco(alimentos)
        gerente.add_risco(esportes)
        gerente.add_risco(horarios)
        lista_risco = gerente.aplica_risco(lista)
        print(lista_risco)
        assert len(lista_risco) == 6
        gerente.remove_risco(horarios)
        lista_risco = gerente.aplica_risco(lista)
        print(lista_risco)
        assert len(lista_risco) == 5
        gerente.remove_risco(esportes)
        lista_risco = gerente.aplica_risco(lista)
        print(lista_risco)
        assert len(lista_risco) == 3

    def test_aplica_comeca_com(self):
        lista = self.lista
        gerente = self.gerente
        bacon = type('ValorParametro', (object, ),
                     {'tipo_filtro': Filtro.comeca_com,
                      'valor': 'baco'
                      })
        coxinha = type('ValorParametro', (object, ),
                       {'tipo_filtro': Filtro.comeca_com,
                        'valor': 'cox'
                        })
        basejump = type('ValorParametro', (object, ),
                        {'tipo_filtro': Filtro.comeca_com,
                         'valor': 'base'
                         })
        surf = type('ValorParametro', (object, ),
                    {'tipo_filtro': Filtro.comeca_com,
                     'valor': 'sur'
                     })
        alimentos = type('ParametroRisco', (object, ),
                         {'nome_campo': 'alimento',
                          'valores': [bacon, coxinha]}
                         )
        esportes = type('ParametroRisco', (object, ),
                        {'nome_campo': 'esporte',
                         'valores': [surf, basejump]}
                        )
        gerente.add_risco(alimentos)
        gerente.add_risco(esportes)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 5
        gerente.remove_risco(esportes)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 3

    def test_aplica_contem(self):
        lista = self.lista
        gerente = self.gerente
        bacon = type('ValorParametro', (object, ),
                     {'tipo_filtro': Filtro.contem,
                      'valor': 'aco'
                      })
        coxinha = type('ValorParametro', (object, ),
                       {'tipo_filtro': Filtro.contem,
                        'valor': 'xinh'
                        })
        basejump = type('ValorParametro', (object, ),
                        {'tipo_filtro': Filtro.contem,
                         'valor': 'jump'
                         })
        surf = type('ValorParametro', (object, ),
                    {'tipo_filtro': Filtro.contem,
                     'valor': 'urf'
                     })
        alimentos = type('ParametroRisco', (object, ),
                         {'nome_campo': 'alimento',
                          'valores': [bacon, coxinha]}
                         )
        esportes = type('ParametroRisco', (object, ),
                        {'nome_campo': 'esporte',
                         'valores': [surf, basejump]}
                        )
        gerente.add_risco(alimentos)
        gerente.add_risco(esportes)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 5
        gerente.remove_risco(esportes)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 3

    def test_aplica_namedcsv(self):
        lista = self.lista
        gerente = self.gerente
        gerente.import_named_csv(CSV_NAMEDRISCO_TEST)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 6

    def test_parametrostocsv(self):
        lista = self.lista
        gerente = self.gerente
        gerente.import_named_csv(CSV_NAMEDRISCO_TEST)
        print(lista)
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 6
        gerente.parametros_tocsv()
        gerente.clear_risco()
        gerente.parametros_fromcsv('alimento')
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 3
        gerente.clear_risco()
        gerente.parametros_fromcsv('esporte')
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 3
        gerente.clear_risco()
        gerente.parametros_fromcsv('horario')
        lista_risco = gerente.aplica_risco(lista)
        assert len(lista_risco) == 2

    def test_juntacsv(self):
        gerente = self.gerente
        autores = type('Tabela', (object, ),
                       {'csv': 'autores',
                        'primario': 'id',
                        'estrangeiro': 'livroid',
                        'csv_file': 'autores.csv'
                        })
        livro = type('Tabela', (object, ),
                     {'csv': 'livros',
                      'primario': 'id',
                      'filhos': [autores],
                      'csv_file': 'livros.csv'
                      })
        autores_livro = type('Visao', (object, ),
                             {'nome': 'autores_livro',
                              'tabelas': [livro, autores],
                              'colunas': []
                              })
        sub_capitulos = type('Tabela', (object, ),
                             {'csv': 'subcapitulos',
                              'primario': 'id',
                              'estrangeiro': 'capituloid',
                              'type': 'outer',
                              'csv_file': 'subcapitulos.csv'
                              })
        capitulos = type('Tabela', (object, ),
                         {'csv': 'capitulos',
                          'primario': 'id',
                          'estrangeiro': 'livroid',
                          'filhos': [sub_capitulos],
                          'csv_file': 'capitulos.csv'
                          })
        capitulos_livro = type('Visao', (object, ),
                               {'nome': 'capitulos_livro',
                                'tabelas': [livro, capitulos, sub_capitulos],
                                'colunas': []
                                })
        path = 'sentinela/tests/juncoes'
        result = gerente.aplica_juncao(autores_livro, path=path)
        print(result)
        assert len(result) == 4
        # result = gerente.aplica_juncao(capitulos_livro, path=path)
        # print(result)
        # assert len(result) == 9
        # print(result)
        # assert False  # Uncomment to view output

    def test_headers(self):
        gerente = self.gerente
        headers = gerente.get_headers_base(
            1, os.path.join(APP_PATH, CSV_FOLDER_TEST))
        print(headers)
        assert len(headers) == 24
        assert isinstance(headers, set)

    def test_importa_base(self):
        gerente = self.gerente
        data = datetime.date.today().strftime('%Y-%m-%d')
        if os.path.exists(CSV_FOLDER_DEST):
            shutil.rmtree(CSV_FOLDER_DEST)
        gerente.importa_base(CSV_FOLDER_DEST,
                             '1',
                             data,
                             CSV_ALIMENTOS)
        gerente.importa_base(CSV_FOLDER_DEST,
                             '2',
                             data,
                             CSV_ADITIVOS)
        assert os.path.isfile(os.path.join(CSV_FOLDER_DEST, '1',
                                           data[:4], data[5:7], data[8:10],
                                           os.path.basename(CSV_ALIMENTOS)))
        assert os.path.isfile(os.path.join(CSV_FOLDER_DEST, '2',
                                           data[:4], data[5:7], data[8:10],
                                           os.path.basename(CSV_ADITIVOS)))
        with self.assertRaises(FileExistsError):
            gerente.importa_base(CSV_FOLDER_DEST,
                                 '2',
                                 data,
                                 CSV_ADITIVOS)
        data = data[:8] + '00'
        with self.assertRaises(ValueError):
            gerente.importa_base(CSV_FOLDER_DEST,
                                 '1',
                                 data,
                                 CSV_ADITIVOS)
        shutil.rmtree(CSV_FOLDER_DEST)
