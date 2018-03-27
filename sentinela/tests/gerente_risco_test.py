"""Testes para o módulo gerente_risco"""
import csv
import datetime
import mongomock
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
CSV_FOLDER_DEST = 'sentinela/tests/DEST'

CSV_ALIMENTOS = os.path.join(
    APP_PATH, CSV_FOLDER_TEST, 'alimentoseesportes.csv')
CSV_ADITIVOS = os.path.join(
    APP_PATH, CSV_FOLDER_TEST, 'aditivoseaventuras.csv')
ZIP = os.path.join(
    APP_PATH, CSV_FOLDER_TEST, 'tests.zip')

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
        self.client = mongomock.MongoClient()
        self.mongodb = self.client['CARGA']
        self.mongodb.collection.drop()
        for _i in 'abx':
            self.mongodb.collection.create_index(
                _i, unique=False, name='idx' + _i,
                sparse=True, background=True)
        self.bulk_op = self.mongodb.collection.initialize_ordered_bulk_op()
        self.tmpdir = tempfile.mkdtemp()
        # Ensure the file is read/write by the creator only
        self.saved_umask = os.umask(0o077)
        self.db = unittest.mock.MagicMock(return_value='OK')

    def tearDown(self):
        os.umask(self.saved_umask)

    def test_criapadrao(self):
        gerente = self.gerente
        gerente.cria_padraorisco('padraorisco', session=self.db)

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
        lista_named_csv = gerente.import_named_csv(CSV_NAMEDRISCO_TEST, tolist=True)
        assert len(lista_named_csv) == 3

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
        nome = type('Tabela', (object, ),
                    {'nome': 'nome'})
        capitulos_livro = type('Visao', (object, ),
                               {'nome': 'capitulos_livro',
                                'tabelas': [livro, capitulos, sub_capitulos],
                                'colunas': [nome]
                                })
        path = 'sentinela/tests/juncoes'
        result = gerente.aplica_juncao(autores_livro, path=path)
        print(result)
        assert len(result) == 4
        result = gerente.aplica_juncao(capitulos_livro, path=path)
        print(result)
        assert len(result) == 9
        gerente.aplica_juncao(capitulos_livro, path=path, filtrar=True)
        # assert False  # Uncomment to view output

    def test_headers(self):
        gerente = self.gerente
        headers = gerente.get_headers_base(
            1, os.path.join(APP_PATH, CSV_FOLDER_TEST))
        assert len(headers) == 4
        assert isinstance(headers, set)
        headers = gerente.get_headers_base(
            1, os.path.join(APP_PATH, CSV_FOLDER_TEST), csvs=True)
        assert len(headers) == 2

    def test_importa_base(self):
        gerente = self.gerente
        data = datetime.date.today().strftime('%Y-%m-%d')
        if os.path.exists(CSV_FOLDER_DEST):
            shutil.rmtree(CSV_FOLDER_DEST)
        gerente.importa_base(CSV_FOLDER_DEST,
                             '3',
                             data,
                             ZIP)
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

    def test_loadmongo(self):
        gerente = self.gerente
        db = self.mongodb
        db.create_collection('CARGA')
        result = gerente.load_mongo(db, collection_name='CARGA.csv')
        print(result)
        # assert False

    def test_fail_tomongo(self):
        gerente = self.gerente
        db = self.db
        base = type('BaseOrigem', (object, ), {
                    'id': '1',
                    'nome': 'baseteste'
                    })
        with self.assertRaises(AttributeError):
            gerente.csv_to_mongo(db, base)
        # gerente.csv_to_mongo(db, base, arquivo=CSV_ALIMENTOS)
        # assert False

    def test_tomongo(self):
        gerente = self.gerente
        db = self.db
        base = type('BaseOrigem', (object, ), {
                    'id': '1',
                    'nome': 'baseteste'
                    })
        gerente.csv_to_mongo(db, base, arquivo=CSV_ALIMENTOS)
        # assert False

    """def test_juntamongo(self):
        gerente = self.gerente
        db = self.mongodb
        db.create_collection('baseteste.livros')
        base = type('Base', (object, ), {
                    'nome': 'baseteste'
                    })
        autores = type('Tabela', (object, ),
                       {'csv': 'autores',
                        'primario': 'id',
                        'estrangeiro': 'livroid',
                        'csv_file': 'autores'
                        })
        livro = type('Tabela', (object, ),
                     {'csv': 'livros',
                      'primario': 'id',
                      'filhos': [autores],
                      'csv_file': 'livros'
                      })
        visao = type('Visao', (object, ), {
                     'base': base,
                     'tabelas': [autores, livro],
                     'colunas': []
                     })
        gerente.aplica_juncao_mongo(db, visao)
        #assert False
        """
