"""Celery tasks.

Tasks/workers que rodarão à parte, em um processo Celery.
Todas as tarefas que demandem tempo/carga de CPU ou I/O devem ser colocadas
preferencialmente no processo Celery, sendo executadas de forma assíncrona/
background.
"""
from datetime import datetime
import os
import shutil
from celery import Celery, states
from pymongo import MongoClient

from ajna_commons.flask.conf import BACKEND, BROKER
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import ascii_sanitizar
from bhadrasana.utils.gerente_risco import GerenteRisco
from bhadrasana.models.models import (Base, BaseOrigem, MySession)

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)

mysession = MySession(Base)
dbsession = mysession.session
engine = mysession.engine


@celery.task(bind=True)
def importar_base(self, csv_folder, baseid, data, filename, remove=False):
    """Função para upload do arquivo de uma extração ou outra fonte externa.

    Utiliza o :class: `bhadrasana.utils.gerenterisco.GerenteRisco`.
    Suporte por ora para csv com títulos e zip com sch (padrão Carga)
    Ver também :func: `bhadrasana.app.importa_base`

    Args:
        csv_folder: caminho onde será salvo o resultado

        abase: Base de Origem do arquivo

        data: data inicial do período extraído (se não for passada,
        assume hoje)

        filename: arquivo csv, sch+txt, ou conjunto deles em formato zip

        remove: exclui arquivo original no final do processamento
    """
    basefilename = os.path.basename(filename)
    self.update_state(state=states.STARTED,
                      meta={'status': 'Processando arquivo ' + basefilename +
                            '. Aguarde!!!'})
    gerente = GerenteRisco()
    try:
        abase = dbsession.query(BaseOrigem).filter(
            BaseOrigem.id == baseid).first()
        self.update_state(state=states.PENDING,
                          meta={'status': 'Processando arquivo ' +
                                basefilename + ' na base ' + abase.nome +
                                '. Aguarde!!!'})
        lista_arquivos = gerente.importa_base(
            csv_folder, baseid, data, filename, remove)
        # Sanitizar base já na importação para evitar
        # processamento repetido depois
        gerente.ativa_sanitizacao(ascii_sanitizar)
        gerente.checa_depara(abase)  # Aplicar na importação???
        gerente.pre_processa_arquivos(lista_arquivos)
        return {'status': 'Base importada com sucesso'}
    except Exception as err:
        logger.error(err, exc_info=True)
        self.update_state(state=states.FAILURE,
                          meta={'status': str(err)})


@celery.task(bind=True)
def arquiva_base_csv(self, baseid, base_csv):
    """Copia CSVs para MongoDB e apaga do disco."""
    # Aviso: Esta função rmtree só deve ser utilizada com caminhos seguros,
    # de preferência gerados pela própria aplicação
    self.update_state(state=states.STARTED,
                      meta={'status': 'Aguarde. Arquivando base ' + base_csv})
    try:
        abase = dbsession.query(BaseOrigem).filter(
            BaseOrigem.id == baseid).first()
        self.update_state(state=states.PENDING,
                          meta={'status': 'Aguarde... arquivando base ' +
                                base_csv + ' na base MongoDB ' + abase.nome})
        conn = MongoClient(host=MONGODB_URI)
        db = conn[DATABASE]
        GerenteRisco.csv_to_mongo(db, abase, base_csv)
        shutil.rmtree(base_csv)
        return {'status': 'Base arquivada com sucesso'}
    except Exception as err:
        logger.error(err, exc_info=True)
        self.update_state(state=states.FAILURE, meta={'status': str(err)})


@celery.task(bind=True)
def aplicar_risco(self, base_csv, padraoid, visaoid,
                  parametros_ativos, dest_path):
    """Chama função de aplicação de risco e grava resultado em arquivo."""
    self.update_state(state=states.STARTED,
                      meta={'status': 'Aguarde. Aplicando risco na base ' +
                            base_csv})
    gerente = GerenteRisco()
    try:
        self.update_state(state=states.PENDING, meta={
            'status': 'Aguarde... aplicando risco na base ' +
            base_csv})
        lista_risco = gerente.aplica_risco_por_parametros(dbsession, base_csv,
                                                          padraoid, visaoid,
                                                          parametros_ativos)
        if lista_risco:
            csv_salvo = os.path.join(dest_path, datetime.today().strftime('%Y-%m-%d %H:%M:%S') + '.csv')
            gerente.save_csv(lista_risco, csv_salvo)
        return {'status': 'Planilha criada com sucesso'}
    except Exception as err:
        logger.error(err, exc_info=True)
        self.update_state(state=states.FAILURE, meta={'status': str(err)})
