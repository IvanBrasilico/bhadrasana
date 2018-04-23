from celery import Celery
from ajna_commons.flask.conf import (BACKEND, BROKER, BSON_REDIS, DATABASE,
                                     MONGODB_URI, redisdb)
from sentinela.utils.gerente_risco import GerenteRisco

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)

@celery.task
def importar_base(csv_folder, baseid, data, filename, remove=False):
    gerente = GerenteRisco()
    result = gerente.importa_base(csv_folder, baseid, data, filename, remove)
    return result
