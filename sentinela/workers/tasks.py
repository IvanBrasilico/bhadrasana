from celery import Celery, states
from ajna_commons.flask.conf import (BACKEND, BROKER)
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import (ascii_sanitizar)
from sentinela.utils.gerente_risco import GerenteRisco

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)


@celery.task(bind=True)
def importar_base(self, csv_folder, abase, data, filename, remove=False):
    self.update_state(state=states.STARTED,
                      meta={'status': 'Iniciando'})
    gerente = GerenteRisco()
    try:
        lista_arquivos = gerente.importa_base(
            csv_folder, abase.id, data, filename, remove)
        # Sanitizar base já na importação para evitar
        # processamento repetido depois
        gerente.ativa_sanitizacao(ascii_sanitizar)
        gerente.checa_depara(abase)  # Aplicar na importação???
        gerente.pre_processa_arquivos(lista_arquivos)
    except Exception as err:
        logger.error(err, exc_info=True)
        self.update_state(state=states.FAILURE)
        return {'status': str(err)}
    return {'status': 'Base importada com sucesso'}
