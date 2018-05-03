"""Celery tasks.

Tasks/workers que rodarão à parte, em um processo Celery.
Todas as tarefas que demandem tempo/carga de CPU ou I/O devem ser colocadas
preferencialmente no processo Celery, sendo executadas de forma assíncrona/
background.
"""
from celery import Celery, states

from ajna_commons.flask.conf import BACKEND, BROKER
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import ascii_sanitizar
from sentinela.utils.gerente_risco import GerenteRisco

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)


@celery.task(bind=True)
def importar_base(self, csv_folder, abase, data, filename, remove=False):
    """Função para upload do arquivo de uma extração ou outra fonte externa.

    Utiliza o :class: `sentinela.utils.gerenterisco.GerenteRisco`.
    Suporte por ora para csv com títulos e zip com sch (padrão Carga)
    Ver também :func: `sentinela.app.importa_base`

    Args:
        csv_folder: caminho onde será salvo o resultado

        abase: Base de Origem do arquivo

        data: data inicial do período extraído (se não for passada,
        assume hoje)

        filename: arquivo csv, sch+txt, ou conjunto deles em formato zip

        remove: exclui arquivo original no final do processamento
    """
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
