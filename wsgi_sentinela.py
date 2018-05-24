import logging

from bhadrasana.app import app
from ajna_commons.flask.log import error_handler, sentry_handler

if __name__ == '__main__':
    print('Iniciando Servidor Bhadrasana...')
    app.logger.addHandler(error_handler)
    if sentry_handler:
        app.logger.addHandler(sentry_handler)
    app.logger.setLevel(logging.WARNING)
    app.logger.warning('Servidor (re)iniciado!')
    # To log more things, change the level:
    # import logging
    # file_handler.setLevel(logging.INFO)
    app.run()

