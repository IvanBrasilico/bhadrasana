import logging

from sentinela.app import app
from ajna_commons.flask.log import error_handler

if __name__ == '__main__':
    print('Iniciando Servidor Bhadrasana...')
    app.logger.addHandler(error_handler)
    app.logger.setLevel(logging.WARNING)
    # To log more things, change the level:
    # import logging
    # file_handler.setLevel(logging.INFO)
    app.run()

