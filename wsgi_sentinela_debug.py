import os
from werkzeug.serving import run_simple
from werkzeug.wsgi import DispatcherMiddleware


from ajna_commons.flask.conf import BHADRASANA_URL

os.environ['DEBUG'] = '1'
from bhadrasana.main import app




if __name__ == '__main__':
    print('Iniciando Servidor Bhadrasana...')
    port = 5000
    if BHADRASANA_URL:
        port = int(BHADRASANA_URL.split(':')[-1])
    application = DispatcherMiddleware(app,
                                    {
                                        '/bhadrasana': app
                                    })
    run_simple('localhost', port, application, use_reloader=True)
