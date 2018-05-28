import os
from ajna_commons.flask.conf import BHADRASANA_URL
os.environ['DEBUG'] = '1'

from bhadrasana.main import app




if __name__ == '__main__':
    print('Iniciando Servidor Bhadrasana...')
    port = int(BHADRASANA_URL.split(':')[-1])
    app.run(port=port)
