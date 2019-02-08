import os
from werkzeug.wsgi import DispatcherMiddleware

from bhadrasana.main import app

application = DispatcherMiddleware(app,
                                   {
                                       '/bhadrasana': app
                                   })
