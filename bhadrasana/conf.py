"""Configurações específicas do Bhadrasana."""
import os
import pickle
import tempfile

from ajna_commons.conf import ENCODE

ENCODE = ENCODE

APP_PATH = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(APP_PATH, 'files')
CSV_FOLDER = os.path.join(APP_PATH, 'CSV')
CSV_DOWNLOAD = CSV_FOLDER
CSV_FOLDER_TEST = os.path.join(APP_PATH, 'tests/CSV')
ALLOWED_EXTENSIONS = set(['txt', 'csv', 'zip'])
tmpdir = tempfile.mkdtemp()

try:
    SECRET = None
    with open('SECRET', 'rb') as secret:
        try:
            SECRET = pickle.load(secret)
        except pickle.PickleError:
            pass
except FileNotFoundError:
    pass

if not SECRET:
    SECRET = os.urandom(24)
    with open('SECRET', 'wb') as out:
        pickle.dump(SECRET, out, pickle.HIGHEST_PROTOCOL)
