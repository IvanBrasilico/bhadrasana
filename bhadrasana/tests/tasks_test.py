import pytest
import unittest
from ajna_commons.flask.conf import BACKEND, BROKER


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': BROKER,
        'result_backend': BACKEND
    }


class FlaskCeleryTestCase(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def init_worker(self, celery_worker):
        self.worker = celery_worker
