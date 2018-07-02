import time
import pytest
import unittest
import os
import shutil

from ajna_commons.flask.conf import BACKEND, BROKER
from celery import states
from bhadrasana.workers.tasks import celery
from bhadrasana.workers.tasks import importar_base


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': BROKER,
        'result_backend': BACKEND
    }


@pytest.fixture(scope='session')
def celery_parameters():
    return {
        'task_cls': celery.task_cls,
        'strict_typing': False,
    }


class FlaskCeleryTestCase(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def init_worker(self, celery_worker):
        self.worker = celery_worker

    def setUp(self):
        self.csv_file = ['iguaria, esporte\n',
                         'temaki, corrida\n',
                         'churros, remo\n']
        self.tempfile_name = 'plan_test.csv'
        self.data = '2018-01-01'
        with open(self.tempfile_name, 'w') as out_csv:
            for linha in self.csv_file:
                out_csv.write(linha)
        self.filepath = os.path.join(
            '1', '2018', '01', '01', self.tempfile_name)

    def tearDown(self):
        try:
            os.remove(self.tempfile_name)
        except FileNotFoundError:
            pass

    def test_1_importabase(self):
        task = importar_base.apply_async(('.',
                                          1,
                                          self.data,
                                          self.tempfile_name,
                                          True))
        print(task)
        tries = 10
        while tries > 0:
            tries -= 1
            task = importar_base.AsyncResult(task.id)
            response = {'state': task.state}
            if task.info:
                response['current'] = task.info.get('current', ''),
                response['status'] = task.info.get('status', '')
            assert task.state != states.FAILURE
            if task.state == states.SUCCESS:
                break
            time.sleep(0.1)

        assert os.path.exists(self.filepath)
        shutil.rmtree('1')
