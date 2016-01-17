from __future__ import absolute_import
from celery import Celery, Task
import os

with open(os.path.dirname(__file__) + "/.master_hostname", "r") as fh:
    master_hostname = fh.read().rstrip()


app = app = Celery('perftests',
                   backend='amqp',
                   broker='amqp://guest:guest@{}'.format(master_hostname),
                   include=['perftest.tests'])
