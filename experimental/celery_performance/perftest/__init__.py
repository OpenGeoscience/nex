from __future__ import absolute_import
from celery import Celery, Task


app = app = Celery('perftests',
                   backend='amqp',
                   broker='amqp://guest:guest@ec2-52-11-87-22.us-west-2.compute.amazonaws.com',
                   include=['perftest.tests'])
