from perftest import app
from time import sleep

@app.task
def noop(s=0):
    sleep(s)
    return "NOOP"
