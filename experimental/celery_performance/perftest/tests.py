from perftest import app
from time import sleep
import numpy as np

@app.task
def noop(s=0):
    sleep(s)
    return "NOOP"

@app.task
def return_ndarray(mb):
    N = (mb * 8e6) / 64.0
    return np.random.random_sample((N,))
