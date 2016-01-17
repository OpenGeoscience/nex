from perftest import app

@app.task
def noop():
    return "NOOP"
