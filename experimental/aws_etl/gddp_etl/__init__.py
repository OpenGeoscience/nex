from celery import Celery

app = Celery('example', broker='amqp://guest@172.31.38.99//')

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERY_SEND_EVENTS=True,
    CELERY_SEND_TASK_SENT_EVENT=True,
)


def build_url(opts):
    return ("http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1"
            "/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc").format(**opts)

def build_range(scenarios=None, models=None, years=None):

    if scenarios is None:
        scenarios = ["rcp45", "rcp85"]

    if models is None:
        models =  ["ACCESS1-0",
                   "bcc-csm1-1",
                   "BNU-ESM",
                   "CanESM2",
                   "CCSM4",
                   "CESM1-BGC",
                   "CNRM-CM5",
                   "CSIRO-Mk3-6-0",
                   "GFDL-CM3",
                   "GFDL-ESM2G",
                   "GFDL-ESM2M",
                   "inmcm4",
                   "IPSL-CM5A-LR",
                   "IPSL-CM5A-MR",
                   "MIROC5",
                   "MIROC-ESM",
                   "MIROC-ESM-CHEM",
                   "MPI-ESM-LR",
                   "MPI-ESM-MR",
                   "MRI-CGCM3",
                   "NorESM1-M"]

    if years is None:
        years = range(1950, 2101)

    for year in years:
        for model in models:
            for scenario in scenarios:

                # If we're in a year less than 2005 we're really in the
                # historical scenario
                _scenario = "historical" if year <= 2005 else scenario

                # These models do not have values for 2100
                if (model == "bcc-csm1-1" or model == "MIROC5") and year == 2100:
                    continue

                yield (build_url({"model": model, "scenario": _scenario, "year": year, "variable": "pr" }),
                       build_url({"model": model, "scenario": _scenario, "year": year, "variable": "tasmin" }),
                       build_url({"model": model, "scenario": _scenario, "year": year, "variable": "tasmax" }))


@app.task
def fib(x):
    if x < 2:
        return 1
    return fib(x-1) + fib(x -2 )
