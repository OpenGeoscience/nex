import celery
import json
import argparse

celeryapp = celery.Celery('romanesco',
    backend='mongodb://localhost/romanesco',
    broker='mongodb://localhost/romanesco')

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--master_url', required=True)
parser.add_argument('-d', '--datafile_path', required=True)
parser.add_argument('-p', '--parameter', required=True)
parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')
parser.add_argument('-v', '--validate', action='store_true')
parser.add_argument('-s', '--partitions', default=8, type=int)
parser.add_argument('-c', '--grid_chunk_size', default=2000, type=int)
parser.add_argument('-o', '--output_path')

config = parser.parse_args()

with open('n_timesteps_mean.json', 'r') as fp:
    analysis = json.loads(fp.read())

with open('n_timesteps_mean.py', 'r') as fp:
    analysis['script'] = fp.read()

analysis['spark_conf']['spark.master'] = config.master_url

data = {
    'inputs': {
        'datafile_path': {'format': 'text' , 'data': str(config.datafile_path)},
        'parameter': {'format': 'text', 'data': config.parameter},
        'timesteps': { 'format': 'number', 'data': config.timesteps},
        'partitions': {'format': 'number', 'data': config.partitions},
        'grid_chunk_size': { 'format': 'number', 'data': config.grid_chunk_size},
        'output_path': { 'format': 'text', 'data': ''}
    },
    'outputs ': {
        'runtime': { 'format': 'json' }
    }
}


if config.output_path:
    data['inputs']['output_path'] = config.output_path

async_result = celeryapp.send_task('romanesco.run', [analysis], data)

print async_result.get()

