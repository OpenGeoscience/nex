import os

import boto3
from botocore.handlers import disable_signing
from boto3.s3.transfer import S3Transfer


class Downloader(object):


    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day

        self.directory = self._create_data_directory()
        self.nasanex = self._connect_to_bucket()


    def _create_data_directory(self):
        """ Create a directory to dump the data """

        # Directory of the script
        directory = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'Data_Directory')

        # Create a new directory
        if not os.path.exists(directory):
            os.makedirs(directory)

        return directory

    def _connect_to_bucket(self):
        # Get the s3
        resource = boto3.resource('s3')
        client = resource.meta.client
        transfer = S3Transfer(client)

        # Connect anonymously
        resource.meta.client.meta.events.register('choose-signer.s3.*',
                                                  disable_signing)

        # Get the nasanex bucket
        nasanex = resource.Bucket('nasanex')

        return {'bucket': nasanex, 'transfer': transfer}


    def _filter_and_download_data(self, prefix):

        for item in self.nasanex['bucket'].objects.filter(Prefix=prefix):
            output_dir = os.path.join(self.directory, os.path.basename(item.key))
            self.nasanex['transfer'].download_file('nasanex', item.key, output_dir)


    def download_aqua_data(self):

        prefix = '{}/{}.{}.{}'.format('MODIS/MOLA/MYD13Q1.005',
            self.year, self.month, self.day)

        self._filter_and_download_data(prefix)

    def download_terra_data(self):

        prefix = '{}/{}.{}.{}'.format('MODIS/MOLT/MOD13Q1.005',
            self.year, self.month, self.day)

        self._filter_and_download_data(prefix)

    def download_tmax_data(self):

        prefix = 'NEX-GDDP/BCSD/rcp45/day/atmos/tasmax/r1i1p1/v1.0/\
tasmax_day_BCSD_rcp45_r1i1p1_CCSM4_{}.nc'.format(self.year)

        self._filter_and_download_data(prefix)

if __name__ == '__main__':
    d = Downloader("2013", "08", "05")
    d.download_aqua_data()
    d.download_tmax_data()
