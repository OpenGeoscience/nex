import os

import boto3
from botocore.handlers import disable_signing
from boto3.s3.transfer import S3Transfer


class Downloader(object):


    def __init__(self, year, day, month):
        self.year = year
        self.month = month
        self.day = day


    def _create_data_directory(self):
        """ Create a directory to dump the data """

        # Directory of the script
        directory = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'Data_Directory')

        # Create a new directory
        if not os.path.exists(directory):
            os.makedirs(directory)

        self.directory = directory


    def download_aqua_data(self):
        self._create_data_directory()


    def download_terra_data(self):
        self._create_data_directory()

    def download_weather_data(self):
        self._create_data_directory()



if __name__ == '__main__':
    d = Downloader("2013", "08", "05")
    d.download_aqua_data()
    print d
