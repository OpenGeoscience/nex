import os

import boto3
from botocore.handlers import disable_signing
from boto3.s3.transfer import S3Transfer


class Downloader(object):


    def __init__(self, year, day, month):
        self.year = year
        self.month = month
        self.day = day

        self.directory = self._create_data_directory()
        self.bucket = self._connect_to_bucket()


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

        return nasanex

    def download_aqua_data(self):
        pass

    def download_terra_data(self):
        pass

    def download_weather_data(self):
        pass


if __name__ == '__main__':
    d = Downloader("2013", "08", "05")
    d.download_aqua_data()
    print d
