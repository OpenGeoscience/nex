import os
import requests


def download(url):
    """
        Downloads the image from the given URL

        :param url: File url
    """

    # Generate the local file name
    local_filename = url.split('/')[-1].split('?')[0]

    if not os.path.exists(local_filename):
        r = requests.get(url, stream=True)

        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

    return local_filename
