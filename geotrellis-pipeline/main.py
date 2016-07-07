import download_image
import hdf2tif
import ingest_image
import serve_tiles


def main():

    url = "https://www.dropbox.com/s/vfv55juwofcyho6/\
L57.Globe.month07.2011.hh09vv04.h6v1.doy182to212.NBAR.v3.0.hdf?dl=1"

    # Download the image
    hdf_image = download_image.download(url)

    # Preprocess the image
    tiff_image = hdf2tif.hdf2tif(hdf_image)

    # Ingest the image to Geotrellis
    catalog = ingest_image.ingest(tiff_image)

    # Serve the tiles
    serve_tiles.serve(catalog)

if __name__ == '__main__':
    main()
