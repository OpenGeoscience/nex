import glob
import os
from xml.etree.ElementTree import parse, SubElement

import gdal


DIRECTORY = os.path.dirname(os.path.realpath(__file__))


def list_files(directory, extension):
    """
    Lists all the files in a given directory with a wildcard

    :param directory: Directory to be checked
    :param extension: File extension to be searched
    :return: List of files matching the extension
    """

    return glob.glob(os.path.join(directory, '*.{}'.format(extension)))


def create_output_directory(hdf):
    """
    Creates a unique output directory to store the intermediate vrt files

    :param hdf: HDF file to be processed
    :return: Folder
    """

    direc = os.path.splitext(hdf)[0]
    if not os.path.exists(direc):
        os.makedirs(direc)

    return direc


def get_scale(subdataset):
    """
    Checks for the scale ratio and returns if it exists

    :param subdataset: HDF subdataset
    :return: Scale ratio
    """

    dataset = gdal.Open(subdataset, gdal.GA_ReadOnly)

    metadata = dataset.GetMetadata_Dict()

    # Filter the metadata
    filtered_meta = {k: v for k, v in metadata.iteritems()
                     if 'scale' in k.lower()}

    # Hopefully there will be one element in the dictionary
    return filtered_meta[filtered_meta.keys()[0]]


def modify_vrt(vrt, scale):
    """
    Makes modifications to the vrt file to fix the values.

    :param vrt: VRT file to be processed
    :param scale: Scale value from get_scale function
    :return: None
    """

    doc = parse(vrt)

    root = doc.getroot()

    # Fix the datatype if it is wrong
    raster_band = root.find('VRTRasterBand')
    raster_band.set('dataType', 'Float32')

    # Add the scale to the vrt file
    source = root.find('VRTRasterBand').find('ComplexSource')
    scale_ratio = SubElement(source, 'ScaleRatio')
    scale_ratio.text = scale

    # Write the scale input
    # vrt files are overwritten with the same name
    doc.write(vrt, xml_declaration=True)


def convert_to_vrt(subdatasets, data_dir):
    """
    Loops through the subdatasets and creates vrt files

    :param subdatasets: Subdataset of every HDF file
    :param data_dir: Result of create_output_directory method
    :return: None
    """

    # For every subdataset loop through the subdatasets
    # Enumerate to keep the bands in order

    for index, subd in enumerate(subdatasets):
        # Get specific bands only
        if index == 3:
            # Generate output name from the band names
            output_name = os.path.join(
                data_dir,
                "Band_{}_{}.vrt".format(str(index + 1).zfill(2),
                                        subd[0].split(":")[4]))

            # Create the virtual raster
            gdal.BuildVRT(output_name, subd[0])

            # Check if scale and offset exists
            scale = get_scale(subd[0])

            # Fix the datatypes and add scale
            modify_vrt(output_name, scale)


def hdf2tif(hdf):
    """
    Converts hdf files to tiff files

    :param hdf: HDF file to be processed
    :return: None
    """

    dataset = gdal.Open(hdf, gdal.GA_ReadOnly)
    subdatasets = dataset.GetSubDatasets()
    data_dir = create_output_directory(hdf)
    convert_to_vrt(subdatasets, data_dir)
    vrt_options = gdal.BuildVRTOptions(separate=True)
    vrt_list = list_files(data_dir, 'vrt')
    vrt_output = hdf.replace('.hdf', '.vrt')
    gdal.BuildVRT(vrt_output, sorted(vrt_list), options=vrt_options)
    proj = "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext"
    warp_options = gdal.WarpOptions(srcSRS=proj, dstSRS="EPSG:3857")
    gdal.Warp(vrt_output.replace(".vrt", "_ndvi_reprojected.tif"),
              vrt_output, options=warp_options)


def main():
    """ Main function which orchestrates the conversion """

    hdf_files = list_files(DIRECTORY, 'hdf')
    for hdf in hdf_files:
        hdf2tif(hdf)

if __name__ == "__main__":
    main()
