import os
import subprocess


def ingest(tiff_image):
    """ Calls the IngestImage scala object """

    # Jar to be called
    jar = "geotrellis/target/scala-2.10/demo-assembly-0.1.0.jar"

    output_dir = "geotrellis/data"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    directory = os.path.dirname(os.path.realpath(__file__))

    # Output directory which will host the ingested image
    output = os.path.join(directory, output_dir, "catalog")

    # Memory parameter
    memory = "-Xms8g"

    # Ingestor object
    ingestor = "landsat.IngestImage"

    # Command to be called by the subprocess
    command = ["java", memory, "-cp", jar, ingestor, tiff_image, output]

    if not os.path.exists(output):
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        out, error = process.communicate()

    return output
