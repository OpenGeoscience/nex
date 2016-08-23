import subprocess


def serve(catalog):

    # Jar to be called
    jar = "geotrellis/target/scala-2.10/demo-assembly-0.1.0.jar"

    # tile_server scala script
    tile_server = "landsat.ServeTiles"

    # Command to be called by the subprocess
    command = ["java", "-cp", jar, tile_server, catalog]

    process = subprocess.Popen(command, stdout=subprocess.PIPE)

    out, error = process.communicate()
