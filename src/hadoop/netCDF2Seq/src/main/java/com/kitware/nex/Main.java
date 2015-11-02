package com.kitware.nex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.units.DateUnit;

import java.io.IOException;
import java.net.URI;

// import static ucar.nc2.NCdumpW.printArray;

/**
 * Hello world!
 *
 */
public class Main
{

    public static void log(String arg){
        System.out.println(arg + "\n");
    }

    public static void log(String arg, Exception e){
        log(arg + e.getMessage());
    }

    public static void main( String[] args ) throws IOException, InvalidRangeException
    {
        String uri = args[0];

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);


        class FloatArrayWritable extends ArrayWritable{ public FloatArrayWritable() { super(FloatWritable.class); } }

        SequenceFile.Writer writer = null;
        NetcdfFile ncfile = null;

        LongWritable key = new LongWritable();
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();

        Path hdfs_path = new Path("/user/nex/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.seq");
        FSDataOutputStream out = fs.create(hdfs_path, true);

        String local_filename = "/data/tmp/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc";


        try {
            writer = SequenceFile.createWriter( conf,
                        SequenceFile.Writer.stream(out),
                        SequenceFile.Writer.keyClass(key.getClass()),
                        SequenceFile.Writer.valueClass(value.getClass()));

            ncfile = NetcdfFile.open(local_filename);

            // The dimention we want to serialize across
            Variable time_var = ncfile.findVariable("time");
            Array time_values = time_var.read();

/*  Date conversion stuff
            DateUnit date_converter = null;
            try {
                date_converter = new DateUnit(time_var.getUnitsString());
            } catch (Exception e) {
                log(String.format("Could not convert %s", time_var.getUnitsString()), e);
                System.exit(1);
            }
*/
            int[] origin = new int[3];

            Variable pr = ncfile.findVariable("pr");
            int[] prShape = pr.getShape();

            int[] size = new int[] {1, prShape[1], prShape[2]};
            for ( int i = 0; i <  prShape[0];  i++){
                origin[0] = i;

                // CalendarDate t_step = date_converter.makeCalendarDate(time_values.getDouble(i));
                key.set(time_values.getLong(i));

                value.set((float[]) pr.read(origin, size).reduce(0).getStorage());
                
                writer.append(key, value);
                // log(key.toString());
                // printArray(data2D,"pr", System.out, null);

            }

        } catch (IOException ioe) {
            Main.log("trying to open " + local_filename, ioe);
        } finally {

            IOUtils.closeStream(writer);

            if (ncfile != null) try {
                ncfile.close();
            } catch (IOException ioe) {
                Main.log("trying to close " + local_filename, ioe);
            }
        }
    }
}
