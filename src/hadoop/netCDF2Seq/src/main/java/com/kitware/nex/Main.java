package com.kitware.nex;

import opendap.dap.NoSuchVariableException;
import org.apache.directory.api.util.ByteBuffer;
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
import java.util.Map;
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

    public static void main( String[] args ) throws Exception {
        String src_uri = args[0];
        String dest_uri = args[1];

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        /*
        for (Map.Entry<String, String> entry : conf) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }

        System.exit(0);
        */
        FileSystem src_fs = FileSystem.get(URI.create(src_uri), conf);
        FileSystem dest_fs = FileSystem.get(URI.create(dest_uri), conf);


        SequenceFile.Writer writer = null;
        NetcdfFile ncfile = null;

        DateTimeWritable key = new DateTimeWritable();
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();

        Path src_path = new Path(src_uri);
        Path dest_path = new Path(dest_uri);

        // Read the Source data into src_data
        FSDataInputStream in = src_fs.open(src_path);

        byte[] src_data = org.apache.commons.io.IOUtils.toByteArray(in);
      //   int length  = in.readInt();
      //  byte[] src_data = new byte[length];
      //  in.read(src_data);
       // in.close();

        // Setup the output stream
        FSDataOutputStream out = dest_fs.create(dest_path, true);

        // String local_filename = "/data/tmp/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc";


        try {
            ncfile = NetcdfFile.openInMemory("tmp.nc", src_data);

            Variable var = null;

            for(Variable v: ncfile.getVariables()){
                if( v.getShortName().equals("pr")){ var = v; break; }
                if( v.getShortName().equals("tasmin")){ var = v; break; }
                if( v.getShortName().equals("tasmax")){ var = v; break; }
            }

            if( var == null) throw new NoSuchVariableException("Could not find pr, tasmin, or tasmax");


            writer = SequenceFile.createWriter( conf,
                        SequenceFile.Writer.stream(out),
                        SequenceFile.Writer.keyClass(key.getClass()),
                        SequenceFile.Writer.valueClass(value.getClass()));



            // The dimention we want to serialize across
            Variable time_var = ncfile.findVariable("time");
            Array time_values = time_var.read();

            //  Date conversion stuff
            DateUnit date_converter = null;
            try {
                date_converter = new DateUnit(time_var.getUnitsString());
            } catch (Exception e) {
                log(String.format("Could not convert %s", time_var.getUnitsString()), e);
                System.exit(1);
            }

            int[] origin = new int[3];

            int[] varShape = var.getShape();

            int[] size = new int[] {1, varShape[1], varShape[2]};
            for ( int i = 0; i <  varShape[0];  i++){
                origin[0] = i;

                CalendarDate t_step = date_converter.makeCalendarDate(time_values.getDouble(i));

                key.set(t_step.toDate());

                value.set((float[]) var.read(origin, size).reduce(0).getStorage());

                writer.append(key, value);
                // log(key.toString());
                // printArray(data2D,"pr", System.out, null);

            }

        } catch (IOException ioe) {
            Main.log("trying to open " + src_uri, ioe);
        } finally {

            IOUtils.closeStream(writer);

            if (ncfile != null) try {
                ncfile.close();
            } catch (IOException ioe) {
                Main.log("trying to close " + src_uri, ioe);
            }
        }
    }
}
