package com.kitware.nex;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NCdumpW;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.units.DateUnit;

import java.io.IOException;

import static ucar.nc2.NCdumpW.printArray;

/**
 * Hello world!
 *
 */
public class Main
{

    public static void log(String arg){
        System.out.println(arg);
    }

    public static void log(String arg, Exception e){
        log(arg + e.getMessage());
    }

    public static void main( String[] args ) throws IOException, InvalidRangeException
    {
        String filename = "/data/tmp/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc";
        NetcdfFile ncfile = null;


        try {
            ncfile = NetcdfFile.open(filename);

            Variable time_var = ncfile.findVariable("time");
            Array time_values = time_var.read();
            DateUnit date_converter = null;

            try {
                date_converter = new DateUnit(time_var.getUnitsString());
            } catch (Exception e) {
                log(String.format("Could not convert %s", time_var.getUnitsString()), e);
                System.exit(1);
            }

            int[] origin = new int[3];

            Variable pr = ncfile.findVariable("pr");
            int[] prShape = pr.getShape();

            int[] size = new int[] {1, prShape[1], prShape[2]};
            for ( int i = 0; i <  prShape[0];  i++){
                origin[0] = i;

                CalendarDate t_step = date_converter.makeCalendarDate(time_values.getDouble(i));
                Array data2D = pr.read(origin, size).reduce(0);
                // log(t_step.toString());
                // printArray(data2D,"pr", System.out, null);

            }

        } catch (IOException ioe) {
            Main.log("trying to open " + filename, ioe);
        } finally {

            if ( null != ncfile ) try {
                ncfile.close();
            } catch (IOException ioe) {
                Main.log("trying to close " + filename, ioe);
            }
        }
    }
}
