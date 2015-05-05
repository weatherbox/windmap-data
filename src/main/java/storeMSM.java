import java.io.*;
import java.util.*;

public class storeMSM {
	private static String MSM_URI = "http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original/";

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		try {
			// File time (UTC)
			TimeZone utc = TimeZone.getTimeZone("UTC");
			Calendar cal = Calendar.getInstance(utc);
			int year = cal.get(Calendar.YEAR);
			int mon  = cal.get(Calendar.MONTH) + 1;
			int day  = cal.get(Calendar.DATE);
			int hour = ((int) cal.get(Calendar.HOUR_OF_DAY)/3) * 3 - 6;

			String date = String.format("%d/%02d/%02d", year, mon, day);
			String time = String.format("%d%02d%02d%02d", year, mon, day, hour);

			Grib2Mongo gm = new Grib2Mongo();

			String file = MSM_URI + date + "/Z__C_RJTD_" + time + "0000_MSM_GPV_Rjp_Lsurf_FH00-15_grib2.bin";
			System.out.println(file);
			gm.download(file, "MSM_Surf_00-15.grib");

			gm.connectMongo(System.getenv("MONGO_DEV_U"));
			gm.store("surface_wind_u", 2, 2, 103, 10);
			gm.closeMongo();
			
			gm.connectMongo(System.getenv("MONGO_DEV_V"));
			gm.store("surface_wind_v", 2, 3, 103, 10);
			gm.closeMongo();

		} catch (FileNotFoundException e){
		 	System.out.println("file not found");
		} catch (IOException e){
			System.out.println("io exception");
		} catch (Exception e) {
			System.out.println("There was an error: " + e.getMessage());
		}
		long end = System.currentTimeMillis();
		System.out.println((end - start)  + "ms");
	}
}
