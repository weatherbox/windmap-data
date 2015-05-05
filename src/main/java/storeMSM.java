import java.io.*;
import java.util.*;
import com.mongodb.*;

public class storeMSM {
	private static String MSM_URI = "http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original/";

	public static void main(String[] args) {
		try {
			Grib2Mongo gm = new Grib2Mongo();

			gm.download(
				MSM_URI + "2015/02/14/Z__C_RJTD_20150214000000_MSM_GPV_Rjp_Lsurf_FH00-15_grib2.bin",
				"MSM_Surf_00-15.grib"
			);

			gm.connectMongo(System.getenv("MONGOLAB_URI"));
			gm.store("surface_wind_u", 2, 2, 103, 10);
			gm.closeMongo();
			
			gm.connectMongo(System.getenv("MONGOLAB_URI"));
			gm.store("surface_wind_v", 2, 3, 103, 10);
			gm.closeMongo();

		} catch (IOException e){
			System.out.println("io exception");

		} catch (Exception e) {
			System.out.println("There was an error: " + e.getMessage());
		}
	}
}
