import java.io.*;
import java.util.*;
import com.mongodb.*;

public class storeMSM {

	public static void main(String[] args) {
		try {
			Grib2Mongo gm = new Grib2Mongo();

			gm.download(
				"http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original/2015/02/14/Z__C_RJTD_20150214000000_MSM_GPV_Rjp_Lsurf_FH00-15_grib2.bin",
				"grib.grib"
			);

			// connect to mongodb
			MongoClient mongoClient = new MongoClient(new MongoClientURI(System.getenv("MONGOLAB_URI")));
			DB db = mongoClient.getDB("heroku_app33876585");

			gm.store(db, "surface_wind_u", 2, 2, 103, 10);
			gm.store(db, "surface_wind_v", 2, 3, 103, 10);

			// close
			mongoClient.close();

		} catch (IOException e){
			System.out.println("io exception");

		} catch (Exception e) {
			System.out.println("There was an error: " + e.getMessage());
		}
	}
}
