import java.io.*;
import java.util.*;

import java.net.URLConnection;
import java.net.URL;
import java.net.MalformedURLException;

import ucar.grib.grib2.*;
import ucar.unidata.io.RandomAccessFile;

import com.mongodb.*;


/**
 * 2015-05-05
 *
 * Store GRIB2 data in MongoDB.
 * GRIB2 decoding is performed by the netCDF-Java GRIB decoder.
 *
 * Refered to Grib2Json (https://github.com/cambecc/grib2json)
 *
 * @author Yuta Tachibana
 */

public class Grib2Mongo {
	private File grib2file = null;
	private MongoClient mongoClient = null;
	private DB db = null;


	public void download(String input_url, String output_file) throws MalformedURLException, FileNotFoundException, IOException {
		URL url = new URL(input_url);
		URLConnection conn = url.openConnection();
		InputStream in = conn.getInputStream();

		grib2file = new File(output_file);
		FileOutputStream out = new FileOutputStream(grib2file, false);

		byte[] bytes = new byte[512];
		while(true){
			int ret = in.read(bytes);
			if(ret <= 0) break;
			out.write(bytes, 0, ret);
		}

		out.close();
		in.close();
	}


	private boolean isSelected(Grib2Record record, int paramCategory, int paramNumber, int surfaceType, double surfaceValue) {
		Grib2Pds pds = record.getPDS().getPdsVars();
		return
			(paramCategory == pds.getParameterCategory()) &&
			(paramNumber == pds.getParameterNumber()) &&
			(surfaceType == pds.getLevelType1()) &&
			(surfaceValue == 0 || surfaceValue == pds.getLevelValue1());
	}


	// write GPV(header, data) to MongoDB Collection
	// split data into ny rows
	private void writeDataToColl(Grib2Data gd, Grib2Record record, DBCollection coll) throws MongoException, IOException{
		// get header data
		Grib2Pds                   pds = record.getPDS().getPdsVars();
        Grib2GDSVariables          gds = record.getGDS().getGdsVars();
		Grib2IdentificationSection ids = record.getId();
		int nx = gds.getNx();
		int ny = gds.getNy();
		int forecastTime = pds.getForecastTime();

		// get GPV data
		float[] data = gd.getData(record.getGdsOffset(), record.getPdsOffset(), ids.getRefTime());

		// insert header
		BasicDBObject header = new BasicDBObject("t", -1)
			.append("nx",  nx)
			.append("ny",  ny)
			.append("lo1", gds.getLo1())
			.append("la1", gds.getLa1())
			.append("dx",  gds.getDx())
			.append("dy",  gds.getDy());
		coll.insert(header);

		// insert data row
		for (int i = 0; i < ny; i++){
			BasicDBObject doc = new BasicDBObject("t", forecastTime)
				.append("r", i)
				.append("d", Arrays.copyOfRange(data, i*nx, (i+1)*nx-1));
			coll.insert(doc);
		}

		System.out.print(".");
	}


	
	public void store (String collName, int paramCategory, int paramNumber, int surfaceType, double surfaceValue) throws IOException {
			
		// collection
		DBCollection coll = db.getCollection(collName);

		// load file
		RandomAccessFile raf = new RandomAccessFile(grib2file.getPath(), "r");
		raf.order(RandomAccessFile.BIG_ENDIAN);
		Grib2Input input = new Grib2Input(raf);

		// scan all records
		if (input.scan(false, false)){
			List<Grib2Record> records = input.getRecords();
			for (Grib2Record record : records){

				// check params -> storeData
				if (isSelected(record, paramCategory, paramNumber, surfaceType, surfaceValue)){
					writeDataToColl(new Grib2Data(raf), record, coll);
				}
			}
		}
		raf.close();

		// ensure Index
		coll.ensureIndex("r");
		coll.ensureIndex("t");
	}


	public void connectMongo(String mongoURI) throws Exception {
		MongoClientURI mongoClientURI = new MongoClientURI(mongoURI);
		mongoClient = new MongoClient(mongoClientURI);
		db = mongoClient.getDB(mongoClientURI.getDatabase());
	}


	public void closeMongo() {
		mongoClient.close();
	}
}
