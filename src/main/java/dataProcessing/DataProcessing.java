package dataProcessing;


import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * this class processes the raw dataset and transforms it into a stream file
 */
public class DataProcessing {
    public static void main(String[] args){

        try {
            nyc_processing();
            smartHome_processing();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * process the NYC Taxi data set
     */
    public static void nyc_processing() throws Exception {

        /**
         * dataset download
         * https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
         */
        File file = new File("src/main/resources/rawDataSet/yellow_tripdata_2019-01.csv");

        FileInputStream inputStream = new FileInputStream(file);
        List<String[]> csvList = new ArrayList<String[]>();
        InputStreamReader reader = new InputStreamReader(inputStream);
        CSVReader csvreader = new CSVReader(reader, ',');

        String [] line;
        int i = 0;

        // selecting relevant columns
        while ((line = csvreader.readNext())!=null){
            String[] data = new String[3];
            data[0] = i+"";  //time stamp
            i++;
            data[1] = line[0];   //vendor id
            data[2] = "1";   // group id

            csvList.add(data);
        }

        //write data into file
        File outputfile = new File("src/main/resources/rawDataSet/taxi_stream.csv");
        if (!file.exists()){
            outputfile.createNewFile();

        }
        FileOutputStream outputStream = new FileOutputStream(outputfile);

        write(outputStream, csvList);


    }

    /**
     * process the Smart Home data set
     */
    public static void smartHome_processing() throws Exception {

        /**
         * dataset download
         * http://www.doc.ic.ac.uk/~mweidlic/sorted.csv.gz
         */
        File file = new File("src/main/resources/rawDataSet/houses10_19_concat.csv");

        FileInputStream inputStream = new FileInputStream(file);
        List<String[]> csvList = new ArrayList<String[]>();
        InputStreamReader reader = new InputStreamReader(inputStream);
        CSVReader csvreader = new CSVReader(reader, ',');

        String [] line;
        int i = 0;

        // selecting relevant columns
        while ((line = csvreader.readNext())!=null){
            String[] data = new String[3];
            data[0] = i+""; //timestamp
            i++;
            data[1] = line[6];   //house id
            data[2] = "1";   // group id

            csvList.add(data);
        }

        //write data into file
        File outputfile = new File("src/main/resources/rawDataSet/houses_stream.csv");
        if (!file.exists()){
            outputfile.createNewFile();

        }
        FileOutputStream outputStream = new FileOutputStream(outputfile);

        write(outputStream, csvList);
    }


    /**
     * write the list of data into a csv file
     * @param outputStream the output stream of the target file
     * @param data a list of data
     * @throws Exception
     */

    public static void write(OutputStream outputStream, List<String[]> data) throws Exception{

        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        CSVWriter csvwriter = new CSVWriter(writer, ',');

        csvwriter.writeAll(data);

    }


}
