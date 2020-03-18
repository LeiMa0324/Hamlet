package Executor;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;


/**
 * Each static method is one experiment
 */
public class Main {
    public static void main(String[] args){

//        varyEventsPerWindow();

        varyNumOfSharedEvents();
    }

    /**
     * Experiment1: fix epw, vary shared events per graphlet
     */

    public static void varyNumOfSharedEvents(){
        String queryFile ="src/main/resources/Queries/GenQueries_3.txt";
        int epw = 50000;

        for (int numofShared = 15; numofShared<41;numofShared+=5){
            System.out.println("==================== number of Bs: "+numofShared+" ====================");
            String streamFile = String.format("src/main/resources/Streams/GenStream_%d.txt",numofShared);
            Executor executor = new Executor(streamFile, queryFile, epw, false);
            executor.run();
            logging(executor, numofShared, "varyNumofShared.csv");

        }
    }

    /**
     * Experiment2: fix # of shared events, vary epw
     */
    public static void varyEventsPerWindow(){
        String queryFile ="src/main/resources/Queries/GenQueries_3.txt";
        System.out.println("#of shared events per graphlet: 20");
        for (int epw = 50000; epw<60000;epw+=10000){

            System.out.println("====================Evernts per Window: "+epw+"====================");
            String streamFile = "src/main/resources/Streams/GenStream_20.txt";

            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run();
            logging(executor, 20, "varyEPW.csv");

        }
    }

    public static void logging(Executor executor, int numofShared, String logFile){
        /**
         * logging
         */

        String filename = "";
        String[] header = {"epw","# of shared", "Hamlet throughput","Greta throughput", "Hamlet latency","Greta latency",
        "Hamlet memory", "Greta memory"};
        String[] data = new String[8];

        filename = logFile;

        data[0] = executor.getEpw()+"";
        data[1] = numofShared+"";
        data[2] = executor.getEpw()*1000/ executor.getHamletLatency() +"";
        data[3] = executor.getEpw()*1000/ executor.getGretaLatency() +"";

        data[4] = executor.getHamletLatency()+"";
        data[5] = executor.getGretaLatency()+"";

        data[6] = executor.getHamletMemory() +"";
        data[7] = executor.getGretaMemory() +"";


        File file = new File("output/"+ filename);


        try {
            if(!file.exists()){
                file.createNewFile();
                FileWriter outputfile = new FileWriter(file, true);
                CSVWriter writer = new CSVWriter(outputfile);
                writer.writeNext(header);
                writer.close();
            }
            FileWriter fileWriter = new FileWriter(file, true);
            CSVWriter writer = new CSVWriter(fileWriter);
            //write the data
            writer.writeNext(data);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
