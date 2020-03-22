package executor;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Each static method is one experiment
 */
public class Main {
    static Integer default_epw = 50000;
    static Integer default_num_of_shared = 20;
    static Integer default_workload_size = 10;

    public static void main(String[] args){

        varyEventsPerWindow();

//        varyNumOfSharedEvents();
//        varyNumofQueries();
    }

    /**
     * Experiment1: fix epw, vary shared events per graphlet
     */

    public static void varyNumOfSharedEvents(){
        String queryFile = String.format("src/main/resources/Queries/Workload_size_%d_len_3_pos_2.txt",default_workload_size);


        for (int numofShared = 15; numofShared<41;numofShared+=5){
            System.out.println("==================== number of Bs: "+numofShared+" ====================");
            String streamFile = String.format("src/main/resources/Streams/GenStream_%d.txt",numofShared);
            Executor executor = new Executor(streamFile, queryFile, default_epw, false);
            executor.run();
            logging(executor, numofShared, "varyNumofShared.csv");

        }
    }

    /**
     * Experiment2: fix # of shared events, vary epw
     */
    public static void varyEventsPerWindow(){
//        String queryFile =String.format("src/main/resources/Queries/Workload_size_%d_len_3_pos_2.txt",default_workload_size);
        String queryFile =String.format("src/main/resources/Queries/GenQueries_3.txt");

        System.out.println("#of shared events per graphlet: 20");

        for (int epw = 50000; epw<60000;epw+=10000){

            System.out.println("====================Evernts per Window: "+epw+"====================");
            String streamFile = "src/main/resources/Streams/GenStream_20.txt";

            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run();
            logging(executor, 20, "varyEPW.csv");

        }


    }

    /**
     * Experiment3: fix # of shared events, number of shared events, vary num of queries
     */
    public static void varyNumofQueries(){
        System.out.println("#of shared events per graphlet: 20");
        System.out.println("#epw: 50000");
        int epw = 50000;
        for (int numofQ = 10; numofQ<100;numofQ+=10){

            System.out.println("====================Number of Queries: "+numofQ+"====================");
            String streamFile = "src/main/resources/Streams/GenStream_20.txt";
            String queryFile =String.format("src/main/resources/Queries/Workload_size_%d_len_3_pos_2.txt",numofQ);

            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run();
            logging(executor, 20, "varyNumofQueries.csv");

        }
    }

    public static void logging(Executor executor, int numofShared, String logFile){
        /**
         * logging
         */

        String filename = "";
        String[] header = {"epw","# of shared","workload size",
                "Hamlet throughput","Greta throughput", "Sharon throughput",
                "Hamlet latency","Greta latency","Sharon latency",
                "Hamlet memory", "Greta memory","Sharon memory"};
        String[] data = new String[12];

        filename = logFile;

        data[0] = executor.getEpw()+"";
        data[1] = numofShared+"";
        data[2] = executor.getQueries().size()+"";

        data[3] = executor.getEpw()*1000/ executor.getHamletLatency() +"";
        data[4] = executor.getEpw()*1000/ executor.getGretaLatency() +"";
        data[5] = executor.getEpw()*1000/ executor.getSharonLatency() +"";

        data[6] = executor.getHamletLatency()+"";
        data[7] = executor.getGretaLatency()+"";
        data[8] = executor.getSharonLatency()+"";


        data[9] = executor.getHamletMemory() +"";
        data[10] = executor.getGretaMemory() +"";
        data[11] = executor.getSharonMemory() +"";


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
