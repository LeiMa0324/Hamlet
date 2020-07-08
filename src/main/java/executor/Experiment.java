package executor;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * All the experiments in the paper. Each method is an experiment.
 * Each experiment run the executor several times, varying the variable of interest,
 * For each experiment, log the throughput, latency, memory for each model
 */

public class Experiment {

    private boolean isBaseline;
    private int dataset;   //0: Ride sharing 1: NYC 2: Smart Home

    //true: running on IDE locally, false: running by jar on a server
    private boolean isLocal;

    //the experiment No.
    private int expNo;

    /**
     * Ride sharing datatset experiment setting
     * hamlet, Greta, Sharon, MCEP
     * Figure 14, 16
     */

    //default number of shared events per graphlet
    private static int SYN_NUMBER_OF_SHARED = 10;

    //default stream file for Ride Sharing dataset
    private static String SYN_DEFAULT_STREAM;

    //default events per window for Ride Sharing dataset
    private static Integer SYN_BASELINE_DEFAULT_EPW = 5000;

    //default workload for Ride Sharing dataset
    private static String SYN_BASELINE_DEFAULT_WORKLOAD ;


    /**
     * NYC Taxi datatset experiment setting
     * hamlet, Greta
     * Figure 15
     */

    //default stream file for NYC Taxi dataset
    private  String NYC_DEFAULT_STREAM;

    //default events per window for NYC Taxi dataset
    private static Integer NYC_DEFAULT_EPW = 50000;

    //default workload for NYC Taxi dataset
    private static String NYC_DEFAULT_WORKLOAD ;

    /**
     * Smart Home datatset experiment setting
     * hamlet, Greta
     * Figure 15
     */

    //default stream file for Smart Home dataset
    private  String SH_DEFAULT_STREAM;

    //default events per window for Smart Home dataset
    private static Integer SH_DEFAULT_EPW = 50000;

    //default workload for Smart Home dataset
    private static String SH_DEFAULT_WORKLOAD ;

    /**
     * Dynamic hamlet experiment setting
     * static hamlet, dynamic hamlet
     * Figure 17
     */

    // default events per window
    private static Integer DYN_DEFAULT_EPW = 10000;

    //default burst size
    private static Integer DYN_DEFAULT_BATCHSIZE = 500;

    //default number of snapshots representing the predicates
    private static Integer DYN_DEFAULT_SNAPSHOTS = 50;

    //default workload
    private static String DYN_DEFAULT_WORKLOAD ;


    static HashMap<Integer, String> datasetHash;
    static
    {
        datasetHash = new HashMap<Integer, String>();
        datasetHash.put(0, "RideSharing");
        datasetHash.put(1, "NYCTaxi");
        datasetHash.put(2, "SmartHome");

    }

    /**
     *
     * @param dataset   select the dataset //0: Ride sharing 1: NYC 2: smart home
     * @param isBaseline running baselines(Hamlet, Greta, Sharon, MCEP) or just hamlet VS.Greta
     * @param expNo the experiment no
     * @param isLocal   is running on IDE locally or by a jar on a server
     */

    public Experiment(int dataset, boolean isBaseline, int expNo, boolean isLocal){
        this.dataset = dataset;
        this.isBaseline = isBaseline;
        this.expNo = expNo;
        System.out.println("Dataset: "+ datasetHash.get(dataset));
        String experiemntName = isBaseline?"Baseline":"Hamlet And Greta";
        System.out.println(experiemntName+" Experiment started!");
        this.isLocal = isLocal;




        if (this.isLocal){
            //local running
            SYN_DEFAULT_STREAM = String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt", SYN_NUMBER_OF_SHARED);
            SYN_BASELINE_DEFAULT_WORKLOAD = "src/main/resources/Synthetic/Queries/BASELINE_DEFAULT_WORKLOAD.txt";

            NYC_DEFAULT_STREAM = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
            NYC_DEFAULT_WORKLOAD = "src/main/resources/NYCTaxi/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            SH_DEFAULT_STREAM = "src/main/resources/SmartHome/Streams/Home_stream.csv";
            SH_DEFAULT_WORKLOAD = "src/main/resources/SmartHome/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            DYN_DEFAULT_WORKLOAD = "src/main/resources/NYCTaxi/Queries/HamletGretaQueries/Workload_size_50_len_3_pos_2.txt";


        }else { //running by jar on the server

            SYN_DEFAULT_STREAM = String.format("Synthetic/Streams/Stream_shared_%d.txt", SYN_NUMBER_OF_SHARED);
            SYN_BASELINE_DEFAULT_WORKLOAD = "Synthetic/Queries/BASELINE_DEFAULT_WORKLOAD.txt";

            NYC_DEFAULT_STREAM = "NYCTaxi/Streams/Taxi_stream.csv";
            NYC_DEFAULT_WORKLOAD = "NYCTaxi/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            SH_DEFAULT_STREAM = "SmartHome/Streams/Home_stream.csv";
            SH_DEFAULT_WORKLOAD = "SmartHome/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            DYN_DEFAULT_WORKLOAD = "NYCTaxi/Queries/HamletGretaQueries/Workload_size_50_len_3_pos_2.txt";



        }

    }


    /**
     * Hamlet versus State-of-the-art Approaches
     * vary events per window(epw), fix number of shared events per graphlet, # of queries
     * Figure 14.a, 14.c, 15.a, 15.c, 16.a
     */
    public void varyEventsPerWindow(){

        String queryFile = "";
        String streamFile = "";
        int start_epw = 0;
        int end_epw = 0;
        int step_epw =0;

        /**
         * select data set and the epw setting for each datat set
         */
        switch (this.dataset){
            case 0: //Ride sharing
                queryFile = SYN_BASELINE_DEFAULT_WORKLOAD;
                streamFile = SYN_DEFAULT_STREAM;
                start_epw = isBaseline?4000: 40000;
                end_epw = isBaseline?11000: 110000;
                step_epw = isBaseline?1000: 10000;
                break;
            case 1: //NYC Taxi
                queryFile = NYC_DEFAULT_WORKLOAD;
                streamFile = NYC_DEFAULT_STREAM;
                start_epw = 40000;
                end_epw = 110000;
                step_epw = 10000;
                break;
            case 2: //Smart Home
                queryFile = SH_DEFAULT_WORKLOAD;
                streamFile = SH_DEFAULT_STREAM;
                start_epw = 40000;
                end_epw = 110000;
                step_epw = 10000;

        }


        System.out.println("Vary Events per window...");

        // run three times
        for (int iter =1;iter<4;iter++) {
            // iterate over the epw
            for (int epw = start_epw; epw < end_epw; epw += step_epw) {

                System.out.println("====================Iter:"+iter+" Evernts per Window: " + epw + "====================");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println(df.format(new Date()));
                System.out.println("query file:"+queryFile);
                System.out.println("stream file"+streamFile);

                // run the executor with corresponding settings
                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.run(isBaseline);

                //log setting
                String file = "EXP_" + expNo + "_varyEPW.csv";
                String dataset = datasetHash.get(this.dataset);
                String output = dataset + "/" + file;
                //log
                logging(executor, this.dataset==0 ? SYN_NUMBER_OF_SHARED : -1,iter, output);

            }
        }

    }

    /**
     * Hamlet versus State-of-the-art Approaches
     * vary num of queries, fix epw, number of shared events per graphlet
     * Figure 14.b, 14.d, 15.b, 15.d, 16.b
     */
    public void varyNumofQueries(){
        int epw = 0;
        int startnumofQ = 0;
        int endnumofQ = 0;
        int stepnumofQ = 0;

        String streamFile = "";

        /**
         * select data set and the epw setting for each datat set
         */
        switch (this.dataset){
            case 0: //Ride sharing
                epw = SYN_BASELINE_DEFAULT_EPW;
                streamFile = SYN_DEFAULT_STREAM;
                break;
            case 1: //nyc
                epw = NYC_DEFAULT_EPW;
                streamFile = NYC_DEFAULT_STREAM;
                break;
            case 2: //smart home
                epw = SH_DEFAULT_EPW;
                streamFile = SH_DEFAULT_STREAM;
                break;

        }


        startnumofQ = isBaseline?5:10;
        endnumofQ = isBaseline?25:100;
        stepnumofQ = isBaseline?5:10;

        // run three times
        for (int iter=1;iter<4;iter++){

            System.out.println("Vary Number of Queries.....");
            // varying number of queries
            for (int numofQ = startnumofQ; numofQ<=endnumofQ;numofQ+=stepnumofQ){

                System.out.println("====================Iter: "+iter+" Number of Queries: "+numofQ+"====================");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println(df.format(new Date()));
                String folder = isBaseline?"BaselineQueries":"HamletGretaQueries";
                String dataset = datasetHash.get(this.dataset);
                String queryFile =isLocal?String.format("src/main/resources/"+dataset+"/Queries/"+folder+"/Workload_size_%d_len_3_pos_2.txt",numofQ):
                        String.format(dataset+"/Queries/"+folder+"/Workload_size_%d_len_3_pos_2.txt",numofQ);

                Executor executor = new Executor(streamFile, queryFile, epw,  false);
                executor.run(isBaseline);
                //log setting
                String file = "EXP_"+expNo+"_varyNumofQueries.csv";
                String output = dataset+"/"+file;
                //log
                logging(executor, this.dataset==0?SYN_NUMBER_OF_SHARED:-1,iter, output);

            }
        }
    }

    /**
     * Dynamic versus Static Sharing Decision.
     * vary epw, fix number of queries, number of burst size
     * Figure 17.a, 17.c
     */

    public void Dynamic_varyEPW(){

        String streamFile = NYC_DEFAULT_STREAM;
        String queryFile = DYN_DEFAULT_WORKLOAD;

        int snapshot = DYN_DEFAULT_SNAPSHOTS;
        int start_epw = DYN_DEFAULT_EPW;
        int end_epw = DYN_DEFAULT_EPW*2;
        int step_epw = DYN_DEFAULT_EPW/5;

        int batchsize = DYN_DEFAULT_BATCHSIZE;

        // run 10 times
        for (int iter = 1; iter<11;iter++) {

            // varying epw
            for (int epw = start_epw; epw <= end_epw; epw += step_epw) {
                System.out.println("====================EPW:" + epw + " ====================");

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.decisionRun(batchsize, snapshot);

                //log file sub path
                String output = "DynamicHamlet/EXP_" + expNo + "_Dynamic_varyEPW.csv";

                //logging
                dynamicLogging(executor, snapshot, batchsize, epw, 0, iter, output);

            }
        }

    }

    /**
     * Dynamic versus Static Sharing Decision.
     * vary number of burst size, fix epw, number of queries
     * Figure 17.b, 17.d
     */

    public void Dynamic_varyBurstSize(){

        String streamFile = NYC_DEFAULT_STREAM;
        String queryFile = DYN_DEFAULT_WORKLOAD;
        int epw = DYN_DEFAULT_EPW;
        int snapshot = DYN_DEFAULT_SNAPSHOTS;

        int start_burstsize = 300;
        int end_burstsize = 800;
        int step_burstsize = 100;

        // run 10 times
        for (int iter =1; iter<11;iter++) {

            // varying burst size
            for (int burstsize = start_burstsize; burstsize < end_burstsize; burstsize += step_burstsize) {

                System.out.println("==================== Burst size :" + burstsize + " ====================");

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.decisionRun(burstsize, snapshot);

                //log file sub path
                String output = "DynamicHamlet/EXP_" + expNo + "_Dynamic_varyBatchSize.csv";

                //logging
                dynamicLogging(executor, snapshot, burstsize, epw, 0, iter, output);

            }
        }

    }


    /**
     * logging method for Dynamic versus Static Sharing Decision.
     * @param executor the executor that run dynamic vs. static
     * @param numofSnapshots number of snapshots
     * @param burstsize the burst size
     * @param epw the events per window
     * @param window window number
     * @param iter iteration number
     * @param logFile the log file
     */
    public void dynamicLogging(Executor executor, int numofSnapshots, int burstsize, int epw, int window,
                               int iter, String logFile){

        String[] header = {"window", "iter", "epw","# of snapshots","batch size",
                "Static Ham throughput","Dynamic Ham throughput",
                "Static Ham latency","Dynamic Ham latency",
                "Static Ham memory", "Dynamic Ham memory",
                "Merge num","Split num"
        };
        String[] data = new String[13];
        int i =0;
        data[i] = window+"";
        i++;
        data[i] = iter+"";        i++;
        data[i] = epw+"";        i++;
        data[i] = numofSnapshots+"";        i++;
        data[i] = burstsize+"";        i++;

        data[i] = ((float)(executor.getEpw()*1000)/ executor.getStaticHamletLatency()) +"";        i++;
        data[i] = ((float)(executor.getEpw()*1000)/ executor.getDynamicHamletLatency()) +"";        i++;

        data[i] = executor.getStaticHamletLatency()+"";        i++;
        data[i] = executor.getDynamicHamletLatency()+"";        i++;

        data[i] = executor.getStaticHamletMemory()+"";        i++;
        data[i] = executor.getDynamicHamletMemory()+"";    i++;

        data[i] = executor.getDynamicHamelt().mergeNum+"";    i++;
        data[i] = executor.getDynamicHamelt().splitNum+"";


        checkFolder("output/"+logFile.split("/")[0]);

        File file = new File("output/"+ logFile);

        writeCSV(file,header, data);
    }

    /**
     * logging method for Hamlet versus State-of-the-art Approaches
     * @param executor the executor that runs all the approaches
     * @param numofShared the number of shared events per graphlet
     * @param iternum the interation number
     * @param logFile the log file
     */
    public void logging(Executor executor, int numofShared,int iternum, String logFile){

        // set up header for the csv
        String[] header = {"iter","epw","# of shared","workload size",
                "Hamlet throughput","Greta throughput", "Sharon throughput","mcep throughput",
                "Hamlet latency","Greta latency","Sharon latency","mcep latency",
                "Hamlet memory", "Greta memory","Sharon memory","mcep,memory"};

        //data
        String[] data = new String[16];

        //iteration number
        data[0] = iternum+"";

        data[1] = executor.getEpw()+"";
        data[2] = numofShared+"";
        data[3] = executor.getQueries().size()+"";

        data[4] = ((float)(executor.getEpw()*1000)/ executor.getHamletLatency()) +"";
        data[5] = ((float)(executor.getEpw()*1000)/ executor.getGretaLatency()) +"";
        data[6] = ((float)(executor.getEpw()*1000)/ executor.getSharonLatency()) +"";
        data[7] = ((float)(executor.getEpw()*1000)/ executor.getMcepLatency()) +"";


        data[8] = executor.getHamletLatency()+"";
        data[9] = executor.getGretaLatency()+"";
        data[10] = executor.getSharonLatency()+"";
        data[11] = executor.getMcepLatency()+"";



        data[12] = executor.getHamletMemory() +"";
        data[13] = executor.getGretaMemory() +"";
        data[14] = executor.getSharonMemory() +"";
        data[15] = executor.getMcepMemory() +"";

        checkFolder("output/"+logFile.split("/")[0]);


        File file = new File("output/"+ logFile);
        writeCSV(file, header, data);


    }

    /**
     * create the folder if it doesn't exist
     * @param path path of the directory
     */
    void checkFolder(String path){

        File folder = new File(path);

        if (!folder.exists() && !folder.isDirectory()) {
            folder.mkdirs();
            System.out.println("directory created");
        } else {
            System.out.println();
        }

    }


    /**
     * method of writing a line into a csv file
     * @param file file name
     * @param header header of the csv
     * @param data a line of data
     */
    public void writeCSV(File file, String[] header, String[] data){
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
