package executor;

import com.opencsv.CSVWriter;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import sun.print.PSPrinterJob;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

public class Experiment {
    private boolean isBaseline;
    private int dataset;   //0: synthetic 1: NYC 2: Smart Home
    private boolean isLocal;
    private int expNo;

    //Synthetic setting
    private static int SYN_NUMBER_OF_SHARED = 10;
    private static String SYN_DEFAULT_STREAM;
    //syn-baseline
    private static Integer SYN_BASELINE_DEFAULT_EPW = 5000;
    private static String SYN_BASELINE_DEFAULT_WORKLOAD ;
    //syn-hamlet and Greta
    private static Integer SYN_DEFAULT_EPW = 50000;
    private static String SYN_DEFAULT_WORKLOAD ;


    //NYC Taxi setting
    private  String NYC_DEFAULT_STREAM;
    private static Integer NYC_DEFAULT_EPW = 50000;
    private static String NYC_DEFAULT_WORKLOAD ;

    //Smart home setting
    private  String SH_DEFAULT_STREAM;
    private static Integer SH_DEFAULT_EPW = 50000;
    private static String SH_DEFAULT_WORKLOAD ;

    //Dynamic home setting


    private static Integer DYN_DEFAULT_EPW = 10000;
    private static Integer DYN_DEFAULT_BATCHSIZE = 500;
    private static Integer DYN_DEFAULT_SNAPSHOTS = 50;
    private static String DYN_DEFAULT_WORKLOAD ;




    static HashMap<Integer, String> datasetHash;
    static
    {
        datasetHash = new HashMap<Integer, String>();
        datasetHash.put(0, "Synthetic");
        datasetHash.put(1, "NYCTaxi");
        datasetHash.put(2, "SmartHome");

    }

    /**
     *
     * @param dataset   select the dataset //0: synthetic 1: NYC 2: smart home
     * @param isBaseline running baselines or hamlet VS.Greta
     * @param expNo the experiment no
     * @param isLocal   is running on IDE or by a jar
     */

    public Experiment(int dataset, boolean isBaseline, int expNo, boolean isLocal){
        this.dataset = dataset;
        this.isBaseline = isBaseline;
        this.expNo = expNo;
        System.out.println("Dataset: "+ datasetHash.get(dataset));
        String experiemntName = isBaseline?"Baseline":"Hamelt And Greta";
        System.out.println(experiemntName+" Experiment started!");
        this.isLocal = isLocal;

        //local running

        if (this.isLocal){
            SYN_DEFAULT_STREAM = String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt", SYN_NUMBER_OF_SHARED);
            SYN_BASELINE_DEFAULT_WORKLOAD = "src/main/resources/Synthetic/Queries/BASELINE_DEFAULT_WORKLOAD.txt";
            SYN_DEFAULT_WORKLOAD = "src/main/resources/Synthetic/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            NYC_DEFAULT_STREAM = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
            NYC_DEFAULT_WORKLOAD = "src/main/resources/NYCTaxi/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            SH_DEFAULT_STREAM = "src/main/resources/SmartHome/Streams/Home_stream.csv";
            SH_DEFAULT_WORKLOAD = "src/main/resources/SmartHome/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            DYN_DEFAULT_WORKLOAD = "src/main/resources/NYCTaxi/Queries/HamletGretaQueries/Workload_size_50_len_3_pos_2.txt";


        }else { //running by jar

            SYN_DEFAULT_STREAM = String.format("Synthetic/Streams/Stream_shared_%d.txt", SYN_NUMBER_OF_SHARED);
            SYN_BASELINE_DEFAULT_WORKLOAD = "Synthetic/Queries/BASELINE_DEFAULT_WORKLOAD.txt";
            SYN_DEFAULT_WORKLOAD = "Synthetic/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            NYC_DEFAULT_STREAM = "NYCTaxi/Streams/Taxi_stream.csv";
            NYC_DEFAULT_WORKLOAD = "NYCTaxi/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            SH_DEFAULT_STREAM = "SmartHome/Streams/Home_stream.csv";
            SH_DEFAULT_WORKLOAD = "SmartHome/Queries/HAMLET_DEFAULT_WORKLOAD.txt";

            DYN_DEFAULT_WORKLOAD = "NYCTaxi/Queries/HamletGretaQueries/Workload_size_50_len_3_pos_2.txt";



        }

    }

    /**
     * Only for synthetic data
     * Experiment1: fix epw, vary shared events per graphlet
     *
     */

    public void varyNumOfSharedEvents(){
        String queryFile = "";
        Integer epw = 0;

        queryFile = isBaseline? SYN_BASELINE_DEFAULT_WORKLOAD : SYN_DEFAULT_WORKLOAD;
        epw = isBaseline?SYN_BASELINE_DEFAULT_EPW: SYN_DEFAULT_EPW;

        for (int iter =1;iter<4;iter++){
            System.out.println("Vary Number of Shared Events.....");
            for (int numofShared = 5; numofShared<31;numofShared+=5){
                System.out.println("====================Iter: "+iter+" number of Bs: "+numofShared+" ====================");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
                String streamFile = isLocal?String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt",numofShared):
                        String.format("Synthetic/Streams/Stream_shared_%d.txt",numofShared);

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.run(isBaseline);

                //log setting
                String file = "EXP_"+expNo+"_varyNumOfSharedEvents.csv";
                String dataset = datasetHash.get(this.dataset);
                String output = isBaseline?dataset+"/Baselines/"+file:dataset+"/HamletGreta/"+file;
                //log
                logging(executor, numofShared, iter,output);

        }
        }
    }

    /**
     * for Synthetic and NYC
     * Experiment2: vary epw
     */
    public void varyEventsPerWindow(){

        String queryFile = "";
        String streamFile = "";
        int start_epw = 0;
        int end_epw = 0;
        int step_epw =0;

        switch (this.dataset){
            case 0: //synthetic
                queryFile = isBaseline? SYN_BASELINE_DEFAULT_WORKLOAD : SYN_DEFAULT_WORKLOAD;
                streamFile = SYN_DEFAULT_STREAM;
                start_epw = isBaseline?4000: 40000;
                end_epw = isBaseline?11000: 110000;
                step_epw = isBaseline?1000: 10000;
                break;
            case 1: //nyc
                queryFile = NYC_DEFAULT_WORKLOAD;
                streamFile = NYC_DEFAULT_STREAM;
                start_epw = 40000;
                end_epw = 110000;
                step_epw = 10000;
                break;
            case 2: //smart home
                queryFile = SH_DEFAULT_WORKLOAD;
                streamFile = SH_DEFAULT_STREAM;
                start_epw = 40000;
                end_epw = 110000;
                step_epw = 10000;

        }


        System.out.println("Vary Events per window...");

        // run three times
        for (int iter =1;iter<4;iter++) {

            for (int epw = start_epw; epw < end_epw; epw += step_epw) {

                System.out.println("====================Iter:"+iter+" Evernts per Window: " + epw + "====================");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
                System.out.println("query file:"+queryFile);
                System.out.println("stream file"+streamFile);

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.run(isBaseline);

                //log setting
                String file = "EXP_" + expNo + "_varyEPW.csv";
                String dataset = datasetHash.get(this.dataset);
                String output = isBaseline ? dataset + "/Baselines/" + file : dataset + "/HamletGreta/" + file;
                //log
                logging(executor, this.dataset==0 ? SYN_NUMBER_OF_SHARED : -1,iter, output);

            }
        }

    }

    /**
     * for Synthetic and NYC
     * Experiment3: fix # of shared events, number of shared events, vary num of queries
     */
    public void varyNumofQueries(){
        int epw = 0;
        int startnumofQ = 0;
        int endnumofQ = 0;
        int stepnumofQ = 0;

        String streamFile = "";


        switch (this.dataset){
            case 0: //synthetic
                epw = isBaseline?SYN_BASELINE_DEFAULT_EPW: SYN_DEFAULT_EPW;
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

        // run each experiment three times
        for (int iter=1;iter<4;iter++){

        System.out.println("Vary Number of Queries.....");

        for (int numofQ = startnumofQ; numofQ<=endnumofQ;numofQ+=stepnumofQ){

            System.out.println("====================Iter: "+iter+" Number of Queries: "+numofQ+"====================");
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
            System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
            String folder = isBaseline?"BaselineQueries":"HamletGretaQueries";
            String dataset = datasetHash.get(this.dataset);
            String queryFile =isLocal?String.format("src/main/resources/"+dataset+"/Queries/"+folder+"/Workload_size_%d_len_3_pos_2.txt",numofQ):
                    String.format(dataset+"/Queries/"+folder+"/Workload_size_%d_len_3_pos_2.txt",numofQ);

            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run(isBaseline);
            //log setting
            String file = "EXP_"+expNo+"_varyNumofQueries.csv";
            String output = isBaseline?dataset+"/Baselines/"+file:dataset+"/HamletGreta/"+file;
            //log
            logging(executor, this.dataset==0?SYN_NUMBER_OF_SHARED:-1,iter, output);

        }
        }
    }

    /**
     * dynamic hamlet experiment
     */
    public void Dynamic_varyNumofSnapshots(){

        int[] snapshotNums = {20,30,40,50,60,70};
        String streamFile = NYC_DEFAULT_STREAM;
        String queryFile = DYN_DEFAULT_WORKLOAD;
        int epw = DYN_DEFAULT_EPW;

        int batchsize = DYN_DEFAULT_BATCHSIZE;

        for (int iter =1; iter<11;iter++) {
            for (int snapshot : snapshotNums) {
                System.out.println("====================number of snapshots:" + snapshot + " ====================");
                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.decisionRun(batchsize, snapshot);
                String output = "DynamicHamlet/EXP_" + expNo + "_Dynamic_varyNumofSnapshots.csv";
                dynamicLogging(executor, snapshot, batchsize, epw, 1, iter, output);

            }
        }

    }

    public void Dynamic_varyEPW(){

        String streamFile = NYC_DEFAULT_STREAM;
        String queryFile = DYN_DEFAULT_WORKLOAD;
        int snapshot = DYN_DEFAULT_SNAPSHOTS;

        int start_epw = DYN_DEFAULT_EPW;
        int end_epw = DYN_DEFAULT_EPW*2;
        int step_epw = DYN_DEFAULT_EPW/5;

        int batchsize = DYN_DEFAULT_BATCHSIZE;

        for (int iter = 1; iter<11;iter++) {

            for (int epw = start_epw; epw <= end_epw; epw += step_epw) {
                System.out.println("====================EPW:" + epw + " ====================");

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.decisionRun(batchsize, snapshot);
                String output = "DynamicHamlet/EXP_" + expNo + "_Dynamic_varyEPW.csv";
                dynamicLogging(executor, snapshot, batchsize, epw, 0, iter, output);

            }
        }

    }

    public void Dynamic_varyBatchSize(){
        String streamFile = NYC_DEFAULT_STREAM;
        String queryFile = DYN_DEFAULT_WORKLOAD;
        int epw = DYN_DEFAULT_EPW;

        int snapshot = DYN_DEFAULT_SNAPSHOTS;

        int start_batchsize = 300;
        int end_batchsize = 800;
        int step_batchsize = 100;


        for (int iter =1; iter<11;iter++) {
            for (int batchsize = start_batchsize; batchsize < end_batchsize; batchsize += step_batchsize) {

                System.out.println("==================== Batchsize :" + batchsize + " ====================");

                Executor executor = new Executor(streamFile, queryFile, epw, false);
                executor.decisionRun(batchsize, snapshot);
                String output = "DynamicHamlet/EXP_" + expNo + "_Dynamic_varyBatchSize.csv";
                dynamicLogging(executor, snapshot, batchsize, epw, 0, iter, output);

            }
        }

    }



    public void dynamicLogging(Executor executor, int numofSnapshots, int batchsize, int epw, int window,
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
        data[i] = batchsize+"";        i++;

        data[i] = ((float)(executor.getEpw()*1000)/ executor.getStaticHamletLatency()) +"";        i++;
        data[i] = ((float)(executor.getEpw()*1000)/ executor.getDynamicHamletLatency()) +"";        i++;

        data[i] = executor.getStaticHamletLatency()+"";        i++;
        data[i] = executor.getDynamicHamletLatency()+"";        i++;

        data[i] = executor.getStaticHamletMemory()+"";        i++;
        data[i] = executor.getDynamicHamletMemory()+"";    i++;

        data[i] = executor.getDynamicHamelt().mergeNum+"";    i++;
        data[i] = executor.getDynamicHamelt().splitNum+"";

        File file = new File("output/"+ logFile);

        writeCSV(file,header, data);
    }

    public void logging(Executor executor, int numofShared,int iternum, String logFile){
        /**
         * logging
         */

        String[] header = {"iter","epw","# of shared","workload size",
                "Hamlet throughput","Greta throughput", "Sharon throughput","mcep throughput",
                "Hamlet latency","Greta latency","Sharon latency","mcep latency",
                "Hamlet memory", "Greta memory","Sharon memory","mcep,memory"};
        String[] data = new String[16];

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



        File file = new File("output/"+ logFile);
        writeCSV(file, header, data);



    }

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
