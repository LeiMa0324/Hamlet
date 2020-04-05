package executor;

import com.opencsv.CSVWriter;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import sun.print.PSPrinterJob;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Experiment {
    private boolean isBaseline;
    private boolean isSynthetic;   //use the synthetic dataset or NYC Taxi
    private int expNo;

    //Synthetic setting
    private static int SYN_NUMBER_OF_SHARED = 10;
    private static String SYN_DEFAULT_STREAM = String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt", SYN_NUMBER_OF_SHARED);
    //syn-baseline
    private static Integer SYN_BASELINE_DEFAULT_EPW = 5000;
    private static String SYN_BASELINE_DEFAULT_WORKLOAD = "src/main/resources/Synthetic/Queries/BASELINE_DEFAULT_WORKLOAD.txt";
    //syn-hamlet and Greta
    private static Integer SYN_DEFAULT_EPW = 50000;
    private static String SYN_DEFAULT_WORKLOAD = "src/main/resources/Synthetic/Queries/HAMLET_DEFAULT_WORKLOAD.txt";


    //NYC Taxi setting
    private  String NYC_DEFAULT_STREAM = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
    private static Integer NYC_DEFAULT_EPW = 50000;
    private static String NYC_DEFAULT_WORKLOAD = "src/main/resources/NYCTaxi/Queries/HAMLET_DEFAULT_WORKLOAD.txt";


    public Experiment(boolean isSynthetic, boolean isBaseline, int expNo){
        this.isSynthetic = isSynthetic;
        this.isBaseline = isBaseline;
        this.expNo = expNo;
        System.out.println("Dataset: "+ (isSynthetic?"Synthetic":"NYCTaxi"));
        String experiemntName = isBaseline?"Baseline":"Hamelt And Greta";
        System.out.println(experiemntName+" Experiment started!");

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


        System.out.println("Vary Number of Shared Events.....");
        for (int numofShared = 5; numofShared<31;numofShared+=5){
            System.out.println("==================== number of Bs: "+numofShared+" ====================");
            String streamFile = String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt",numofShared);
            Executor executor = new Executor(streamFile, queryFile, epw, false);
            executor.run(isBaseline);
            String file = "EXP_"+expNo+"_varyNumOfSharedEvents.csv";
            String dataset = isSynthetic?"Synthetic":"NYCTaxi";
            String output = isBaseline?dataset+"/Baselines/"+file:dataset+"/HamletGreta/"+file;
            logging(executor, numofShared, output);

        }
    }

    /**
     * for Synthetic and NYC
     * Experiment2: vary epw
     */
    public void varyEventsPerWindow(){

        String queryFile = "";
        String streamFile = "";

        if (isSynthetic){
            queryFile = isBaseline? SYN_BASELINE_DEFAULT_WORKLOAD : SYN_DEFAULT_WORKLOAD;
            streamFile = SYN_DEFAULT_STREAM;
        }else {
            queryFile = NYC_DEFAULT_WORKLOAD;
            streamFile = NYC_DEFAULT_STREAM;

        }


        System.out.println("Vary Events per window...");
        int start_epw = isBaseline?4000: 40000;
        int end_epw = isBaseline?11000: 110000;
        int step_epw = isBaseline?1000: 10000;

        for (int epw = start_epw; epw<end_epw;epw+=step_epw){

            System.out.println("====================Evernts per Window: "+epw+"====================");


            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run(isBaseline);
            String file = "EXP_"+expNo+"_varyEPW.csv";
            String dataset = isSynthetic?"Synthetic":"NYCTaxi";

            String output = isBaseline?dataset+"/Baselines/"+file:dataset+"/HamletGreta/"+file;
            logging(executor, isSynthetic?SYN_NUMBER_OF_SHARED:-1, output);

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

        if (isSynthetic){
            epw = isBaseline?SYN_BASELINE_DEFAULT_EPW: SYN_DEFAULT_EPW;

        }else {
            epw = NYC_DEFAULT_EPW;

        }
        startnumofQ = isBaseline?5:10;
        endnumofQ = isBaseline?25:100;
        stepnumofQ = isBaseline?5:10;


        System.out.println("Vary Number of Queries.....");

        for (int numofQ = startnumofQ; numofQ<=endnumofQ;numofQ+=stepnumofQ){

            System.out.println("====================Number of Queries: "+numofQ+"====================");
            String streamFile = SYN_DEFAULT_STREAM;
            String folder = isBaseline?"BaselineQueries":"HamletGretaQueries";
            String dataset = isSynthetic?"Synthetic":"NYCTaxi";
            String queryFile =String.format("src/main/resources/"+dataset+"/Queries/"+folder+"/Workload_size_%d_len_3_pos_2.txt",numofQ);

            Executor executor = new Executor(streamFile, queryFile, epw,  false);
            executor.run(isBaseline);
            String file = (isSynthetic?"Syn_":"Nyc_")+"EXP_"+expNo+"_varyNumofQueries.csv";

            String output = isBaseline?dataset+"/Baselines/"+file:dataset+"/HamletGreta/"+file;

            logging(executor, isSynthetic?SYN_NUMBER_OF_SHARED:-1, output);

        }
    }

    public void logging(Executor executor, int numofShared, String logFile){
        /**
         * logging
         */

        String filename = "";
        String[] header = {"epw","# of shared","workload size",
                "Hamlet throughput","Greta throughput", "Sharon throughput","mcep throughput",
                "Hamlet latency","Greta latency","Sharon latency","mcep latency",
                "Hamlet memory", "Greta memory","Sharon memory","mcep,memory"};
        String[] data = new String[15];

        filename = logFile;

        data[0] = executor.getEpw()+"";
        data[1] = numofShared+"";
        data[2] = executor.getQueries().size()+"";

        data[3] = ((float)(executor.getEpw()*1000)/ executor.getHamletLatency()) +"";
        data[4] = ((float)(executor.getEpw()*1000)/ executor.getGretaLatency()) +"";
        data[5] = ((float)(executor.getEpw()*1000)/ executor.getSharonLatency()) +"";
        data[6] = ((float)(executor.getEpw()*1000)/ executor.getMcepLatency()) +"";


        data[7] = executor.getHamletLatency()+"";
        data[8] = executor.getGretaLatency()+"";
        data[9] = executor.getSharonLatency()+"";
        data[10] = executor.getMcepLatency()+"";



        data[11] = executor.getHamletMemory() +"";
        data[12] = executor.getGretaMemory() +"";
        data[13] = executor.getSharonMemory() +"";
        data[14] = executor.getMcepMemory() +"";



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
