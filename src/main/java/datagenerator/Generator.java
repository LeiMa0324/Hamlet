package datagenerator;

import java.util.Random;


/**
 * every loop has the same number of 1-14, we vary the number of 2
 */
public class Generator {
    public static void main(String[] agrs){

         //synthetic baseline Workload
//        for (int l=5; l<30; l+=5){
//            generateWorkload(l, 3,2, true, true, true);
//
//        }
        //synthetic hamlet Workload
//        for (int l=10; l<110; l+=10){
//            generateWorkload(l, 3, 2, true, false,true);
//
//        }

        //NYC Taxi Workload
        for (int l=10; l<110; l+=10){
            generateWorkload(l, 3, 0, true, false,true);

        }
//        generateStream_vary_sharedNum();

    }

    /**
     * generate a bunch of streams which has different num of shared events in one graphlet
     */
    static void generateStream_vary_sharedNum(){

        int numofevents = 100000;      //fix the number of events in the stream file
        Random random = new Random();
        int numofunShared = random.nextInt(3)+3;   //a fixed random number of unshared events

        for (int numofShared = 5; numofShared<35;numofShared+=5){      //vary number of Bs for each stream file

            String streamFile = String.format("src/main/resources/Synthetic/Streams/Stream_shared_%d.txt",numofShared);
            StreamTemplate streamTemplate = new StreamTemplate(numofevents, numofunShared, numofShared, streamFile);
            streamTemplate.generate();
        }
    }

    /**
     * generate a Workload
     */
    static void generateWorkload(int num, int length, int sharedPos, boolean isSynthetic, boolean isBaseline, boolean isDiverse){
        String queryFile = "";
        String dataset = isSynthetic?"Synthetic":"NYCTaxi";
        String folder = isBaseline?"BaselineQueries":"HamletGretaQueries";

        queryFile = String.format("src/main/resources/"+dataset+"/Queries/"+folder+"/Workload_size_%d_len_%d_pos_%d.txt",num,length,sharedPos);

        WorkloadTemplate workload = new WorkloadTemplate(num, length, isSynthetic?14:10, sharedPos,  queryFile,isSynthetic?2:1, isDiverse);
        workload.generate();

    }
}
