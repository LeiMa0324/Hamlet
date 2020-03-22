package datagenerator;

import java.util.Random;


/**
 * every loop has the same number of 1-14, we vary the number of 2
 */
public class Generator {
    public static void main(String[] agrs){

//        generateStream_vary_sharedNum();
        // generate 10 queries, each has the length of 6 and the shared event is at position 2
        for (int l=10; l<100; l+=10){
            generateWorkload(l, 3, 2);

        }

    }

    /**
     * generate a bunch of streams which has different num of shared events in one graphlet
     */
    //TODO: a mixed workload, lenngth, position of shared
    static void generateStream_vary_sharedNum(){

        int numofevents = 100;      //fix the number of events in the stream file
        Random random = new Random();
        int numofunShared = random.nextInt(3)+3;   //a fixed random number of unshared events

        for (int numofShared = 15; numofShared<41;numofShared+=5){      //vary number of Bs for each stream file

            String streamFile = String.format("src/main/resources/Streams/Stream_shared_%d.txt",numofShared);
            StreamTemplate streamTemplate = new StreamTemplate(numofevents, numofunShared, numofShared, streamFile);
            streamTemplate.generate();
        }
    }

    /**
     * generate a workload
     */
    static void generateWorkload(int num, int length, int sharedPos){

        String queryFile = String.format("src/main/resources/Queries/Workload_size_%d_len_%d_pos_%d.txt",num,length,sharedPos);

        WorkloadTemplate workload = new WorkloadTemplate(num, length,  sharedPos,  queryFile);
        workload.generate();

    }
}
