package StreamGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


/**
 * every loop has the same number of 1-14, we vary the number of 2
 */
public class Generator {
    public static void main(String[] agrs){
        int numofevents = 1000000;      //fix the number of events in the stream file
        Random random = new Random();
        int numofunShared = random.nextInt(3)+3;   //a fixed random number of unshared events
        for (int numofShared = 15; numofShared<41;numofShared+=5){      //vary number of Bs for each stream file
            singleStreamGenerator(numofevents,numofShared, numofunShared);
        }

        queryGenerator(3,3);

    }

    static void singleStreamGenerator(int numofevents, int numofShared, int numofunshared){
        String file_of_stream = String.format("src/main/resources/Streams/GenStream_%d.txt",numofShared);
        try {
            File output_file = new File(file_of_stream);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int second_id = 0;
            for (int j =0;second_id< numofevents;j++){

                //Generate 14 unshared event types, each event type has numofunshared consecutive events
                for (int e = 1;e<15;e++){
                    if (e==2){
                        continue;
                    }
                    for (int i=0;i<numofunshared;i++){
                        output.append(String.valueOf(second_id)).append(",").append(String.valueOf(e)).append(",1").append('\n');
                        second_id++;
                    }
                }

                //Generate shared events
                for (int i=0;i<numofShared;i++){
                    output.append(String.valueOf(second_id)).append(",").append("2").append(",1").append('\n');
                    second_id++;
                }

            }
            output.close();
        } catch (IOException e) { e.printStackTrace(); }

    }

    /**
     * generate random events from 1-14(except 2), then add 2+ at the end of the query
     * @param querynum how many queries needs to be generated
     * @param queryLength   the length of the queries
     */
    //TODO: may vary query length in one workload
    static void queryGenerator(int querynum, int queryLength){
        String file_of_query = String.format("src/main/resources/Queries/GenQueries_%d.txt",queryLength);
        try{
            File output_file = new File(file_of_query);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));

            for (int i=0;i<querynum;i++){
                StringBuilder q = new StringBuilder();
                for (int l =0;l<queryLength-1;l++){
                    Random random = new Random();
                    //TODO: make sure no repeat
                    int randomUnshared = random.nextInt(15);
                    while(randomUnshared==2){
                        randomUnshared = random.nextInt(15);
                    }
                    q.append(randomUnshared+",");

                }
                q.append("2+\n");
                output.append(q.toString());
            }
            output.close();
        }catch (IOException e) { e.printStackTrace(); }


    }
}
