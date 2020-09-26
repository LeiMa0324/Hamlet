package revision;


import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

/**
 * partition the stream into substreams
 */
@Data
public class StreamPartitioner {


    //<(vendorID, payment): sub streams>
    private HashMap<Integer[], SubStream> substreams;

    /**
     * given a query, a stream file, partition the stream into substreams
     * @param streamFile the stream file
     */
    public StreamPartitioner(WorkloadAnalyzer workloadAnalyzer, String streamFile){

        this.substreams = new HashMap<>();

        try{
            Scanner scanner = new Scanner(new File(streamFile));
            scanner.nextLine();

            while (scanner.hasNext()){
                String line = scanner.nextLine();
                String[] data = line.split(",");
                Integer vendorID = Integer.parseInt(data[1]);
                Float tripDistance = Float.parseFloat(data[5]);
                Integer payment = Integer.parseInt(data[10]);
                Float totalAmount = Float.parseFloat(data[17]);

                Integer[] keyPair = {vendorID, payment};

                //if the event is a relevant one in the workload
                //and it satisfies the predicate(trip distance >0)

                if (workloadAnalyzer.getMiniWorkloads().keySet().contains(vendorID)&&
                        tripDistance>workloadAnalyzer.getMiniWorkloads().get(vendorID).getTripPred()){

                    SubStream subStream ;

                    //create a new event
                    Event event = new Event(vendorID, payment,tripDistance, totalAmount);
                    //the actual key
                    Integer[] actualKey;

                    //if this sub stream doesn't exist, create a new sub-stream
                    if (!containsKey(keyPair)){
                        subStream = new SubStream(vendorID, payment, tripDistance);
                        actualKey = keyPair;

                    }else {

                        actualKey = findKey(keyPair);
                        subStream = substreams.get(actualKey); //get the substream if the it exists

                    }
                    subStream.addEvent(event); //add the event into the substream
                    substreams.put(actualKey, subStream); //put it back into the hashtable

                }
            }

        }catch (IOException e){
            e.printStackTrace();

        }

    }

    /**
     * check if the substream hashtable contains the key
     * @param key
     * @return
     */
    private boolean containsKey(Integer[] key){
        for (Integer[] k: substreams.keySet()){
            if (Arrays.equals(k, key)){
                return true;
            }
        }

        return false;
    }

    /**
     * find the actual key in the hashmap
     * @param keypair
     * @return
     */
    private Integer[] findKey(Integer[] keypair){
        for (Integer[] k: substreams.keySet()){
            if (Arrays.equals(k, keypair)){
                return k;
            }
        }

        return null;
    }
}
