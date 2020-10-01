package datagenerator;

import lombok.Data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * control the number of queries, the length of queries, the position of shared events
 * all queries in the StockWorkloadTemplate share the same setting
 */
@Data
public class WorkloadTemplate {
    private String query;
    private String queryFile;
    private int num;        //number of queries
    private int length;     // the length of each query
    private int sharedPos;
    private int sharedE;
    private int eventBound;     // the largest event type
    private boolean randomLength;       //open the random length

    /**
     * create a StockWorkloadTemplate of queries
     * @param num the number of queries
     * @param length the default length of all queries
     * @param eventBound the biggest event number
     * @param sharedPos the position of the shared event type
     * @param queryFile the query file
     * @param sharedE the shared event type
     * @param randomLength if use random length
     */
    public WorkloadTemplate(int num,int length, int eventBound, int sharedPos, String queryFile, int sharedE, boolean randomLength){
        this.query = "";

        //number of queries
        this.num = num;
        this.queryFile = queryFile;
        this.length = length;
        this.sharedPos = sharedPos; //start from 1
        this.sharedE = sharedE;
        this.eventBound = eventBound;
        this.randomLength = randomLength;


    }

    public void generate(){

        try{
            File output_file = new File(this.queryFile);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            //for each query
            for (int i=0;i<this.num;i++){

                int lastEvent = 0;
                Random rand = new Random();
                int randLength = rand.nextInt(8)+3;    // length vary from 3-10
                int length = randomLength?randLength:this.length;
                ArrayList<Integer> existedEvents = new ArrayList<>();
                // for the length of this query
                for (int l =0;l<length;l++){

                    if (l ==sharedPos){
                        output.append(","+sharedE+"+");
                        lastEvent =sharedE;
                        existedEvents.add(sharedE);
                        if (l ==length-1){
                            output.append("\n");
                        }
                        continue;
                    }

                    Random random = new Random();
                    int randomUnshared = random.nextInt(eventBound)+1;
                    //regenerate an event if it's consecutive| it's shared event| it existed in the query already
                    while(randomUnshared == lastEvent||randomUnshared == sharedE||existedEvents.contains(randomUnshared)){
                        randomUnshared = random.nextInt(eventBound)+1;
                    }
                    existedEvents.add(randomUnshared);
                    lastEvent = randomUnshared;
                    String tmp = l==0?randomUnshared+"":","+randomUnshared;
                    output.append(tmp);
                    if (l ==length-1){
                        output.append("\n");
                    }
                }
            }
            output.close();
        }catch (IOException e) { e.printStackTrace(); }
    }

}
