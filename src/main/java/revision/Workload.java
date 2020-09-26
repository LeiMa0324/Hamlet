package revision;

import lombok.Data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Data
public class Workload {

    //a list of queries
    private ArrayList<Query> queries = new ArrayList<>();

    //predicate for the mini-workload
    private int tripPred = 0;

    /**
     * generate a workload by itself
     * @param queryNum the number of queries in the workload
     * @param miniWorkloads the mini-workloads
     */
    public Workload(int queryNum, int miniWorkloads){

        ArrayList<Integer> usedET = new ArrayList<>();

        //the number of queries in each groups
        int queriesPerGroup = queryNum/miniWorkloads;
        List<String> aggregFuncs = Arrays.asList("SUM", "COUNT");


        //for each query group, the queries share the same setting
        for (int i =0; i<miniWorkloads; i++){

            //set the shared event type
            Random rand = new Random();
            int sharedET = rand.nextInt(10)+1;    // shared event type in random(1-10)

            while (usedET.contains(sharedET)){
                sharedET = rand.nextInt(10)+1;

            }
            usedET.add(sharedET);

            //set the predicate on trip_distance>0

            int predicate = 0;

            //set the aggregation function
            int index = (int) (Math.random()* aggregFuncs.size());
            String aggreg = aggregFuncs.get(index);

            for (int j=0;j<queriesPerGroup;j++){
               Query query = new Query(sharedET, predicate, aggreg);
               queries.add(query);
            }
        }

    }

    /**
     * take in a list of queries to construct a workload
     * @param queries
     */
    public Workload(ArrayList<Query> queries){
        this.queries = queries;

    }

    public void addQuery(Query q){
        this.queries.add(q);
        tripPred = q.getPredicate();
    }

    /**
     * write the workload into file
     * @param workloadFile the output file
     */
    public void toFile(String workloadFile){

        try{
            File output_file = new File(workloadFile);
            if (!output_file.exists()){
                output_file.createNewFile();
            }
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            int i = 0;
            for(Query q: this.queries){
                output.append("q"+i+"\n");
                output.append(q.toString()+"\n\n");
                i++;
            }
            output.close();
        }catch (IOException e) { e.printStackTrace(); }


    }
}
