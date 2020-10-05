package hamlet.users.stockUser;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.stream.streamLoader;
import hamlet.stream.streamPartitioner;
import hamlet.workload.Workload;
import hamlet.workload.WorkloadAnalyzer;
import hamlet.workload.WorkloadTemplate;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;

public class test {

    private static DatasetSchema stockSchema;
    private static Workload stockWorkload;
    private static HashMap<String, Workload> stockMiniWorkloads;

    public static void main(String[] args) throws FileNotFoundException {

        String workloadFile = "src/main/resources/Revision/Workload_Original.txt";
        String streamFile = "src/main/resources/Revision/Nasdaq.csv";

//        /**
//         * generate the workload
//         */
//        stockWorkloadGeneration(workloadFile);

        /**
         * maintain the dataset schema
         */
        stockSchema = new DatasetSchema();
        for (stockAttributeEnum a: stockAttributeEnum.values()){
            stockSchema.addAttribute(new Attribute(a.toString()));
        }

        /**
         * read and analyze workload
         */
        workloadAnalysis(workloadFile);

        /**
         * stream Partition for each mini-workload
         */
        streamPartitioning(streamFile);

        /**
         * run for each pair of <mini-workload, sub stream>
         */

    }


    private static void workloadAnalysis(String workloadFile){

        WorkloadAnalyzer wa = new WorkloadAnalyzer(stockSchema);
        stockWorkload = wa.analyzeToWorkload(workloadFile);
        stockMiniWorkloads = wa.analyzeToMiniWorkloads(workloadFile);
        System.out.printf("Workload Analysis finished!");

    }

    private static void streamPartitioning(String streamFile){
        ArrayList<Event> events = new streamLoader(streamFile, stockSchema, stockWorkload).stream();
        HashMap<Workload, HashMap<Object, ArrayList<Event>>> substreams = new HashMap<>();
        for (Workload miniWorkload: stockMiniWorkloads.values()){
            streamPartitioner sp = new streamPartitioner(miniWorkload, events);
            HashMap<Object, ArrayList<Event>> subStreamsForOneMiniWorkload = sp.partition();
            substreams.put(miniWorkload, subStreamsForOneMiniWorkload);
        }

        System.out.printf(substreams.toString());


    }

    private static void stockWorkloadGeneration(String workloadFile){
        WorkloadTemplate template = new WorkloadTemplate();
        template.generate(50, 5);
        template.toFile(workloadFile);

    }
}
