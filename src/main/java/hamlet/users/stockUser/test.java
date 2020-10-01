package hamlet.users.stockUser;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.workload.Workload;
import hamlet.workload.WorkloadAnalyzer;

import java.io.FileNotFoundException;
import java.util.HashMap;

public class test {

    public static void main(String[] args) throws FileNotFoundException {

        String workloadFile = "src/main/resources/Revision/Workload_Shuffled.txt";
        String streamFile = "src/main/resources/Revision/Nasdaq.csv";
        HashMap<String, Workload> miniworkloads = workloadAnalysis(workloadFile);

        for (Workload mini: miniworkloads.values()){
            streamPartitioning(streamFile,mini);

        }


    }


    private static HashMap<String, Workload> workloadAnalysis(String workloadFile){
        DatasetSchema stockSchema = new DatasetSchema();
        stockSchema.addAttribute(new Attribute("tick"));
        stockSchema.addAttribute(new Attribute("high"));
        stockSchema.addAttribute(new Attribute("low"));
        stockSchema.addAttribute(new Attribute("open"));
        stockSchema.addAttribute(new Attribute("close"));
        stockSchema.addAttribute(new Attribute("vol"));

        WorkloadAnalyzer wa = new WorkloadAnalyzer(stockSchema);
        HashMap<String, Workload> miniWorkloads = wa.analyze(workloadFile);
        System.out.printf("Workload Analysis finished!");
        return miniWorkloads;

    }

    private static void streamPartitioning(String streamFile, Workload miniWorkload){

    }
}
