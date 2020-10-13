package hamlet.users.stockUser;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.executor.Executor;

import java.io.FileNotFoundException;

public class main {

    public static void main(String[] args) throws FileNotFoundException {

        String workloadFile = "src/main/resources/Revision/Workload_Original.txt";
        String streamFile = "src/main/resources/Revision/Nasdaq.csv";



        /**
         * maintain the dataset schema
         */
        DatasetSchema stockSchema = new DatasetSchema();
        for (stockAttributeEnum a: stockAttributeEnum.values()){
            stockSchema.addAttribute(new Attribute(a.toString()));
        }

        Executor executor = new Executor(stockSchema);

        //        /**
//         * generate the workload
//         */
//        executor.stockWorkloadGeneration(workloadFile);

        /**
         * read and analyze workload
         */
        executor.workloadAnalysis(workloadFile);

        /**
         * stream Partition for each mini-workload
         */
        executor.streamPartitioning(streamFile);

        /**
         * run for each pair of <mini-workload, sub stream>
         */
        executor.singleRun();


    }
}
