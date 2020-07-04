package baselines.mcep;

import executor.Executor;
import executor.Experiment;
import hamlet.graph.prefixGraph;
import hamlet.template.Template;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class testMCEP {

    public static void main(String[] args){

        Experiment experiment = new Experiment(0, true, 12, true);

//        experiment.varyEventsPerWindow();
        experiment.varyNumofQueries();
        experiment.varyNumOfSharedEvents();

//        String queryFile = "src/main/resources/Synthetic/Queries/BaselineQueries/Workload_size_15_len_3_pos_2.txt";
//        String streamFile = "src/main/resources/Synthetic/Streams/Stream_shared_10.txt";
//
////
////        String queryFile = "src/main/java/baselines/mcep/Queries.txt";
////        String streamFile = "src/main/java/baselines/mcep/Stream.txt";
//
//
//        int epw =5000;
//
//        ArrayList<String> queries = new ArrayList<>();
//
//        //read query file
//        try {
//            Scanner query_scanner = new Scanner(new File(queryFile));
//            while (query_scanner.hasNextLine()) {
//                queries.add(query_scanner.nextLine());
//            }
//            query_scanner.close();
//        } catch(FileNotFoundException e) {e.printStackTrace();}
//
////
////        //Hamlet
//        Template template = new Template(queries);
//        McepGraph mcepGraph = new McepGraph(template, streamFile, epw);
//        long start1 =  System.currentTimeMillis();
//        mcepGraph.run();
//        long end1 =  System.currentTimeMillis();
//        System.out.println("MCEP latency: "+(end1-start1));
//
//
//        Executor executor = new Executor(streamFile,queryFile,epw,true);
//        executor.gretaRun();
//        System.out.println(executor.getGreta().finalcount);
//
//        prefixGraph newHamlet = new prefixGraph(template, streamFile,  epw,  false);
//        long start =  System.currentTimeMillis();
//
//        newHamlet.run();
//        long end =  System.currentTimeMillis();
//        System.out.println(newHamlet.getFinalCount());
//        System.out.println("new Hamlet latency: "+(end-start));

    }
}
