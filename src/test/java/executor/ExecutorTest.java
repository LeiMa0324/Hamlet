package executor;


import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class ExecutorTest {

    /**
     * the main case for the example in the paper
     */
    @Test
    public void Hamlet_FinalCount_Test(){     //

        String streamFile = "src/main/resources/Paper_TestStream.txt";
        String queryFile = "src/main/resources/Paper_TestQueries.txt";

        int epw = 100;
        Executor executor = new Executor(streamFile, queryFile, epw, true);
        HashMap<Integer, BigInteger> expectedSnapshots = new HashMap<>();
        expectedSnapshots.put(1,new BigInteger("34"));
        expectedSnapshots.put(2,new BigInteger("19"));
        executor.run(false);       //run
        assertEquals(executor.getHamletG().getSnapShot().getCounts(),expectedSnapshots);

        HashMap<Integer, BigInteger> expectedFinalCounts= new HashMap<>();
        expectedFinalCounts.put(1,new BigInteger("132"));

        expectedFinalCounts.put(2,new BigInteger("72"));
        assertEquals(executor.getHamletG().getFinalCount(),expectedFinalCounts);

    }
    /**
     * compare hamlet and greta final count in the paper example
     */
    @Test
    public void Hamlet_Greta_FinalCount_PaperEx_Test(){
        int epw = 100;
//
        //paper exsample
        String paper_streamFile = "src/test/resources/Paper_TestStream.txt";
        String paper_queryFile = "src/test/resources/Paper_TestQueries.txt";
        Executor paper_executor = new Executor(paper_streamFile, paper_queryFile, epw, true);
        paper_executor.run(false);       //run
        System.out.println(paper_executor.getGreta().finalcount);

        assertEquals(paper_executor.getHamletG().getFinalCount(),paper_executor.getGreta().finalcount);
    }

    /**
     * compare hamlet and greta final count on synthetic dataset
     */

    @Test
    public void Hamlet_Greta_FinalCount_SyntheticEx_Test(){
        int epw = 500;
//
        //synthetic example
//        String syn_queryFile = "src/main/resources/Syn_TestQuery.txt";
//        String syn_streamFile = "src/main/resources/Syn_TestStream.txt";

        String syn_queryFile = "src/test/Resources/Syn_TestQuery.txt";
        String syn_streamFile = "src/test/Resources/Syn_TestStream.txt";

        Executor syn_executor = new Executor(syn_streamFile, syn_queryFile, epw, true);
//        syn_executor.run(false);       //run
        syn_executor.run(false);
        System.out.println(syn_executor.getGreta().finalcount);

        assertEquals(syn_executor.getHamletG().getFinalCount(),syn_executor.getGreta().finalcount);
    }

    /**
     * compare hamlet and greta final count on NYC Taxi dataset
     */
    @Test
    public void Hamlet_Greta_FinalCount_NYCEx_Test(){     //

        int epw = 100;
        //nyc example
        String nyc_queryFile = "src/test/resources/NYC_TestQuery.txt";
        String nyc_streamFile = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";


        Executor nyc_executor = new Executor(nyc_streamFile, nyc_queryFile, epw, true);
        nyc_executor.run(false);       //run
        System.out.println(nyc_executor.getGreta().finalcount);

        assertEquals(nyc_executor.getHamletG().getFinalCount(),nyc_executor.getGreta().finalcount);
    }
    /**
     * compare hamlet and greta final count on smart home dataset
     */
    @Test
    public void Hamlet_Greta_FinalCount_SmartHomeEx_Test(){     //

        int epw = 50000;
        //nyc example
//        String nyc_queryFile = "src/main/resources/SmartHome_TestQuery.txt";
//        String nyc_streamFile = "src/main/resources/SmartHome_TestStream.txt";
        String nyc_queryFile = "src/main/resources/SmartHome/Queries/HamletGretaQueries/Workload_size_70_len_3_pos_2.txt";
        String nyc_streamFile = "src/main/resources/SmartHome/Streams/Home_stream.csv";


        Executor nyc_executor = new Executor(nyc_streamFile, nyc_queryFile, epw, true);
        nyc_executor.run(false);       //run
        System.out.println(nyc_executor.getGreta().finalcount);

        assertEquals(nyc_executor.getHamletG().getFinalCount(),nyc_executor.getGreta().finalcount);
    }

//
//    @Test
//    public void Hamlet_Decision_Figure9_Test(){     //
//
//        int epw = 7;
//        int batchsize = 4;
//        int snapshots = 7;
//        String queryFile = "src/main/resources/Decision_TestQuery.txt";
//        String streamFile = "src/main/resources/Decision_TestStream.txt";
////        String streamFile = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
//
//
//        ArrayList<String> queries = new ArrayList<>();
//        //read query file
//        try {
//            Scanner query_scanner = new Scanner(new File(queryFile));
//            while (query_scanner.hasNextLine()) {
//                queries.add(query_scanner.nextLine());
//            }
//            query_scanner.close();
//        } catch(FileNotFoundException e) {e.printStackTrace();}
//
//        Template template = new Template(queries);
//        StaticGraph g = new StaticGraph(template, streamFile, epw,snapshots, batchsize,false);
//
//        System.out.println("Static hamlet running(always share): ");
//
//        long start =  System.currentTimeMillis();
//        g.staticRun();
//        long end =  System.currentTimeMillis();
//
//        System.out.println("static hamlet final count: ");
//        System.out.println(g.getFinalCount());
//        System.out.println("static hamlet latency: "+(end-start));
//
//
//    // no predicates, no extra snapshots
//        DynamicGraph dynamic_g = new DynamicGraph(template, streamFile, epw, snapshots,batchsize,false);
//
//        System.out.println("Dynamic hamlet running: ");
//
//        long start2 =  System.currentTimeMillis();
//        dynamic_g.dynamicRun();
//        long end2 =  System.currentTimeMillis();
//
//        System.out.println("Dynamic hamlet final count: ");
//        System.out.println(dynamic_g.getFinalCount());
//
//    }
//
//    @Test
//    public void Hamlet_Decision_Figure10_Test(){     //
//
//        int epw = 11;
//        int batchsize = 4;
//        int snapshots = 7;
//
//        String queryFile = "src/main/resources/Decision_TestQuery.txt";
//        String streamFile = "src/main/resources/Decision_TestStream.txt";
////        String streamFile = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
//
//
//        ArrayList<String> queries = new ArrayList<>();
//        //read query file
//        try {
//            Scanner query_scanner = new Scanner(new File(queryFile));
//            while (query_scanner.hasNextLine()) {
//                queries.add(query_scanner.nextLine());
//            }
//            query_scanner.close();
//        } catch(FileNotFoundException e) {e.printStackTrace();}
//
//        Template template = new Template(queries);
//        StaticGraph g = new StaticGraph(template, streamFile, epw,snapshots, batchsize,false);
//
//        System.out.println("Static hamlet running(always share): ");
//
//        long start =  System.currentTimeMillis();
//        g.staticRun();
//        long end =  System.currentTimeMillis();
//
//        System.out.println("static hamlet final count: ");
//        System.out.println(g.getFinalCount());
//        System.out.println("static hamlet latency: "+(end-start));
//
//
//        // 1 snapshot one batch
//        DynamicGraph dynamic_g = new DynamicGraph(template, streamFile, epw, snapshots,batchsize,false);
//
//        System.out.println("Dynamic hamlet running: ");
//
//        long start2 =  System.currentTimeMillis();
//        dynamic_g.dynamicRun();
//        long end2 =  System.currentTimeMillis();
//
//        System.out.println("Dynamic hamlet final count: ");
//        System.out.println(dynamic_g.getFinalCount());
//        System.out.println("Dynamic hamlet latency: "+(end2-start2));
//
//
//    }
//
//    @Test
//    public void Hamlet_Decision_Figure10_01_Test(){     //
//
//        int epw = 1000;
//        int batchsize = 10;
//        String queryFile = "src/main/resources/SmartHome/Queries/HAMLET_DEFAULT_WORKLOAD.txt";
//        String streamFile = "src/main/resources/SmartHome/Streams/Home_stream.csv";
////        String streamFile = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";
//
//
//        ArrayList<String> queries = new ArrayList<>();
//        //read query file
//        try {
//            Scanner query_scanner = new Scanner(new File(queryFile));
//            while (query_scanner.hasNextLine()) {
//                queries.add(query_scanner.nextLine());
//            }
//            query_scanner.close();
//
//
//
//        } catch(FileNotFoundException e) {e.printStackTrace();}
//
//
//        int snapshotNum = 500;
//
//        Template template = new Template(queries);
//        StaticGraph g = new StaticGraph(template, streamFile, epw, snapshotNum,batchsize,false);
//
//
//        long start =  System.currentTimeMillis();
//        g.staticRun();
//        long end =  System.currentTimeMillis();
//
////        System.out.println("static hamlet final count: ");
////        System.out.println(g.getFinalCount());
//        System.out.println("static hamlet latency: "+(end-start));
//
//
//        // 1 snapshot one batch
//        DynamicGraph dynamic_g = new DynamicGraph(template, streamFile, epw, snapshotNum,batchsize,false);
//
//        System.out.println("Dynamic hamlet running: ");
//
//        long start2 =  System.currentTimeMillis();
//        dynamic_g.dynamicRun();
//        long end2 =  System.currentTimeMillis();
////
////        System.out.println("Dynamic hamlet final count: ");
////        System.out.println(dynamic_g.getFinalCount());
//        System.out.println("Dynamic hamlet latency: "+(end2-start2));
//
//
//    }
}