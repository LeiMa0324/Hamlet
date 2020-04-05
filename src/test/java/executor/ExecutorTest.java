package executor;


import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;


import static org.junit.Assert.assertEquals;

public class ExecutorTest {

    /**
     * the test case for the example in the paper
     */
    @Test
    public void Hamlet_FinalCount_Test(){     //

        String streamFile = "src/test/resources/PaperExample_Stream.txt";
        String queryFile = "src/test/resources/PaperExample_Queries.txt";

        int epw = 500000;
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


    @Test
    public void Hamlet_Greta_FinalCount_Test(){     //
        int epw = 10000;

        //paper exsample
        String paper_streamFile = "src/test/resources/PaperExample_Stream.txt";
        String paper_queryFile = "src/test/resources/PaperExample_Queries.txt";
        Executor paper_executor = new Executor(paper_streamFile, paper_queryFile, epw, true);
        paper_executor.run(false);       //run
        System.out.println(paper_executor.getGreta().finalcount);

        assertEquals(paper_executor.getHamletG().getFinalCount(),paper_executor.getGreta().finalcount);

        //synthetic example
        String syn_queryFile = "src/test/resources/Syn_ExampleQuery.txt";
        String syn_streamFile = "src/test/resources/Syn_ExampleStream.txt";

        Executor syn_executor = new Executor(syn_streamFile, syn_queryFile, epw, true);
        syn_executor.run(false);       //run
        System.out.println(syn_executor.getGreta().finalcount);

        assertEquals(syn_executor.getHamletG().getFinalCount(),syn_executor.getGreta().finalcount);

        //nyc example
        String nyc_queryFile = "src/test/resources/NYC_ExampleQuery.txt";
        String nyc_streamFile = "src/main/resources/NYCTaxi/Streams/Taxi_stream.csv";


        Executor nyc_executor = new Executor(nyc_streamFile, nyc_queryFile, epw, true);
        nyc_executor.run(false);       //run
        System.out.println(nyc_executor.getGreta().finalcount);

        assertEquals(nyc_executor.getHamletG().getFinalCount(),nyc_executor.getGreta().finalcount);


    }

}