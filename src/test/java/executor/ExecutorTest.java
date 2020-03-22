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
    public void Example_Test(){     //

        String streamFile = "src/test/resources/SampleStream.txt";
        String queryFile = "src/test/resources/SampleQueries.txt";

        int epw = 500000;
        Executor executor = new Executor(streamFile, queryFile, epw, true);
        HashMap<Integer, BigInteger> expectedSnapshots = new HashMap<>();
        expectedSnapshots.put(1,new BigInteger("34"));
        expectedSnapshots.put(2,new BigInteger("19"));
        executor.run();       //run
//        assertEquals(executor.getHamletG().getSnapShot().getCounts(),expectedSnapshots);
//
//        HashMap<Integer, BigInteger> expectedFinalCounts= new HashMap<>();
//        expectedFinalCounts.put(1,new BigInteger("132"));
//
//        expectedFinalCounts.put(2,new BigInteger("72"));
//        assertEquals(executor.getHamletG().getFinalCount(),expectedFinalCounts);

    }

    @Test
    public void Hamlet_Greta_Sharon_FinalCount_Test(){     //

        String streamFile = "src/test/resources/short_stream_shared_15.txt";
//        String queryFile = "src/test/resources/GenQueries_2.txt";
//        String queryFile = "src/main/resources/Queries/GenQueries_3.txt";
        String queryFile = "src/main/resources/Queries/Workload_size_10_len_3_pos_2.txt";


        int epw = 500000;
        Executor executor = new Executor(streamFile, queryFile, epw, true);
        HashMap<Integer, BigInteger> expectedSnapshots = new HashMap<>();
        executor.run();       //run

        assertEquals(executor.getHamletG().getFinalCount(),executor.getGreta().finalcount);
        //TODO:比较sharon的final count


    }

}