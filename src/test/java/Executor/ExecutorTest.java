package Executor;


import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.logging.Level;


import static org.junit.Assert.assertEquals;

public class ExecutorTest {

    /**
     * the test case for the example in the paper
     */
    @Test
    public void Example_Test(){     //
        String streamFile = "src/main/resources/Streams/SampleStream.txt";
        String queryFile = "src/main/resources/Queries/SampleQueries.txt";
        String thruFile = "test_throughput.csv";
        String latFile = "test_latency.csv";
        String memFile = "test_memory.csv";
        int epw = 500000;
        Executor executor = new Executor(streamFile, queryFile, epw, thruFile, latFile, memFile, true);
        HashMap<Integer, BigInteger> expectedSnapshots = new HashMap<>();
        expectedSnapshots.put(1,new BigInteger("34"));
        expectedSnapshots.put(2,new BigInteger("19"));
        executor.run();       //run
        assertEquals(executor.getHamletG().getSnapShot().getCounts(),expectedSnapshots);

        HashMap<Integer, BigInteger> expectedFinalCounts= new HashMap<>();
        expectedFinalCounts.put(1,new BigInteger("132"));

        expectedFinalCounts.put(2,new BigInteger("72"));
        assertEquals(executor.getHamletG().getFinalCount(),expectedFinalCounts);

    }

}