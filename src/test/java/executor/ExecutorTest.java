package executor;


import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class ExecutorTest {
    @Test
    public void Example_Test(){     //
        String streamFile = "src/main/resources/Streams/SampleStream.txt";
        String queryFile = "src/main/resources/Queries/SampleQueries.txt";
        String logFile = "throughput.csv";
        Executor executor = new Executor(streamFile, queryFile, logFile);
        HashMap<Integer, BigInteger> expectedSnapshots = new HashMap<>();
        expectedSnapshots.put(1,new BigInteger("34"));
        expectedSnapshots.put(2,new BigInteger("19"));
        assertEquals(executor.getG().getSnapShot().getCounts(),expectedSnapshots);

        HashMap<Integer, BigInteger> expectedFinalCounts= new HashMap<>();
        expectedFinalCounts.put(1,new BigInteger("132"));
        expectedFinalCounts.put(2,new BigInteger("72"));
        assertEquals(executor.getG().getFinalCount(),expectedFinalCounts);


    }

}