package Executor;

import java.util.ArrayList;

/**
 * one experiment is consisted of several executors
 */
public class Main {
    public static void main(String[] args){
        String queryFile ="src/main/resources/Queries/SampleQueries.txt";
        int epw = 50000;

        for (int numofShared = 15; numofShared<41;numofShared+=5){
            System.out.println("number of Bs: "+numofShared);
            String streamFile = String.format("src/main/resources/Streams/GenStream_%d.txt",numofShared);
            String logFile = String.format("throughput_%d.csv",numofShared);
            Executor executor = new Executor(streamFile, queryFile, logFile, epw);
            executor.run();
        }
    }
}
