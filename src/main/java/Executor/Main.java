package Executor;

import java.util.ArrayList;

/**
 * one experiment is consisted of several executors
 */
public class Main {
    public static void main(String[] args){
        varyEventsPerWindow();
    }
    //experiment 1:
    // fix epw, vary shared events per graphlet
    public static void varyNumOfSharedEvents(){
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

    //experiment 2:
    // fix #of shared, vary epw
    public static void varyEventsPerWindow(){
        String queryFile ="src/main/resources/Queries/SampleQueries.txt";

        for (int epw = 50000; epw<110000;epw+=10000){
            System.out.println("Evernts per Window: "+epw);
            String streamFile = String.format("src/main/resources/Streams/GenStream_20.txt");
            String logFile = String.format("throughput_epw_%d.csv",epw);
            Executor executor = new Executor(streamFile, queryFile, logFile, epw);
            executor.run();
        }
    }
}
