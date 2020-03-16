package Executor;

import java.util.ArrayList;

/**
 * Each static method is one experiment
 */
public class Main {
    public static void main(String[] args){
        varyEventsPerWindow();
    }

    /**
     * Experiment1: fix epw, vary shared events per graphlet
     */

    public static void varyNumOfSharedEvents(){
        String queryFile ="src/main/resources/Queries/SampleQueries.txt";
        int epw = 50000;

        for (int numofShared = 15; numofShared<41;numofShared+=5){
            System.out.println("====================number of Bs: "+numofShared+"====================");
            String streamFile = String.format("src/main/resources/Streams/GenStream_%d.txt",numofShared);
            String thruFile = String.format("throughput_%d.csv",numofShared);
            String latFile = String.format("latency_%d.csv",numofShared);
            String memFile = String.format("memory_%d.csv",numofShared);

            Executor executor = new Executor(streamFile, queryFile, epw, thruFile, latFile, memFile);
            executor.run();
        }
    }

    /**
     * Experiment2: fix # of shared events, vary epw
     */
    public static void varyEventsPerWindow(){
        String queryFile ="src/main/resources/Queries/GenQueries_3.txt";
        System.out.println("#of shared events per graphlet: 20");
        for (int epw = 50000; epw<110000;epw+=10000){

            System.out.println("====================Evernts per Window: "+epw+"====================");
            String streamFile = "src/main/resources/Streams/GenStream_20.txt";
            String thruFile = "throughput_epw.csv";
            String latFile = "latency_epw.csv";
            String memFile = "memory_epw.csv";
            Executor executor = new Executor(streamFile, queryFile, epw, thruFile, latFile, memFile);
            executor.run();

        }
    }
}
