package executor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;

import template.SingleQueryTemplate;
import event.*;
import transaction.*;
 
public class Main {
	
	/**
	 * Create and call the chain: Input file -> Driver -> Scheduler -> Executor -> Output files 
	 * -type stock -path ../../../Dropbox/DataSets/Stock/ -file sorted.txt -pred none -epw 500 -ess any -algo sase
	 * -type activity -path ../../../Dropbox/DataSets/PhysicalActivity/ -file 114.dat -pred none -epw 100 -ess cont -algo sase
	 * -type transport -path ../../../Dropbox/DataSets/PublicTransport/ -file transport.txt -pred none -epw 100 -ess next -algo sase
	 * 
	 * -type cluster -path ../../../Dropbox/DataSets/Cluster/ -file cluster.txt -pred 50% -epw 443947 -algo greta	 
	 * -type position -path ../../../Dropbox/DataSets/LR/InAndOutput/1xway/ -file position.dat -pred 50% -epw 1000 -algo greta
	 * -type stock -path src/iofiles/ -file stream.txt -pred 100% -epw 10 -algo hcet -graphlets 2
	 * 
	 * -type test -path ../../Documents/datasets/muse/ -file stream1200k_uniform.txt -queries S_20l_10k.txt -algo muse -epw 32000 -isPrefix -1
	 */
	public static void main (String[] args) { 
		
		try {
		
		/*** Print current time ***/
		Date dNow = new Date( );
	    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));
	    
	    /* Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s); */
	    
	    /*** Input and output ***/
	    // Set default values
	    String type = "stock";
	    String path = "src/iofiles/";
		String inputfile = "stream.txt";
		String queryfile = "queries.txt";
		String algorithm = "greta"; // muse, lazy
		String predicate = "none";
		String isPrefix = "0";
		int events_per_window = Integer.MAX_VALUE;
				
		// Read input parameters
	    for (int i=0; i<args.length; i++){
	    	if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-file")) 		inputfile = args[++i];
			if (args[i].equals("-queries")) 	queryfile = args[++i];
			if (args[i].equals("-algo")) 		algorithm = args[++i];
			if (args[i].equals("-pred")) 		predicate = args[++i];
			if (args[i].equals("-isPrefix"))	isPrefix = args[++i]; // 0 for nonshared; -1 for completely shared; otherwise use suffix length
			if (args[i].equals("-epw")) 		events_per_window = Integer.parseInt(args[++i]);
		}
	    	   	    
	    // Print input parameters
	    String input = path + inputfile;
	    System.out.println(	"Event type: " + type +
	    					"\nQuery file: " + queryfile +
	    					"\nAlgorithm: " + algorithm +
	    					"\nPredicate: " + predicate +
//	    					"\nGuaranteed Shared: " + isPrefix +
	    					"\nEvents per window: " + events_per_window +
							"\n----------------------------------");	    

		/*** SHARED DATA STRUCTURES ***/		
	    CountDownLatch done = new CountDownLatch(1);
		AtomicLong latency = new AtomicLong(0);	
		AtomicInteger memory = new AtomicInteger(0);
		
		/*** STREAM PARTITIONING ***/
		StreamPartitioner sp = new StreamPartitioner(type, input, events_per_window);
		
		/*** PARSE QUERIES ***/
		ArrayList<String> queries = new ArrayList<String>();
		try {
	    	Scanner query_scanner = new Scanner(new File(path+queryfile));
	    	while (query_scanner.hasNextLine()) {
				String query = query_scanner.nextLine();
	    		queries.add(query);	    		
	    	}
	    	query_scanner.close();
		} catch(FileNotFoundException e) {e.printStackTrace();}
		
		/*** EXECUTORS ***/
		ExecutorService executor = Executors.newFixedThreadPool(1);
//		ExecutorService executor = Executors.newSingleThreadExecutor();
		TransactionMQ TrS;
		
		if (algorithm.equals("prefix")) {
			SingleQueryTemplate prefixT = new SingleQueryTemplate(queries.get(0));
			TrS = new PrefixMQ(done, latency, prefixT, sp);
			for (int i=1; i<queries.size(); i++) {
				((PrefixMQ)TrS).addQuery(new SingleQueryTemplate(queries.get(i)));
			}
		}
		else if (algorithm.equals("greta")) {
			TrS = new GretaMQ(done,latency);
			for (String P : queries) {
				SingleQueryTemplate query = new SingleQueryTemplate(P);
				Stream stream = sp.partition();
				((GretaMQ)TrS).addQuery(new Greta(stream,query,done,latency,memory,false));
			}
		} else if (algorithm.equals("muse")) {
			Stream stream = sp.partition();
			if (isPrefix.equals("0")) {
				TrS = new MuseMQ(stream, done, latency, queries, false);
			} else if (isPrefix.equals("-1")){
				TrS = new MuseMQ(stream, done, latency, queries, true);
			} else {
				int suffLen = Integer.parseInt(isPrefix);
				TrS = new MuseMQ(stream, done, latency, queries, suffLen);
			}
		} else {
			TrS = new ReverseMQ(done,latency);
			for (String P : queries) {
				SingleQueryTemplate query = new SingleQueryTemplate(P);
				Stream stream = sp.partition();
				((ReverseMQ)TrS).addQuery(new Reverse(stream,query,done,latency,memory));
			}
		}
		
//		executor.execute(TrS);
//		TrS.run();
				
		/*** Wait till all input events are processed and terminate the executor ***/
		done.await();		
		executor.shutdown();			
		System.out.println( "\nLatency: " + latency.get() + "\nMemory: " + memory.get() + "\n");
				
		} catch (InterruptedException e) { e.printStackTrace(); }
	}		
}