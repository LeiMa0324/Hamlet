// Old

package executor;

import template.Template;
//import transaction.Aseq;
import transaction.Greta;
//import transaction.Sase;
import transaction.Transaction;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.Stream;
import event.StreamPartitioner;
//import query.Query;

public class NonShared {

	public static void main(String[] args) {
		String file_of_patterns = "";
		String type = "test";
		String inputfile = "stream.txt";
		int events_per_window = Integer.MAX_VALUE;

		// Read input parameters
	    for (int i=0; i<args.length; i++){
			if (args[i].equals("-file")) 		file_of_patterns = args[++i];
			if (args[i].equals("-in"))			inputfile = args[++i];
		}
	    
	    // Build templates
	    ArrayList<Template> templates = new ArrayList<Template>();
	    
		 try {
			 String line;
			 BufferedReader reader = new BufferedReader(new FileReader(file_of_patterns));
			 while ((line = reader.readLine()) != null) {
				 Template T = new Template();
				 T.addPattern(line);
				 templates.add(T);
				 System.out.println(T);
			}
		    reader.close();
		 } catch (IOException e) { e.printStackTrace(); }
		 
		 // MQGRETA (Non-shared)
			/*** Print current time ***/
			Date dNow = new Date( );
		    SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
		    System.out.println("----------------------------------\nCurrent Date: " + ft.format(dNow));    

			/*** SHARED DATA STRUCTURES ***/		
		    CountDownLatch done = new CountDownLatch(1);
			AtomicLong latency = new AtomicLong(0);	
			AtomicInteger memory = new AtomicInteger(0);
			
			try {
				/*** EXECUTORS ***/
				ExecutorService executor = Executors.newFixedThreadPool(3);
				Transaction transaction;
				
				/*** STREAM PARTITIONING ***/
				for (Template T : templates) {
					StreamPartitioner sp = new StreamPartitioner(type, inputfile, events_per_window);
					Stream stream = sp.partition();
					
					transaction = new Greta(stream,T,done,latency,memory);
					executor.execute(transaction);
				}
						
				/*** Wait till all input events are processed and terminate the executor ***/
				done.await();		
				executor.shutdown();			
				System.out.println( "\nLatency: " + latency.get() + "\nMemory: " + memory.get() + "\n");
						
			} catch (InterruptedException e) { e.printStackTrace(); }
	}

}
