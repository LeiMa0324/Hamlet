package generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import event.*;

public class InputFileGenerator {
	
	// -type transport -path ../../../transport/ -o_file transport-30.txt -groups 30 -sec 1000000
	// -type stock -path ../../../Dropbox/DataSets/Stock/ -i_file_1 replicated.txt -o_file sorted.txt -real 1
	// -type cluster -sec 1000 -lambda 20 -path ../../../Dropbox/DataSets/Cluster/ -o_file cluster-10.txt -groups 10 
	// -type position -path ../../../Dropbox/DataSets/LR/InAndOutput/1xway/ -i_file_1 0;2.dat -o_file position.dat
	
	public static void main (String[] args) {
		
		Path currentRelativePath = Paths.get("");
	    String s = currentRelativePath.toAbsolutePath().toString();
	    System.out.println("Current relative path is: " + s); 
	    		
		/*** Set default input parameters ***/
		String type = "";
		boolean real = false;
		String path = "";
		String i_file_1 = "";
//		String i_file_2 = "";
		String o_file = "";
		
		int last_sec = 0;
		int total_rate = 0;
		int matched_rate = 0;
		int lambda = 0;
		int number_of_groups = 0;
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-type")) 		type = args[++i];
			if (args[i].equals("-real")) 		real = Integer.parseInt(args[++i])==1;
			if (args[i].equals("-path")) 		path = args[++i];
			if (args[i].equals("-i_file_1")) 	i_file_1 = args[++i];
//			if (args[i].equals("-i_file_2")) 	i_file_2 = args[++i];
			if (args[i].equals("-o_file")) 		o_file = args[++i];
			if (args[i].equals("-sec")) 		last_sec = Integer.parseInt(args[++i]);
			if (args[i].equals("-trate"))   	total_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-mrate")) 		matched_rate = Integer.parseInt(args[++i]);
			if (args[i].equals("-lambda")) 		lambda = Integer.parseInt(args[++i]);
			if (args[i].equals("-groups")) 		number_of_groups = Integer.parseInt(args[++i]);
		}
				
		/*** Generate input event stream ***/
		try {		
			// Open the output file
			String output_file_name = path + o_file; 
			File output_file = new File(output_file_name);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			int id = 1;
			
			// Generate input event stream
			if (type.equals("transport")) {	
				
				Random random = new Random();
				int min = 0;
	        	int max = 10;
				for (int sec=1; sec<=last_sec; sec++) {					
		        	int rate = random.nextInt((max - min) + 1) + min;
		        	for (int count=1; count<=rate; count++) {			
						generate_transport_stream (output, sec, id++, number_of_groups);
					}
				}
			} else {
			if (type.equals("stock")) {		
				if (real) {
					String input_1 = path + i_file_1;
					//readInReverseOrder(input_1, output);
					//String input_2 = path + i_file_2;
					//twoInputsOneOutput (input_1, input_2, output);
					//transform(input_1,output);
					sort(input_1,output);
					//replicate(input_1,output);
				} else {					
					for (int sec=1; sec<=last_sec; sec++) 
						for (int count=1; count<=total_rate; count++) 
							generate_stock_stream(output, id++, sec, count, matched_rate, lambda);	
				}
			} else { 
			if (type.equals("cluster")) {	
				
				Random random = new Random();
				int min = 0;
	        	int max = 10;
				for (int sec=1; sec<=last_sec; sec++) {					
		        	int rate = random.nextInt((max - min) + 1) + min;
		        	for (int count=1; count<=rate; count++) {			
						generate_cluster_stream (output, sec, id++, lambda, number_of_groups);
					}
				}
			} else { // position
				String input = path + i_file_1;
				create_id(input,output);
			}}}
			// Close the file
			output.close();
			System.out.println("Done!");
			
		} catch (IOException e) { e.printStackTrace(); }	
	}
	
	public static void create_id (String input_file_name, BufferedWriter output) {
		
		File input_file = new File(input_file_name);
		Scanner input;
		try { 
			input = new Scanner(input_file); 
			
			String eventString = input.nextLine();
			PositionReport event = PositionReport.parse(eventString);			
			int id = 1;
				
			while (event != null) {
			
				try { 
					// Write event
					event.id = id++;
					output.write(event.toFile()); 
									
				} catch (IOException e) { e.printStackTrace(); }
		
				// Reset event
				if (input.hasNextLine()) {
					eventString = input.nextLine();
					event = PositionReport.parse(eventString);
				} else {
					event = null;
				}			
			}
		} catch (FileNotFoundException e1) { e1.printStackTrace(); }
	}
	
	/*** SYNTHETIC PUBLIC TRANSPORT ***/
	public static void generate_transport_stream (BufferedWriter output, int sec, int id, int number_of_groups) {
		
		Random random = new Random();
		        
		// Passenger identifier and station identifier are random values in a range between min and max
        int min = 1;
        int max = number_of_groups;
        int passenger = random.nextInt((max - min) + 1) + min;
        
        int min1 = 1;
        int max1 = 100;
        int station = random.nextInt((max1 - min1) + 1) + min1;
        
        // Event type is check in (1), waiting (2), travel (3), and check out (4)              
        int min2 = 1;
        int max2 = 4;
        int type = random.nextInt((max2 - min2) + 1) + min2;
        
        // Duration is a random variable between 1 and 3600        
        int min3 = 1;
        int max3 = 3600;
        int duration = random.nextInt((max3 - min3) + 1) + min3;        
        
        // Save this event in the file
        String event = id + "," + sec + "," + passenger + "," + type + "," + station + "," + duration + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        //System.out.println("id " + id + " sec " + sec + " passenger " + passenger + " type " + type + " station " + station + " duration " + duration);
	}
	
	/*** SYNTHETIC CLUSTER ***/
	public static void generate_cluster_stream (BufferedWriter output, int sec, int id, double lambda, int number_of_groups) {
		
		Random random = new Random();
		        
		// Mapper identifier, job identifier, cpu and memory measurements are random values in a range between min and max
        int min = 1;
        int max = number_of_groups;
        int mapper = random.nextInt((max - min) + 1) + min;
        int job = random.nextInt((max - min) + 1) + min;       
        
        int min2 = 1;
        int max2 = 1000;
        int cpu = random.nextInt((max2 - min2) + 1) + min2;
        int memory = random.nextInt((max2 - min2) + 1) + min2;
        
        // Poisson distribution of load
        double limit = Math.exp(-lambda);
		double prod = random.nextDouble();
        int load;
        for (load = 0; prod >= limit; load++)
            prod *= random.nextDouble();
        
        // Save this event in the file
        String event = id + "," + sec + "," + mapper + "," + job + "," + cpu + "," + memory + "," + load + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        //System.out.println("id " + id + "sec " + sec + " mapper " + mapper + " job " + job + " cpu " + cpu + " mem " + memory + " load " + load);
	}
	
	/*** SYNTHETIC STOCK ***/
	public static void generate_stock_stream (BufferedWriter output, int id, int sec, int count, int matched_rate, double lambda) {
		
		Random random = new Random();
		        
        // Sector identifier determines event relevance: 1 for relevant, 0 for irrelevant
        int sector = (count <= matched_rate) ? 1 : 0;
        
        // Company identifier is a random value in a range between min and max
        int min = 1;
        int max = 3;
        int company = random.nextInt((max - min) + 1) + min;        
        
        // Poisson distribution of price
        double limit = Math.exp(-lambda);
		double prod = random.nextDouble();
        int price;
        for (price = 0; prod >= limit; price++)
            prod *= random.nextDouble();
        
        // Save this event in the file
        String event = id + "," + sec + "," + sector + "," + company + "," + price + "\n";
        try { output.append(event); } catch (IOException e) { e.printStackTrace(); }
        System.out.println("id " + id + " sec " + sec + " sector " + sector + " company " + company + " price " + price);
	}
	
	/*** REAL STOCK ***/
	public static void readInReverseOrder(String input_file_name, BufferedWriter output) throws IOException {

		//String input = "../../stock_data/TimeAndSales_AMAT_210019836.txt";
	    BufferedReader br = null;

	    try {
	        br = new BufferedReader(new FileReader(input_file_name));
	        Stack<String> lines = new Stack<String>();
	        String line = br.readLine();
	        
	        while(line != null) {
	            lines.push(line);
	            line = br.readLine();
	        }

	        while(!lines.empty()) 
	        	 output.append(lines.pop() + "\n");	        

	    } finally {
	        if(br != null) {
	            try { br.close(); } catch(IOException e) { }
	        }
	    }
	}
	
	/***
	 * Open 2 input files and 1 output file, call the method and close all files.
	 * @param first input file
	 * @param second input file
	 * @param output file
	 */
	public static void twoInputsOneOutput (String inputfilename1, String inputfilename2, BufferedWriter output) {
		
		Scanner input1 = null;
		Scanner input2 = null;
		try {		
			/*** Input file ***/
			File input_file_1 = new File(inputfilename1);
			File input_file_2 = new File(inputfilename2);
			input1 = new Scanner(input_file_1);  			
			input2 = new Scanner(input_file_2);
					
			/*** Call the method ***/            
            merge(input1,input2,output);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Merges 2 sorted files input1 and input2 into one sorted file output. The files are sorted by time stamps. 
	 * @param input1
	 * @param input2
	 * @param output
	 */
	public static void merge (Scanner input1, Scanner input2, BufferedWriter output) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		StockEvent event1 = StockEvent.parse2(eventString1);
		StockEvent event2 = StockEvent.parse2(eventString2);
		int count = 0; 
		
		try {
			
			while (event1 != null && event2 != null) {
					
				if (event1.sec < event2.sec) {				
					
					// Write event1
					output.write(eventString1 + "\n");
						
					// Reset event1
					if (input1.hasNextLine()) {
						eventString1 = input1.nextLine();
						event1 = StockEvent.parse2(eventString1);
					} else {
						event1 = null;
					}
				} else {				
					
					// Write event2
					output.write(eventString2 + "\n");
				
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = StockEvent.parse2(eventString2);
					} else {
						event2 = null;
					}
				}
				count++;
			}
			if (event1 == null) {
				while (event2 != null) {
					// Write event2
					output.write(eventString2 + "\n");
				
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = StockEvent.parse2(eventString2);
					} else {
						event2 = null;
					}
					count++;
				}				
			}
			if (event2 == null) {
				while (event1 != null) {
					// Write event1
					output.write(eventString1 + "\n");
				
					// Reset event1
					if (input1.hasNextLine()) {
						eventString1 = input1.nextLine();
						event1 = StockEvent.parse2(eventString1);
					} else {
						event1 = null;
					}
					count++;
				}
			}				
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count);
	}
	
	/*** Sort by time stamp ***/
	public static void sort (String inputfilename, BufferedWriter output) {
		
		File input_file = new File(inputfilename);
		Scanner input;
		try { 
			input = new Scanner(input_file); 
			
			String eventString = input.nextLine();
			StockEvent event = StockEvent.parse(eventString);
			
			HashMap<Integer,ArrayList<StockEvent>> events_per_second = new HashMap<Integer,ArrayList<StockEvent>>();
				
			while (event != null) {
				
				// Safe this event in the array of events for event.sec
				if (!events_per_second.containsKey(event.sec)) {
					events_per_second.put(event.sec, new ArrayList<StockEvent>());
				}
				ArrayList<StockEvent> events = events_per_second.get(event.sec);
				events.add(event);			
				
				// Reset event
				if (input.hasNextLine()) {
					eventString = input.nextLine();
					event = StockEvent.parse(eventString);
				} else {
					event = null;
				}			
			}		
			// Write events in order
			Set<Integer> keys = events_per_second.keySet();
			List<Integer> sortedList = new ArrayList<Integer>(keys);
			Collections.sort(sortedList);
			int count = 1;
			for (int i = 0; i<sortedList.size(); i++) {
				Integer second = (Integer) sortedList.get(i);
				ArrayList<StockEvent> events = events_per_second.get(second);
				for (StockEvent e : events) {
					output.write(e.toFile(count++)); 
				}
			}	
		} catch (IOException e) { e.printStackTrace(); }

	}
	
	/*** Add sector, transform the time stamp to second, separate attribute values by commas ***/
	public static void transform (String inputfilename, BufferedWriter output) {
		
		File input_file = new File(inputfilename);
		Scanner input;
		try { 
			input = new Scanner(input_file); 
			
			String eventString = input.nextLine();
			StockEvent event = StockEvent.parse2(eventString);
				
			while (event != null) {
			
				// Write event
				try { output.write(event.toFile()); } catch (IOException e) { e.printStackTrace(); }
		
				// Reset event
				if (input.hasNextLine()) {
					eventString = input.nextLine();
					event = StockEvent.parse2(eventString);
				} else {
					event = null;
				}			
			}
		} catch (FileNotFoundException e1) { e1.printStackTrace(); }
	}
	
	/*** Replicate each event 10 times, change company name and sector, replace identifier ***/
	public static void replicate (String inputfilename, BufferedWriter output) {
		
		File input_file = new File(inputfilename);
		Scanner input;
		try { 
			input = new Scanner(input_file); 
			
			String eventString = input.nextLine();
			StockEvent event = StockEvent.parse(eventString);			
			int id = 1;
				
			while (event != null) {
			
				try { 
					// Write event
					event.id = id++;
					output.write(event.toFile()); 
					// Replicate event
					for (int count=1; count<=10; count++) {
						event.id = id++;
						event.company = count + "" + count + "" + count;
						event.sector = count;
						output.write(event.toFile()); 
					}				
				} catch (IOException e) { e.printStackTrace(); }
		
				// Reset event
				if (input.hasNextLine()) {
					eventString = input.nextLine();
					event = StockEvent.parse(eventString);
				} else {
					event = null;
				}			
			}
		} catch (FileNotFoundException e1) { e1.printStackTrace(); }
	}
	
}		