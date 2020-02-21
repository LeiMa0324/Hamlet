package event;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamPartitioner {	
	
	String type;
	String filename;
	int events_per_window;
			
	public StreamPartitioner (String ty, String f, int epw) {		
		type = ty;
		filename = f;
		events_per_window = epw;
	}

	/*** Read the input file, parse the events, and partition the events into sub-streams ***/
	public Stream partition() {	
		Stream stream = new Stream();
		try {			
			// Input file
			Scanner scanner = new Scanner(new File(filename));
			// Event number	
	 		int event_number = 1;
			// First event
			String line = scanner.nextLine();
	 		Event event = Event.parse(event_number,line,type);
 			
 			while (event != null && event_number <= events_per_window) {	 	
	 			
	 			/*** Put the event into its sub-stream and increment the counter ***/						
	 			if (event.isRelevant()) {
	 				String substream_id = event.getSubstreamid();
	 				ConcurrentLinkedQueue<Event> substream = (stream.substreams.containsKey(substream_id)) ? 
	 						stream.substreams.get(substream_id) : 
	 						new ConcurrentLinkedQueue<Event>();
					substream.add(event);
					stream.substreams.put(substream_id,substream);
	 				event_number++;
	 			}
	 					
	 			/*** Reset event ***/
	 			if (scanner.hasNextLine()) {		 				
	 				line = scanner.nextLine();   
	 				event = Event.parse(event_number,line,type);		 				
	 			} else {
	 				event = null;		 				
	 			}	 			
	 		}		 			
	 		/*** Clean-up ***/		
			scanner.close();				
//			System.out.println("Stream partitioner created " + stream.substreams.size() + " substreams.");	
 		
		} catch (FileNotFoundException e) { e.printStackTrace(); }
		return stream;
	}	
}
