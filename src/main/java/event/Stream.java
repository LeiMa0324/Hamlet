package event;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Stream {
	
	public HashMap<String,ConcurrentLinkedQueue<Event>> substreams;
					
	public Stream () {		
		substreams = new HashMap<String,ConcurrentLinkedQueue<Event>>();				
	}
}
