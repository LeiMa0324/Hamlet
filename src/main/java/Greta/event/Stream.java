package Greta.event;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Stream {
	
	public HashMap<String,ConcurrentLinkedQueue<Event>> substreams;
					
	public Stream () {		
		substreams = new HashMap<String,ConcurrentLinkedQueue<Event>>();				
	}
	
	public int generateRates(int k) {
		int rate = 0;
		for (String ssId : substreams.keySet()) {
			int count = 0;
			for (Event e : substreams.get(ssId)) {
				if (e.type == k) {
					count++;
				}
			}
			if (rate < count) {
				rate = count;
			}
		}
		return rate;
	}
}
