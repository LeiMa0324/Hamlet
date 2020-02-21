package transaction;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import event.*;

public class Aseq extends Transaction {
	
	public Aseq (Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		super(str,d,time,mem);
	}
	
	public void run () {
		
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		latency.set(latency.get() + duration);			
		done.countDown();
	}
	
	public void computeResults () {
		
		Set<String> substream_ids = stream.substreams.keySet();
		
		for (String substream_id : substream_ids) {					
		 
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			BigInteger count = computeResults(events);
			System.out.println("Sub-stream id: " + substream_id + " with count " + count);
		}		
	}
	
	public BigInteger computeResults (ConcurrentLinkedQueue<Event> events) {
		
		// Prefix counters per prefix length 
		HashMap<Integer,BigInteger> prefix_counters_in_previous_second = new HashMap<Integer,BigInteger>();
		HashMap<Integer,BigInteger> prefix_counters_in_current_second = new HashMap<Integer,BigInteger>();		
		int curr_sec = -1;
		int curr_length = 0;
		BigInteger count_per_substream = new BigInteger("0");
		
		Event event = events.peek();
		
		while (event != null) {
			
			event = events.poll();
			
			// Update current second, current length, and prefix counters
			if (curr_sec < event.sec) {
				
				curr_sec = event.sec;
				curr_length++;
				prefix_counters_in_current_second.put(curr_length,new BigInteger("0"));
				
				// Prefix counters in current second become prefix counters in previous second
				for (int length=1; length<=curr_length; length++) 
					prefix_counters_in_previous_second.put(length,prefix_counters_in_current_second.get(length));				
			} 
			// Each event updates prefix counters for each length and the final count
			count_per_substream = new BigInteger("0");
			for (int length=1; length<=curr_length; length++) {
				
				BigInteger count_of_new_matches = (length-1<=0) ? new BigInteger("1") : prefix_counters_in_previous_second.get(length-1);				
				BigInteger count_of_old_matches = prefix_counters_in_current_second.get(length);				
				BigInteger new_count_for_length = count_of_new_matches.add(count_of_old_matches);
				prefix_counters_in_current_second.put(length,new_count_for_length);	
				count_per_substream = count_per_substream.add(new_count_for_length);
				//System.out.println("Event " + event.id + " length: " + length + " counts: " + count_of_new_matches + " " + count_of_old_matches );	
			}
			event = events.peek();
		}
		memory.set(memory.get() + prefix_counters_in_previous_second.size() + prefix_counters_in_current_second.size());		
		return count_per_substream;
	}
}
