package transaction;

import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import event.*;
import graph.*;
import template.*;

public class Reverse extends Transaction {
	
	SingleQueryTemplate query;
	Template template;
	boolean MQ = false;
		
	public Reverse (Stream str, SingleQueryTemplate t, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		super(str,d,time,mem);
		query = t;
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
			Graph graph = new Graph();
			
			graph = graph.getCompleteLazyGraph(events, query);
								
			count = count.add(new BigInteger(graph.final_count + ""));
			System.out.println("Sub-stream id: " + substream_id + " with count " + graph.final_count);
			
			memory.set(memory.get() + graph.nodeNumber * 12);		
		}
	}
}
