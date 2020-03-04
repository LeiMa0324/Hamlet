package Greta.transaction;

import Greta.event.Event;
import Greta.event.Stream;
import Greta.graph.Graph;
import Greta.template.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PrefixMQ extends TransactionMQ{
	
	public Stream stream;
	private SingleQueryTemplate template;
	private BigInteger[] final_counts;
	private int numQueries;
	public AtomicInteger memory;

	public PrefixMQ(Stream str, CountDownLatch d, AtomicLong time, ArrayList<String> queries) {
		// given a list of identical queries
		super(d, time);
		stream = str;
		
		// build hamletTemplate
		template = new SingleQueryTemplate(queries.get(0));
		
		numQueries = queries.size();
		final_counts = new BigInteger[numQueries];
	}
	
	public PrefixMQ(Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem, String pattern, int num) {
		// given one shared pattern and number of queries
		super(d, time);
		memory = mem;
		stream = str;
		
		// build hamletTemplate
		template = new SingleQueryTemplate(pattern);
		
		numQueries = num;
		final_counts = new BigInteger[numQueries];
	}

	public void run() {
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
			graph = graph.getCompleteGraph(events, template);
			
			// print final counts
			for (int i=0; i<numQueries; i++) {
				final_counts[i] = new BigInteger(graph.final_count + "");
//				System.out.println("Query id: " + (i+1) + " Substream id: " + substream_id +" with counts " + final_counts[i]);
			}
			
			memory.set(memory.get() + graph.nodeNumber * 12);
		}
	}
}
