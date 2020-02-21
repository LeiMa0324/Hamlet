package transaction;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashMap;

import template.*;
import event.Event;
import event.Stream;
import graph.MuseGraph;

public class MuseMQ extends TransactionMQ{
	public Stream stream;
	private MultiQueryTemplate template;
	private HashMap<Integer, BigInteger> final_counts;
	private int numQueries;
	private boolean prefixtest;

	public MuseMQ(Stream str, CountDownLatch d, AtomicLong time, ArrayList<String> queries, boolean isPrefix) {
		super(d, time);
		stream = str;
		prefixtest = isPrefix;
		if (prefixtest) {

			template = new MultiQueryTemplate(queries.get(0), 1);
		} else {
			//创建multiple query template
			template = new MultiQueryTemplate(queries);
		}
		final_counts = new HashMap<Integer, BigInteger>();
		numQueries = queries.size();
	}
	
	public MuseMQ(Stream str, CountDownLatch d, AtomicLong time, ArrayList<String> queries, int suffixLength) {
		super(d, time);
		stream = str;
		prefixtest = false;
		template = new MultiQueryTemplate(queries, suffixLength);
		final_counts = new HashMap<Integer, BigInteger>();
		numQueries = queries.size();
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
			for (int i=1; i <= numQueries; i++) {
				final_counts.put(i, new BigInteger("0"));
			}
		 
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			MuseGraph graph = new MuseGraph(final_counts.size());
			graph = graph.getCompleteGraph(events, template);
//			if (old) {
//				graph = graph.getCompleteGraphGS(events, template);
//			} else {
//				graph = graph.getCompleteGraph(events, template);
//			}
			for (Integer i : final_counts.keySet()) {
				if (prefixtest) {
					final_counts.put(i, final_counts.get(i).add(new BigInteger(graph.final_count.get(1) + "")));
				} else {
					final_counts.put(i, final_counts.get(i).add(new BigInteger(graph.final_count.get(i) + "")));
				}
				System.out.println("Query id: " + i + " Substream id: " + substream_id +" with count " + final_counts.get(i));
			}
			
//			memory.set(memory.get() + graph.nodeNumber * 12);		//??????????? 4 for count, 8 for event (id + sec)?
		}
	}
}
