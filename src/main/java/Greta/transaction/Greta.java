package Greta.transaction;

//import java.math.BigInteger;

import Greta.event.*;
import Greta.graph.*;
import Greta.template.*;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Greta extends Transaction {
	
	SingleQueryTemplate query;
	int id;
		
	public Greta (Stream str, SingleQueryTemplate t, CountDownLatch d, AtomicLong time, AtomicInteger mem, int qid) {		
		super(str,d,time,mem);
		query = t;
		id = qid;
	}
	
	public void run () {
		/* This method is not called in MUSE experiments */
		long start =  System.currentTimeMillis();
		computeResults();
		long end =  System.currentTimeMillis();
		long duration = end - start;
		latency.set(latency.get() + duration);				
		done.countDown();
	}
	
	public void computeResults () {
		
		Set<String> substream_ids = stream.substreams.keySet();	//将stream分为substream
		ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<Event>();
		for (String substream_id : substream_ids) {		//每个substream建立一个Graph
			events.addAll(stream.substreams.get(substream_id));
		}
		Graph graph = new Graph();
		graph = graph.getCompleteGraph(events, query);

//			latency.set(latency.get() + (int) (0.01*graph.weight));
//			count = count.add(new BigInteger(graph.final_count + ""));

			System.out.println("Query id: " + id + " Substream id: " +  " with count " + graph.final_count);

		memory.set(memory.get() + graph.nodeNumber * 12);
	}
}