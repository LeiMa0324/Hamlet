package Greta.transaction;

import Greta.event.StreamPartitioner;
import Greta.event.Stream;
import Greta.event.StreamPartitioner;
import Greta.template.SingleQueryTemplate;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import java.util.HashSet;

public class GretaMQ extends TransactionMQ {
	
	public StreamPartitioner sp;
	public ArrayList<SingleQueryTemplate> workload;
	public AtomicInteger memory;
	
//	public ArrayList<Greta> graphs;

	public GretaMQ(CountDownLatch d, AtomicLong time, AtomicInteger mem, StreamPartitioner strmp) {
		super(d, time);
		memory = mem;
		sp = strmp;
		workload = new ArrayList<SingleQueryTemplate>();

//		graphs = new ArrayList<Greta>();
	}


	public void addQuery(SingleQueryTemplate q) {
		workload.add(q);
	}
	
	public void run() {
		int qid = 1;
		for (SingleQueryTemplate q : workload) {
			
//			long start =  System.currentTimeMillis();
			Stream stream = sp.partition();
//			long end =  System.currentTimeMillis();
//			long duration = end - start;
			
			Greta g = new Greta(stream, q, done, latency, memory, qid);
			
			long start =  System.currentTimeMillis();
			g.computeResults();
//			graphs.add(g);
			long end =  System.currentTimeMillis();
			long duration = end - start;
//			System.out.println(duration);
			
			latency.set(latency.get() + duration);
			
			qid++;
			
//			graphs.add(g);
		}
		done.countDown();
	}

}
