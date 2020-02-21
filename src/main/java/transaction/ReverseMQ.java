package transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashSet;

public class ReverseMQ extends TransactionMQ {
	
	public HashSet<Reverse> trSet;

	public ReverseMQ(CountDownLatch d, AtomicLong time) {
		super(d, time);
		trSet = new HashSet<Reverse>();
	}
	
	public void addQuery(Reverse g) {
		trSet.add(g);
	}
	
	public void run() {
		long start =  System.currentTimeMillis();
		for (Reverse r : trSet) {		
			r.computeResults();
		}
		long end =  System.currentTimeMillis();
		long duration = end - start;		
		latency.set(latency.get() + duration);	
		done.countDown();
	}

}