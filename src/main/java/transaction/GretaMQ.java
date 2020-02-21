package transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashSet;

public class GretaMQ extends TransactionMQ {
	
	public HashSet<Greta> trSet;

	public GretaMQ(CountDownLatch d, AtomicLong time) {
		super(d, time);
		trSet = new HashSet<Greta>();
	}
	
	public void addQuery(Greta g) {
		trSet.add(g);
	}
	
	public void run() {
		long start =  System.currentTimeMillis();
		for (Greta g : trSet) {		
//			long start2 =  System.currentTimeMillis();
			g.computeResults();
//			long end2 =  System.currentTimeMillis();
//			long dur2 = end2 - start2;
//			System.out.println(dur2);
		}
		long end =  System.currentTimeMillis();
		long duration = end - start;
		latency.set(latency.get() + duration);	
		done.countDown();
	}

}
