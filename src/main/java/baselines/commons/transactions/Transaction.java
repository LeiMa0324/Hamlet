package baselines.commons.transactions;



import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import baselines.commons.event.Stream;

public abstract class Transaction implements Runnable {
	
	public Stream stream;
	public CountDownLatch done;	
	public AtomicLong latency;
	public AtomicInteger memory;
	public BigInteger count;
	
	public Transaction (Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem) {		
		stream = str;	 
		done = d;
		latency = time;
		memory = mem;
		count = new BigInteger("0");
	}
}
