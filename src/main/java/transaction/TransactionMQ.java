package transaction;

//import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TransactionMQ implements Runnable {
	
	public CountDownLatch done;
	AtomicLong latency;
	//public HashSet<Transaction> trSet;

	public TransactionMQ(CountDownLatch d, AtomicLong time) {
		done = d;
		latency = time;
	}

}
