package transaction;

import event.StreamPartitioner;
import lombok.Data;
import template.SingleQueryTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;


public class PrefixMQ extends TransactionMQ{
    private CountDownLatch done;
    private AtomicLong latency;
    private SingleQueryTemplate prefixT;
    private StreamPartitioner sp;

    public PrefixMQ(CountDownLatch d, AtomicLong time, SingleQueryTemplate prefixT, StreamPartitioner sp){
        super( d,  time);
    }

    @Override
    public void run() {
        System.out.println("run");
    }
    public void addQuery(SingleQueryTemplate s){

    }
}
