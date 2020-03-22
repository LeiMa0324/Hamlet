package baselines.sharon;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import baselines.commons.event.Event;
import baselines.commons.event.Stream;
import baselines.commons.templates.SharonType;
import baselines.commons.transactions.TransactionMQ;

public class Sharon extends TransactionMQ {
    /**
     * Sharon 以B的指数级增长，所以B越多,memroy 爆炸
     * 要跑只能在较少的epw内跑
     */
    public Stream stream;
    private ArrayList<HashMap<Integer, ArrayList<SharonType>>> allQueries;
    // list of prefix counters for each flattened query
    // Prefix counter is a hashmap with key event type id, value type object
    private BigInteger[] final_counts;
    private int numQueries;

    public AtomicInteger memory;

    public Sharon (Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem, String pattern, int num) {
        // given one shared pattern and number of queries
        super(d, time);
        stream = str;
        memory = mem;

        // build prefix counters
        ArrayList<String> seqQueries = flattenQueries(pattern);
        allQueries = new ArrayList<HashMap<Integer, ArrayList<SharonType>>>();

        for (String q : seqQueries) {
            allQueries.add(generatePrefixCtrs(q));
        }

        numQueries = num;
        final_counts = new BigInteger[numQueries];
    }

    /**
     * flatten a query 1,2+ to
     * 1,2
     * 1,2,2
     * 1,2,2,2...
     * the number of 2s is the number of 2 in the stream file
     * @param P
     * @return
     */

    public ArrayList<String> flattenQueries(String P) {
        ArrayList<String> Q = new ArrayList<String>();
        String[] query = P.split(",");
        ArrayList<String> types = new ArrayList<String>();
        HashMap<String, ArrayList<String>> flatTypes = new HashMap<String, ArrayList<String>>();

        //遍历query中所有event type
        for (int i = 0; i < query.length; i++) {
            String type = query[i].substring(0, 1);
            ArrayList<String> repTypes = new ArrayList<String>();
            repTypes.add(type + ",");

            //如果event type包含+
            if (query[i].endsWith("+")) {
                //计算stream中包含type的个数
                int R = stream.generateRates(Integer.parseInt(type));
                for (int j=1; j<R; j++) {
                    repTypes.add(repTypes.get(j-1).concat(type + ","));
                }
            }

            types.add(type);
            flatTypes.put(type, repTypes);
        }

        Q.add(types.get(0) + ",");
        int numPrefix = 1;

        for (int i=1; i<types.size(); i++) {
            ArrayList<String> repTypes = flatTypes.get(types.get(i));
            int nextNumPrefix = 0;

            for (int j=0; j<numPrefix; j++) {
                for (String s : repTypes) {
                    Q.add(Q.get(0).concat(s));
                    nextNumPrefix++;
                }
                Q.remove(0);
            }

            numPrefix = nextNumPrefix;
        }

        return Q;
    }

    public HashMap<Integer, ArrayList<SharonType>> generatePrefixCtrs(String pattern) {
        HashMap<Integer, ArrayList<SharonType>> prefix_counters = new HashMap<Integer, ArrayList<SharonType>>();
        String[] types = pattern.split(",");

        SharonType pred = new SharonType();
        ArrayList<SharonType> startType = new ArrayList<SharonType>();
        startType.add(pred);
        prefix_counters.put(Integer.parseInt(types[0]), startType);

        for (int i=1; i<types.length; i++) {
            SharonType nextPred = new SharonType(false, pred);
            if (i == types.length-1) {
                nextPred.isTRIG = true;
            }

            int k = Integer.parseInt(types[i]);
            if (prefix_counters.containsKey(k)) {
                ArrayList<SharonType> v = prefix_counters.get(k);
                v.add(nextPred);
                prefix_counters.put(k, v);
            } else {
                ArrayList<SharonType> v = new ArrayList<SharonType>();
                v.add(nextPred);
                prefix_counters.put(k, v);
            }
            pred = nextPred;
        }

        return prefix_counters;
    }

    public void run () {
        computeResults();
        done.countDown();
    }

    public void computeResults () {
        Set<String> substream_ids = stream.substreams.keySet();
        for (String substream_id : substream_ids) {

            long start =  System.currentTimeMillis();

            ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
            BigInteger count = computeResults(events);

            // print final counts
            for (int i=0; i<numQueries; i++) {
                final_counts[i] = new BigInteger(count + "");
				System.out.println("Query id: " + (i+1) + " Substream id: " + substream_id +" with count " + final_counts[i]);
            }

            long end =  System.currentTimeMillis();
            long duration = end - start;
            latency.set(latency.get() + duration);

            // clean up for next iteration
            for (int i=0; i<allQueries.size(); i++) {
                HashMap<Integer, ArrayList<SharonType>> ts = allQueries.get(i);
                for (Integer tid : ts.keySet()) {
                    ArrayList<SharonType> sh_list = ts.get(tid);
                    for (SharonType sh : sh_list) {
                        sh.resetTime();
                    }
                }
            }
        }
    }

    public BigInteger computeResults (ConcurrentLinkedQueue<Event> events) {

        // Set up final counters
        ArrayList<BigInteger> counts_per_substream = new ArrayList<BigInteger>();
        for (int i=0; i<allQueries.size(); i++) {
            counts_per_substream.add(new BigInteger("0"));
        }

        Event event = events.peek();

        while (event != null) {

            event = events.poll();
            Integer type = event.type;
            Integer time = event.sec;
//			System.out.println("--------------" + event.id + " TYPE: " + type);

            for (int i=0; i<allQueries.size(); i++) {
                if (allQueries.get(i).containsKey(type)) {
                    for (SharonType prefix_counter : allQueries.get(i).get(type)) {

                        // if necessary, update current second
                        if (time > prefix_counter.current_second) {
                            prefix_counter.updateTime(time);
                        }

                        // START or UPD event
                        if (prefix_counter.isSTART) {
                            prefix_counter.current_second_count = prefix_counter.current_second_count.add(new BigInteger("1"));
                        } else {
                            SharonType predecessor = prefix_counter.predecessor;
                            if (time > predecessor.current_second) {
                                predecessor.updateTime(time);
                            }

                            //Share part
                            prefix_counter.current_second_count = prefix_counter.current_second_count.add(predecessor.previous_second_count);
                        }

//						System.out.println(prefix_counter.current_second_count);

                        // TRIG event
                        if (prefix_counter.isTRIG) {
                            counts_per_substream.set(i, prefix_counter.current_second_count);
                        }
                    }
                }
            }

            event = events.peek();
        }

        for (int i=0; i<allQueries.size(); i++) {
            for (int k : allQueries.get(i).keySet()) {
                memory.set(memory.get() + allQueries.get(i).get(k).size() * 8);
            }
        }

        BigInteger retCount = new BigInteger("0");
        for (BigInteger fqCount : counts_per_substream) {
            retCount = retCount.add(fqCount);
        }

        return retCount;
    }
}
