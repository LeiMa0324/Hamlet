package baselines.sharon;

import baselines.commons.templates.SharonType;
import baselines.commons.transactions.TransactionMQ;
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
     * newSharon 以B的指数级增长，所以B越多,memroy 爆炸
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
        ArrayList<String> seqQueries = flatten(pattern);
        allQueries = new ArrayList<HashMap<Integer, ArrayList<SharonType>>>();

        for (String q : seqQueries) {
            //generate a hashmap by each flattened query
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

    public ArrayList<String> flatten(String P) {
        ArrayList<String> flattenQueies = new ArrayList<String>();
        String[] query = P.split(",");
        HashMap<String, ArrayList<String>> flatTypes = new HashMap<String, ArrayList<String>>();
        int frequency = 0;

        // find kleene and find the frequency of the kleene event
        for (String et:query){
            if (et.endsWith("+")){
                frequency = stream.generateRates(Integer.parseInt(et.replace("+","")));
            }
        }
        //form the flattened queries
        for (int i=1; i<frequency+1;i++){
            StringBuilder flattenQuery = new StringBuilder();
            for (String et:query){
                if (!et.endsWith("+")){
                    flattenQuery.append(et+",");

                }else {
                    for (int j=0;j<i;j++){
                        flattenQuery.append(et.replace("+","")+",");
                    }
                }
            }
            flattenQueies.add(flattenQuery.toString());
        }

        return flattenQueies;

    }

    public HashMap<Integer, ArrayList<SharonType>> generatePrefixCtrs(String pattern) {

        //event type: newSharon Type list
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

            // print final interCounts
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

        BigInteger retCount = new BigInteger("0");

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

                //一个event进来，对于每一个Sharon Type进行计算
                if (allQueries.get(i).containsKey(type)) {

                    for (SharonType sharonType : allQueries.get(i).get(type)) {

                        // if necessary, update current second
                        if (time > sharonType.current_second) {
                            sharonType.updateTime(time);    //保存上一次的计数
                        }

                        // START or UPD event
                        if (sharonType.isSTART) {
                            //start event type current +1
                            sharonType.current_second_count = sharonType.current_second_count.add(new BigInteger("1"));
                        } else {
                            SharonType predecessor = sharonType.predecessor;
                            if (time > predecessor.current_second) {
                                predecessor.updateTime(time);
                            }

                            //Share part
                            sharonType.current_second_count = sharonType.current_second_count.add(predecessor.previous_second_count);
                        }

//						System.out.println(prefix_counter.current_second_count);

                        // TRIG event
                        if (sharonType.isTRIG) {
                            counts_per_substream.set(i, sharonType.current_second_count);
//                            retCount = retCount.add(prefix_counter.current_second_count);
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

        for (BigInteger fqCount : counts_per_substream) {
            retCount = retCount.add(fqCount);
        }

        return retCount;
    }
}
