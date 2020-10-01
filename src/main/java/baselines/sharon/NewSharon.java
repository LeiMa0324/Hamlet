package baselines.sharon;

import baselines.commons.event.Event;
import baselines.commons.event.Stream;
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

public class NewSharon extends TransactionMQ {
    /**
     * newSharon 以B的指数级增长，所以B越多,memroy 爆炸
     * 要跑只能在较少的epw内跑
     */
    public Stream stream;
    private ArrayList<HashMap<Integer, ArrayList<MySharonType>>> allQueries;
    // list of prefix counters for each flattened query
    // Prefix counter is a hashmap with key event type id, value type object
    private BigInteger[] final_counts;
    private int numQueries;

    public AtomicInteger memory;

    public NewSharon(Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem, String pattern, int num) {
        // given one shared Pattern and number of queries
        super(d, time);
        stream = str;
        memory = mem;

        // build prefix counters
        ArrayList<String> flattenedQueries = flatten(pattern);
        allQueries = new ArrayList<>();

        for (String q : flattenedQueries) {
            //generate a hashmap by each flattened query
            allQueries.add(flattenedQToHash(q));
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

    /**
     * transform a flattened query into a hashmap
     * event type: newSharon Type
     * @param flattenedQuery
     * @return
     */
    public HashMap<Integer, ArrayList<MySharonType>> flattenedQToHash(String flattenedQuery) {

        //event type: newSharon Type list
        HashMap<Integer, ArrayList<MySharonType>> sharonTypeHash = new HashMap<>();
        String[] types = flattenedQuery.split(",");


        MySharonType pred = new MySharonType();
        ArrayList<MySharonType> start = new ArrayList<>();
        start.add(pred);
        sharonTypeHash.put(Integer.parseInt(types[0]), start); //将第一个event type放入hashmap中

        //keep track of the last event in the hashmap
        int lastEvent = Integer.parseInt(types[0]);

        // 遍历剩余的type
        for (int i=1; i<types.length; i++) {

            //if meet consecutive events
            if (types[i].equals(lastEvent+"")){

                //kleene的SharonType在hashmap中仅有一个
                MySharonType lastSharon = sharonTypeHash.get(lastEvent).get(0);
                lastSharon.isKleene = true; //last sharon has kleene
                //设置type
                if (i == types.length-1) {
                    lastSharon.isTRIG = true;
                }
                //maintain pred, increment the repeat
                lastSharon.setRepeats(lastSharon.getRepeats()+1);
                ArrayList<MySharonType> sharons = new ArrayList<>();
                sharons.add(lastSharon);
                sharonTypeHash.put(lastEvent, sharons);     //maintain hashmap

                pred = lastSharon;  //maintain pred
                //maintain last event, do nothing
            }
            else{
                //与上一个event不同，新建一个sharon type

                //初始化nextpred
                MySharonType nextPred = new MySharonType(false);
                nextPred.isKleene = true;
                //设置type
                if (i == types.length-1) {
                    nextPred.isTRIG = true;
                }

                //maintain predecessor
                nextPred.setPredecessor(pred);

                //case1: new list
                if (!sharonTypeHash.containsKey(Integer.parseInt(types[i]))){
                    ArrayList<MySharonType> sharons = new ArrayList<>();
                    sharons.add(nextPred);
                    sharonTypeHash.put(Integer.parseInt(types[i]), sharons);     //maintain hashmap
                }else {
                    ArrayList<MySharonType> sharons = sharonTypeHash.get(Integer.parseInt(types[i]));  //maintain hashmap
                    sharons.add(nextPred);
                    sharonTypeHash.put(Integer.parseInt(types[i]), sharons);     //maintain hashmap

                }

                pred = nextPred; //maintain pred
                lastEvent = Integer.parseInt(types[i]); //maintain last Event

                }

            }


        return sharonTypeHash;
    }

    public void run () {
        computeResults();
        done.countDown();
    }
    public BigInteger NewcomputeResults (ConcurrentLinkedQueue<Event> events) {

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

            //第几个query
            for (int i=0; i<allQueries.size(); i++) {
                HashMap<Integer, ArrayList<MySharonType>> sharonTypeHash = allQueries.get(i);

                //如果当前flatten query不包含该事件，则去下一个flattened query
                if (!allQueries.get(i).containsKey(type)){
                    continue;
                }

                //一个event进来，对于每一个SharonType进行计算
                for (MySharonType sharonType : allQueries.get(i).get(type)) {

                    // if necessary, update current second
                    if (time > sharonType.current_second) {
                        sharonType.updateTime(time);    //保存上一次的计数
                    }

                    // START or UPD event
                    if (sharonType.isSTART) {
                        //start event type current +1
                        sharonType.current_second_count = sharonType.current_second_count.add(new BigInteger("1"));
                    } else {
                        //其他情况，获取predecessor
                        MySharonType predecessor = sharonType.predecessor;
                        if (time > predecessor.current_second) {
                            predecessor.updateTime(time);
                        }
                        int kleeneFlag =sharonType.isKleene?1:0;

                        switch (kleeneFlag){
                            case 1: //kleene events
                                //第一次update count
                                if (sharonType.notCalculated)
                                {
                                    sharonType.current_second_count = sharonType.current_second_count.add(predecessor.previous_second_count);
                                    sharonType.notCalculated = false;
                                    sharonType.setCalculatedRepeats(sharonType.calculatedRepeats+1);


                                }else {
                                    if (sharonType.calculatedRepeats<sharonType.repeats){
                                        //do nothing
                                        sharonType.setCalculatedRepeats(sharonType.calculatedRepeats+1);

                                    }else {
                                        sharonType.setCalculatedRepeats(sharonType.calculatedRepeats+1);

                                        sharonType.current_second_count = new BigInteger(Combination(sharonType.calculatedRepeats, sharonType.repeats)+"").
                                                multiply(sharonType.predecessor.previous_second_count);
                                        counts_per_substream.set(i, sharonType.current_second_count);


                                    }

                                }

                                break;
                            case 0: //normal events

                                //Share part
                                sharonType.current_second_count = sharonType.current_second_count.add(predecessor.previous_second_count);
                                // TRIG event
                                if (sharonType.isTRIG) {
                                    counts_per_substream.set(i, sharonType.current_second_count);
//                            retCount = retCount.add(prefix_counter.current_second_count);

                                }
                                break;
                            }
                        }
                    if (sharonType.isTRIG) {
                        counts_per_substream.set(i, sharonType.current_second_count);
//                            retCount = retCount.add(prefix_counter.current_second_count);

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
            System.out.println(fqCount);
            retCount = retCount.add(fqCount);
        }

        return retCount;
    }
    public void computeResults () {
        Set<String> substream_ids = stream.substreams.keySet();
        for (String substream_id : substream_ids) {

            long start =  System.currentTimeMillis();

            ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
            BigInteger count = NewcomputeResults(events);

            // print final interCounts
            for (int i=0; i<numQueries; i++) {
                final_counts[i] = new BigInteger(count + "");
                System.out.println("stockQuery id: " + (i+1) + " Substream id: " + substream_id +" with count " + final_counts[i]);
            }

            long end =  System.currentTimeMillis();
            long duration = end - start;
            latency.set(latency.get() + duration);

            // clean up for next iteration
            for (int i=0; i<allQueries.size(); i++) {
                HashMap<Integer, ArrayList<MySharonType>> ts = allQueries.get(i);
                for (Integer tid : ts.keySet()) {
                    ArrayList<MySharonType> sh_list = ts.get(tid);
                    for (MySharonType sh : sh_list) {
                        sh.resetTime();
                    }
                }
            }
        }
    }

    public int Combination(int n, int k){
        int a=1,b=1;
        if(k>n/2) {
            k=n-k;
        }
        for(int i=1;i<=k;i++) {
            a*=(n+1-i);
            b*=i;
        }
        return a/b;
    }

}
