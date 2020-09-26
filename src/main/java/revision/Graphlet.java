package revision;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class Graphlet {

    private ArrayList<Event> events;
    public boolean isShared;

    public int vendorID;    //the event type of this graphlet

    private BigInteger coeff;

    // the counts of this graphlet
    //<query id, count>
    public HashMap<Integer, BigInteger> counts;

    // the sums of this graphlet
    //<query id, sum>
    public HashMap<Integer, BigInteger> sums;

    public Graphlet(Event event, boolean isShared){
        this.vendorID = event.getVendorID();
        this.events = new ArrayList<>();
        this.events.add(event);
        this.isShared = isShared;
        this.coeff = BigInteger.ONE;
        this.counts = new HashMap<>();

    }

    /**
     * add a batch into the graphlet
     * @param batch a given batch
     */
    public void addBatch(ArrayList<Event> batch){
        events.addAll(batch);
        this.coeff = new BigInteger("2").pow(events.size()).subtract(new BigInteger("1"));

    }

    /**
     * given a snapshot
     * do the count for the graphlet
     */
    public void count(HashMap<Integer, BigInteger> snapshot){

        //update the counts for each query
        // count = coeff* snapshot
        for (Integer i: snapshot.keySet()){
            this.counts.put(i,snapshot.get(i).multiply(this.coeff));
        }

    }

    /**
     * given a snapshot
     * do the sum for the totalAmount
     * @param snapshot
     */
    public void sum(HashMap<Integer, BigInteger> snapshot){

    }
}
