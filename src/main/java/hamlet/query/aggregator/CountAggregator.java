package hamlet.query.aggregator;

import hamlet.base.Event;

import java.util.ArrayList;

/**
 * the aggregator of count
 */
public class CountAggregator extends Aggregator {

    public CountAggregator(String funcName, String attributeName ){
        super(funcName, attributeName);

    }

    /**
     * given the list of the predecessor events, update the count
     * @param events
     *
     */
    // events: the list of predecessors
    // count = sum(event,counts)
    public void aggregate(ArrayList<Event> events){
//
//        BigInteger count = this.values.isEmpty()?BigInteger.ZERO: (BigInteger) this.values.get(0);

//        for (Event e: events){
//            count = count.add(e.getCounts().get(0));
//        }
//
//        this.values.set(0, count);
    }

    public String toString(){
        return "COUNT(*)";
    }
}
