package Hamlet.Graphlet;

import Hamlet.Event.Event;
import Hamlet.Template.EventType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigInteger;
import java.util.HashMap;


/**
each unshared event type has an unshared graphlet
 Maintains:
 1. is this shared
 2. the event type of this graphlet
 3. the counts for each query
 */

@Data
@EqualsAndHashCode(callSuper=false)
public class NonSharedGraphlet extends Graphlet{

    public final EventType eventType;        // the event type of this graphlet
    private HashMap<Integer, BigInteger> counts;

    /**
     * construct a unshared hamletG with an event
     * @param e the incoming event
     */
    public NonSharedGraphlet(Event e){
        super();
        this.eventType = e.eventType;
        counts = new HashMap<Integer, BigInteger>();
        for (Integer q: eventType.getQids()){
            counts.put(q,new BigInteger("0"));
        }
        isShared = false;
        isCalculated = false;
        addEvent(e);
    }

    /**
     * increment hash map by corresponding query id of e
     * @param e the coming event e
     */
    @Override
    public void addEvent(Event e){
        for (Integer q: e.eventType.getQids()){
            BigInteger count = counts.get(q);
            counts.put(q,count.add(new BigInteger("1")));   //increment the count for this query
        }

    }

    public boolean IsCompatibleOf(Event e){
        return eventType.equals(e.eventType);
    }

    @Override
    public void notify(Object object){

        String activeFlag = (String)object;
        this.isActive = this.eventType.string.equals(activeFlag);
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder("***************** GRAPHLET INFO *****************\n\n");
        stringBuilder.append( String.format("%-18s %-5s", "Event Type: ", eventType.string));

        stringBuilder.append(String.format("\n%-18s %-5s", "Is shared: ", "No"));
        stringBuilder.append("\nCounts:");
        for (Integer q: counts.keySet()){
            stringBuilder.append(String.format("\n%23s %-5s", "qid: ", q+","));
            stringBuilder.append("count: "+ counts.get(q));
        }
        return stringBuilder.toString();
    }
}
