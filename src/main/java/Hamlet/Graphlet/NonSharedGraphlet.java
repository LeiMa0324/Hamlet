package Hamlet.Graphlet;

import Hamlet.Event.Event;
import Hamlet.Template.EventType;
import lombok.Data;

import java.math.BigInteger;
import java.util.HashMap;


/**
one unshared event type has one unshared graphlet
 */

@Data
public class NonSharedGraphlet extends Graphlet{

    public final EventType eventType;        // the event type of this graphlet
    private HashMap<Integer, BigInteger> counts;
    /**
     * construct a unshared g with an event type
     * @param eventType
     */
    public NonSharedGraphlet(EventType eventType){
        super();
        this.eventType = eventType;
        counts = new HashMap<Integer, BigInteger>();
        for (Integer q: eventType.getQids()){
            counts.put(q,new BigInteger("0"));
        }
    }

    /**
     * increment hash map by corresponding query id of e
     * @param e the coming event e
     */
    @Override
    public void addEvent(Event e ){
        for (Integer q: e.eventType.getQids()){
            BigInteger count = counts.get(q);
            counts.put(q,count.add(new BigInteger("1")));
        }
        System.out.println("=======================================");
        System.out.println("incoming event"+e);;
        System.out.println("count is"+counts);
    }

    public boolean IsCompatibleOf(Event e){
        return eventType.equals(e.eventType);
    }
}
