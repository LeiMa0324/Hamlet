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
     * construct a unshared g with an event
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
            counts.put(q,count.add(new BigInteger("1")));
        }

    }

    public boolean IsCompatibleOf(Event e){
        return eventType.equals(e.eventType);
    }

    @Override
    public void notify(Object object){

        String activeFlag = (String)object;
        if (this.eventType.string.equals(activeFlag)){
            this.isActive = true;
        }else {
            this.isActive = false;

        }

    }
}
