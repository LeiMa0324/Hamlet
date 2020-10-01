package hamlet.aoldgraphlet;

import hamlet.aoldtemplate.EventType;
import hamlet.zutils.*;
import hamlet.aoldevent.Event;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/***
 * abstract Graphlet class
 *
 */
@Data
public abstract class Graphlet implements Observer {

    ArrayList<Event> eventList;     // event list
    public boolean isShared;
    public boolean isActive;

    // the event type of this graphlet
    public EventType eventType;

    //intermediate aggregation interCounts after this graphlet is finished
    public HashMap<Integer, BigInteger> interCounts;



    public Graphlet(){
        this.eventList = new ArrayList<>();
        this.interCounts = new HashMap<>();
    }

    public abstract void addEvent(Event e);
    public abstract boolean IsCompatibleOf(Event e);
    public abstract void activeNotify(Object object);


}
