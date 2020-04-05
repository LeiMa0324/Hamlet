package hamlet.graphlet;

import hamlet.template.EventType;
import hamlet.utils.*;
import hamlet.event.Event;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/***
 * abstract Graphlet class
 *
 */

public abstract class Graphlet implements Observer {

    ArrayList<Event> eventList;     // event list
    public boolean isShared;
    public boolean isActive;
    public EventType eventType;        // the event type of this graphlet
    public HashMap<Integer, BigInteger> interCounts;    //intermediate final interCounts after this graphlet is finished



    public Graphlet(){
        this.eventList = new ArrayList<>();
        this.interCounts = new HashMap<>();
    }

    public abstract void addEvent(Event e);
    public abstract boolean IsCompatibleOf(Event e);
    public abstract void activeNotify(Object object);

}
