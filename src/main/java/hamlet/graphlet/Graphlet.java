package hamlet.graphlet;

import hamlet.utils.*;
import hamlet.event.Event;

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

    public Graphlet(){
        this.eventList = new ArrayList<>();
    }

    public abstract void addEvent(Event e);
    public abstract boolean IsCompatibleOf(Event e);
    public abstract void activeNotify(Object object);
    public abstract void finishNotify(Object object);

}
