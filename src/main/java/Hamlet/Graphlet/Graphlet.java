package Hamlet.Graphlet;

import Hamlet.Event.Event;
import lombok.Data;

import java.util.ArrayList;

/***
 * One Hamlet.Graphlet has a list of events
 *
 */
@Data
public abstract class Graphlet {

    ArrayList<Event> eventList;     // event list

    public Graphlet(){
        this.eventList = new ArrayList<Event>();

    }

    public abstract void addEvent(Event e);

}
