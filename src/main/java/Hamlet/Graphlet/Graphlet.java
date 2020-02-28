package Hamlet.Graphlet;

import Hamlet.Event.Event;
import Hamlet.Template.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;

/***
 * One Hamlet.Graphlet has a list of events
 *
 */


public abstract class Graphlet {

    ArrayList<Event> eventList;     // event list

    public Graphlet(){
        this.eventList = new ArrayList<Event>();
    }

    public abstract void addEvent(Event e);
    public abstract boolean IsCompatibleOf(Event e);

}
