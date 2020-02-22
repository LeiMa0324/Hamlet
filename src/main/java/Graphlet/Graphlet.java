package Graphlet;

import Event.Event;
import Graph.Snapshot;
import HamletTemplate.EventType;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

/***
 * One Graphlet has a list of events
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
