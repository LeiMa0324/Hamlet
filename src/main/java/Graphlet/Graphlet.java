package Graphlet;

import Event.Event;
import Graph.Snapshot;
import HamletTemplate.EventType;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

/***
 * 一个Graphlet有一个序列的events，同一个event type
 * 有一个输入的snapshots（hashmap）
 */
@Data
public abstract class Graphlet {

    ArrayList<Event> eventList;     // event list

    public Graphlet(){
        this.eventList = new ArrayList<Event>();

    }

    public abstract void addEvent(Event e);

}
