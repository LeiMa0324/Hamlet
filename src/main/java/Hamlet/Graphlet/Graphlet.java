package Hamlet.Graphlet;

import Hamlet.Utils.*;
import Hamlet.Event.Event;
import Hamlet.Template.EventType;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import Hamlet.Utils.*;

import java.util.ArrayList;

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
    public abstract void notify(Object object);

}
