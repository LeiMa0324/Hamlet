package Graphlet;

import Event.Event;
import Graph.Snapshot;
import HamletTemplate.EventType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
/**
 * The graphlet of the shared events
 */

@Data
@EqualsAndHashCode(callSuper = true)
public class SharedGraphlet extends Graphlet{
    EventType eventType;        // the event type of this graphlet
    private int coeff;
    public SharedGraphlet(){
        super();

    }

    /**
     * add an event to this shared graphlet
     * @param e
     */
    @Override
    public void addEvent(Event e){
        if (this.eventList==null){
            e.setId(0);
            this.eventList.add(e);
            this.eventType = e.getEventType();
        }
        else {
            e.setId(eventList.size());
            this.eventList.add(e);
        }

    }

    /**
     * return true when event can be added to this graphlet
     * @param e a coming event
     * @return boolean
     */
    public boolean IsCompatibleOf(Event e){
        return this.eventType == null || this.eventType == e.getEventType();
    }
    /**
     * calculate coefficient, Fibonacci-like
     */
    public void  CalculateCoefficient(){
        int inter_sum =0;
        int coeff = 0;
        int coeff_per_event =0;
        for(int i=0;i<eventList.size();i++){
            coeff_per_event = inter_sum +1;
            inter_sum += coeff_per_event;
            System.out.println(coeff_per_event+" ");
            coeff +=coeff_per_event;
        }
        this.coeff = coeff;
    }
}
