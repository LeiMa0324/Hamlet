package Hamlet.Graphlet;

import Hamlet.Event.Event;
import Hamlet.Template.EventType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigInteger;

/**
 * The graphlet of the shared events
 */

@Data
@EqualsAndHashCode(callSuper = true)
public class SharedGraphlet extends Graphlet{
    public final EventType eventType;        // the event type of this graphlet
    private BigInteger coeff;
    private boolean calculated;

    public SharedGraphlet(Event e){
        super();
        this.eventType = e.eventType;
        coeff = new BigInteger("0");
        isShared = true;
        this.calculated = false;
        addEvent(e);

    }

    /**
     * add an event to this shared graphlet
     * @param e a coming event
     */
    @Override
    public void addEvent(Event e){
        if (this.eventList==null){
            e.setId(0);
            this.eventList.add(e);
        }
        else {
            e.setId(eventList.size());
            this.eventList.add(e);
        }
        CalculateCoefficient();

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

        BigInteger temp_coeff = this.coeff.add(new BigInteger("1"));
        this.coeff = this.coeff.add(temp_coeff);
    }

    /**
     * set its active status by notification from graph
     * @param object
     */
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
