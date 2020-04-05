package hamlet.graphlet;

import hamlet.event.Event;
import hamlet.graph.Snapshot;
import hamlet.template.EventType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigInteger;
import java.util.HashMap;

/**
 * The graphlet of the shared events
 *  Maintains:
 *  1. is this shared
 *  2. the event type of this graphlet
 *  3. the current coefficient
 *  4. has this graphlet been calculated into the final count
 *
 */

@Data
@EqualsAndHashCode(callSuper = true)
public class SharedGraphlet extends Graphlet{
    private BigInteger coeff;
    private boolean calculated;


    public SharedGraphlet(Event e){
        super();
        this.eventType = e.eventType;
        coeff = new BigInteger("0");
        isShared = true;
        this.calculated = false;
        for (Integer q: eventType.getQids()){
            interCounts.put(q,new BigInteger("0"));
        }
        addEvent(e);

    }

    /**
     * add an event to this shared graphlet
     * Maintain it's count if e is an end event
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


    public void updateCounts(Snapshot snapshot){
        if (!snapshot.getCounts().isEmpty()){
            for (Integer q: eventType.getQids()){
                interCounts.put(q, snapshot.getCounts().get(q).multiply(coeff));
            }
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

        BigInteger temp_coeff = this.coeff.add(new BigInteger("1"));
        this.coeff = this.coeff.add(temp_coeff);
    }

    /**
     * set its active status by notification from graph
     * @param object
     */
    @Override
    public void activeNotify(Object object){

        String activeFlag = (String)object;
        this.isActive =this.eventType.string.equals(activeFlag);

    }

    @Override
    public void finishNotify(Object object){


    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder("***************** GRAPHLET INFO *****************\n\n");
        stringBuilder.append(String.format("%-18s %-5s", "Event Type: ", eventType.string));
        stringBuilder.append(String.format("\n%-18s %-5s", "Is shared: ", "Yes"));
        stringBuilder.append(String.format("\n%-18s %-5s", "Counts: ", eventList.size()));
        stringBuilder.append(String.format("\n%-18s %-5s", "Sum of coeffs: ", coeff));

        return stringBuilder.toString();
    }
}
