package hamlet.aoldgraphlet;

import hamlet.aoldevent.Event;
import hamlet.aoldgraph.Snapshot;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.ArrayList;

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
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SharedGraphlet extends Graphlet{
     BigInteger coeff;
     boolean calculated;


    public SharedGraphlet(Event e){
        super();
        this.eventType = e.eventType;
        coeff = new BigInteger("0");
        isShared = true;
        this.calculated = false;

        //initiate all the inter counts
        for (Integer q: eventType.getQids()){
            interCounts.put(q,BigInteger.ZERO);
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

    public void addEvents(ArrayList<Event> batch){
        this.eventList.addAll(batch);
        /**
         * coefficient of the graphlet = 2**n-1
         */
        this.coeff = new BigInteger("2").pow(eventList.size()).subtract(new BigInteger("1"));
    }

    /***
     * update the intermediate counts when finishing this graphlet，
     * called by finishingGraphlet
     * @param snapshot the old snapshot
     */
    public void updateCounts(Snapshot snapshot){

        if (!snapshot.getCounts().isEmpty()){
            for (Integer q: eventType.getQids()){
                // if the shared event is the start event, inter count = coeff
                if (eventType.getTypes().get(q).equals("START")||eventType.getTypes().get(q).equals("START|END")){
                    interCounts.put(q, coeff);
                }else {
                    //if not stat event, inter count = snapshot * coeff
                interCounts.put(q, snapshot.getCounts().get(q).multiply(coeff));
                }
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
