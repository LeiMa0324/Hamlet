package hamlet.graphlet;

import hamlet.event.Event;
import hamlet.template.EventType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigInteger;
import java.util.HashMap;


/**
each unshared event type has an unshared graphlet
 Maintains:
 1. is this shared
 2. the event type of this graphlet
 3. the counts for each query
 */

@Data
@EqualsAndHashCode(callSuper=false)
public class NonSharedGraphlet extends Graphlet{

    public final EventType eventType;        // the event type of this graphlet
    private Integer eventNum;
    private HashMap<Integer, BigInteger> counts;
    public HashMap<Integer, Boolean> isCalculated;
    private HashMap<Integer, BigInteger> predCounts;


    /**
     * construct a unshared hamletG with an event
     * @param e the incoming event
     */
    public NonSharedGraphlet(Event e){
        super();
        this.eventType = e.eventType;
        this.counts = new HashMap<Integer, BigInteger>();
        this.predCounts = new HashMap<>();
        for (Integer q: eventType.getQids()){
            counts.put(q,new BigInteger("0"));
        }
        this.isShared = false;
        this.eventNum = new Integer(0);
        this.isCalculated = new HashMap<>();
        for (Integer q: eventType.getQids()){
            isCalculated.put(q, false);

        }
        addEvent(e);
    }

    public void updateCounts(){
        //for start type, count = num of events
        for (Integer qid: eventType.getQids()){
            if (eventType.getTypebyQid(qid).equals("START")){
                counts.put(qid, new BigInteger(eventNum+""));

            }else {
                // not start type, count = pred count* event number
                counts.put(qid, predCounts.get(qid).multiply(new BigInteger(eventNum+"")));
            }
        }
    }

    /**
     * increment hash map by corresponding query id of e
     * @param e the coming event e
     */
    @Override
    public void addEvent(Event e){
        eventNum++;

    }

    public boolean IsCompatibleOf(Event e){
        return eventType.equals(e.eventType);
    }

    @Override
    public void activeNotify(Object object){

        String activeFlag = (String)object;
        this.isActive = this.eventType.string.equals(activeFlag);

    }
    @Override
    public void finishNotify(Object object){
        String activeFlag = (String)object;
        if (this.eventType.string.equals(activeFlag)){
            updateCounts();
        }
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder("***************** GRAPHLET INFO *****************\n\n");
        stringBuilder.append( String.format("%-18s %-5s", "Event Type: ", eventType.string));

        stringBuilder.append(String.format("\n%-18s %-5s", "Is shared: ", "No"));
        stringBuilder.append("\nCounts:");
        for (Integer q: counts.keySet()){
            stringBuilder.append(String.format("\n%23s %-5s", "qid: ", q+","));
            stringBuilder.append("count: "+ counts.get(q));
        }
        return stringBuilder.toString();
    }
}
