package hamlet.event;


import hamlet.template.EventType;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class Event {
    private int id; //id in the graphlet
    private int sec;
    public final String string;
    public final EventType eventType;
    private HashMap<Integer, BigInteger> count;
    private ArrayList<Integer> endQueries;
    private BigInteger coeff;

    /**
     * take a line of record，convert it to a Hamlet.Event
     * @param line line of record
     */
    public Event(String line, EventType et){
        this.eventType = et;
        String[] record = line.split(",");
        this.sec = Integer.parseInt(record[0]);
        this.string = record[1];
        this.count = new HashMap<>();
        this.endQueries = new ArrayList<>();
        this.coeff = new BigInteger("0");
        findEndQuery();
    }

    public void updateCount(Integer qid, BigInteger count){
        this.count.put(qid, count);
    }

    private void findEndQuery(){

        for(Integer q: eventType.getQids()){
            if (eventType.getTypebyQid(q).equals("END")){
                this.endQueries.add(q);
            }
        }
    }

}
