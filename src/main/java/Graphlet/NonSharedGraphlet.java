package Graphlet;

import Event.Event;
import Graph.Snapshot;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;


/**
 * hold the count for all unshared events in all queries
 * qid=1: A, 10
 * qid=2: C, 5
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class NonSharedGraphlet extends Graphlet{

    private HashMap<Integer, Integer> countPerQueryHashMap;
    public NonSharedGraphlet(){
        super();
        countPerQueryHashMap = new HashMap<Integer, Integer>();
    }

    /**
     * increment hash map by corresponding query id of e
     * @param e the coming event e
     */
    @Override
    public void addEvent(Event e ){
        // TODO: 2020/2/21 only support AB+,
        //  If one query has multiple non shared event types，we need two keys in the hashmap
        //  For query A,E,B+, we need HashMap
        //  like {<query1, A>: 5},{<query1, E>: 3}
        //
        if (countPerQueryHashMap.get(e.getEventType().getQid())==null){ //if e is the first event
            countPerQueryHashMap.put(e.getEventType().getQid(),1);
        }else {
            int count = countPerQueryHashMap.get(e.getEventType().getQid());
            countPerQueryHashMap.put(e.getEventType().getQid(),count+1);
        }
    }


}
