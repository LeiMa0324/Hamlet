package Hamlet.Template;

import Hamlet.Event.Event;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Event type store the information about an event type
 * edges: the edge between this event type and its predecessors, can query by qid
 * isShared: if this event type is shared
 * qids: the queries it belongs to
 * type: the types is has, for each query
 */
@Data
public class EventType {
    public final String string;    // the actual event type string
    // 1: 'A', 2: 'C'
    private HashMap<Integer, ArrayList<EventType>> edges;     //follow the edge to find the predecessors
    public final boolean isShared;
    private ArrayList<Integer> qids;
    private HashMap<Integer, String> types;    //"START", "END", "REGULAR"


    public EventType(String string, boolean isShared, int qid){
        this.qids = new ArrayList<Integer>();
        qids.add(qid);
        this.types = new HashMap<Integer, String>();
        this.string = string;
        this.isShared = isShared;
        this.edges = new HashMap<Integer, ArrayList<EventType>>();
    }

    /**
     * pred: query id list
     * put the query id to the list of the predecessor
     * @param qid
     */
    public void addEdges( Integer qid, EventType pred){
        if (edges.get(qid)==null){
            ArrayList<EventType> preds = new ArrayList<EventType>();
            preds.add(pred);
            edges.put(qid, preds);
        }
        else {
            edges.get(qid).add(pred);
        }
        if (!qids.contains(qid)){
            qids.add(qid);
        }
    }

    /**
     * return the immediate predecessor for a given query
     * @param qid
     * @return
     */
    public EventType getPred(int qid){
        int index=0;
        for (EventType p: edges.get(qid)){
            if (!p.equals(this)){
                index = edges.get(qid).indexOf(p);

            }

        }
        return edges.get(qid).get(index);
    }

    public void addType(int qid, String type){
        types.put(qid, type);
    }

    public String getTypebyQid(Integer qid){
        return types.get(qid);
    }
    @Override
    public String toString(){
        StringBuilder strbuilder = new StringBuilder( "Event Type info:\n   String:"+string+"\n"+"   isShared: "+isShared+"\n");
        for (Integer qid: edges.keySet()){
            strbuilder.append(" qid: "+qid +", type:" +types.get(qid)+ ", pred: ");
            for (EventType p: edges.get(qid)){
                strbuilder.append(p.string+", ");
            }
            strbuilder.append("\n");
        }
        return strbuilder.toString();

    }

}
