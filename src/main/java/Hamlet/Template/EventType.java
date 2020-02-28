package Hamlet.Template;

import Hamlet.Event.Event;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class EventType {
    public String string;    // the actual event type string
    // 1: 'A', 2: 'C'
    private HashMap<Integer, ArrayList<EventType>> edges;     //follow the edge to find the predecessors
    private boolean isShared;
    private Integer Qid;
    private HashMap<Integer, String> types;    //"START", "END", "REGULAR"

    public EventType(String string, boolean isShared){
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
    }
    public ArrayList<EventType> getPreds(int qid){
        return edges.get(qid);
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
