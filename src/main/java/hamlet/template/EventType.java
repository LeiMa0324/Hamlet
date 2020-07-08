package hamlet.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Event type store the following information about an event type in the template
 * if it's shared
 * its predecessor event types
 * event type in each query
 */
@Data
public class EventType {

    //the actual event type string
    public final String string;

    //a hashmap from the query id to a list of predecessor event types
    private HashMap<Integer, ArrayList<EventType>> edges;

    //if this event type is shared
    public final boolean isShared;

    //the queries it belongs to
    private ArrayList<Integer> qids;

    //a hashmap from the query id to the event types, "START", "END", "REGULAR"
    private HashMap<Integer, String> types;

    //the queries that this event type starts
    private ArrayList<Integer> startqids;

    //the queries that this event type ends
    private ArrayList<Integer> endQueries;


    /**
     * constructor
     * @param string the string of the event type
     * @param isShared if this event type is shared
     * @param qid the query id
     */
    public EventType(String string, boolean isShared, int qid){
        this.qids = new ArrayList<Integer>();
        qids.add(qid);
        this.types = new HashMap<Integer, String>();
        this.string = string;
        this.isShared = isShared;
        this.edges = new HashMap<Integer, ArrayList<EventType>>();
        this.startqids = new ArrayList<>();
        this.endQueries = new ArrayList<>();

    }

    /**
     * add an edge to this event type, link it with its predecessor
     * @param qid the query id that this link belongs to
     * @param pred the predecessor event type
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

    /**
     * return the predecessor(not including itself) for a given query
     * @param qid the id of the query
     * @return immediate predecessor
     */

    public EventType getPred(Integer qid){
        int index=0;

        EventType pred = null;

        if (!edges.isEmpty()&&edges.keySet().contains(qid)){

            for (EventType p: edges.get(qid)){

                if (!p.string.equals(this.string)){
                    index = edges.get(qid).indexOf(p);
                    pred = edges.get(qid).get(index);

                }

            }
        }
        return pred;
    }



    /**
     * add the type for a query
     * @param qid query id
     * @param type the type needs to be added
     */
    public void addType(Integer qid, String type){
        types.put(qid, type);
        if (type.equals("START")){
            startqids.add(qid);
        }
        if (!qids.contains(qid)){
            qids.add(qid);
        }
    }

    /**
     * add a query ending with this event type
     * @param qid the query id
     */
    public void addEndQuery(Integer qid){
        this.endQueries.add(qid);

    }
    /**
     * get the event type for a given query id
     * @param qid the query id
     */
    public String getTypebyQid(Integer qid){
        return types.get(qid);
    }


    @Override
    public String toString(){
        StringBuilder strbuilder = new StringBuilder( "   Event type string: "+string+"\n"+"   isShared: "+isShared+"\n");
        for (Integer qid: edges.keySet()){
            strbuilder.append("   qid: "+qid +", type: " +types.get(qid)+ ", pred: ");
            for (EventType p: edges.get(qid)){
                strbuilder.append(p.string+", ");
            }
            strbuilder.append("\n");
        }
        return strbuilder.toString();

    }

}
