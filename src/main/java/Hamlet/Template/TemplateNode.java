package Hamlet.Template;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;

@Data
@AllArgsConstructor
public class TemplateNode {

    private Integer nodeID;
    private HashMap<Integer, EventType> qid_EventType_HashMap;     //Hash Map: {Query id: Hamlet.Event type}
    public boolean isShared;        //if this node is shared

    public TemplateNode(boolean isShared, Integer nodeID){
        this.isShared = isShared;
        this.nodeID = nodeID;
        qid_EventType_HashMap = new HashMap<Integer, EventType>();
    }

    /**
     * add an event type to a node
     * @param qid the query id of the event type
     * @param et  the Hamlet.Event Type instance
     */
    public void addEventType(int qid, EventType et){
        qid_EventType_HashMap.put(qid, et);
        et.setTemplateNode(this);
    }

}