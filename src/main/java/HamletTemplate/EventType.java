package HamletTemplate;

import lombok.Data;

import java.util.ArrayList;

/**
 * store the event type info
 */
@Data
public class EventType {
    private Integer qid;    //which query it belongs to
    private String EventTypeStr;    // the actual event type string
    private ArrayList<EventType> predEventTypes;    // the predecessor event type
    private boolean selfPred;
    private TemplateNode templateNode;  //the templateNode this event type belongs to

    public EventType(Integer qid, String EventTypeStr){
        this.predEventTypes = new ArrayList<EventType>();
        this.qid = qid;
        this.EventTypeStr = EventTypeStr;
    }

    public void addpredEventTypes(EventType et){
        this.predEventTypes.add(et);
    }

    @Override
    public String toString(){
        return "qid:"+this.qid+". Event: "+EventTypeStr+". Pred Event Types:"+predEventTypes +". Self Pred: "+selfPred
                +". TemplateNode:"+templateNode.getNodeID();
    }

}
