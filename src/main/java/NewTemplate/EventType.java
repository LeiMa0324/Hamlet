package NewTemplate;

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

    public EventType(Integer qid, String EventTypeStr){
        this.qid = qid;
        this.EventTypeStr = EventTypeStr;
    }

    public void addpredEventTypes(EventType et){
        this.predEventTypes.add(et);
    }


}
