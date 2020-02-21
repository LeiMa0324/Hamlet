package NewTemplate;

import lombok.AllArgsConstructor;
import lombok.Data;
import template.Template;

import java.util.HashMap;

@Data
@AllArgsConstructor
public class TemplateNode {
    //Query id: Event type
    private HashMap<Integer, EventType> qid_EventType_HashMap;
    private boolean isShared;

    public TemplateNode(boolean isShared){
        this.isShared = isShared;
        qid_EventType_HashMap = new HashMap<Integer, EventType>();
    }

    public void addEventType(int qid, EventType et){
        qid_EventType_HashMap.put(qid, et);
    }

}
