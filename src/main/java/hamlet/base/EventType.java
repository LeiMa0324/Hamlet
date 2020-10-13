package hamlet.base;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * an event type
 * contains the event type name, a list of attributes
 */
@Data
public class EventType {
    private final String name;
    private final DatasetSchema schema;
    private boolean isKleene;
    private HashMap<Integer, Type> posType;

    public EventType(String name, DatasetSchema schema, boolean isKleene){
        this.name = name;
        this.schema = schema;
        this.isKleene = isKleene;
        this.posType = new HashMap<>();
    }

    public boolean equals(EventType eventType){
        return this.name.equals(eventType.name)&&(this.schema.equals(eventType.schema));
    }

    public enum Type{
        START,
        END,
        STARTANDEND,
        OTHER,
    }

    public ArrayList<Integer> getQueriesStartWith(){
        ArrayList<Integer> qids = new ArrayList<>();
        for (Integer qid: posType.keySet()){
            if (posType.get(qid)==Type.START || posType.get(qid)==Type.STARTANDEND){
                qids.add(qid);
            }
        }
        return qids;
    }

    public ArrayList<Integer> getQueriesEndWith(){
        ArrayList<Integer> qids = new ArrayList<>();
        for (Integer qid: posType.keySet()){
            if (posType.get(qid)==Type.END || posType.get(qid)==Type.STARTANDEND){
                qids.add(qid);
            }
        }
        return qids;

    }

    public ArrayList<Integer> getQueries(){
        return new ArrayList<>(this.posType.keySet());
    }


}
