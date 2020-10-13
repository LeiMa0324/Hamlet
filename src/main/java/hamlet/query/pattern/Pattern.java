package hamlet.query.pattern;

import hamlet.base.DatasetSchema;
import hamlet.base.EventType;
import lombok.Data;

import java.util.ArrayList;
import java.util.Set;

@Data
public class Pattern {

    private ArrayList<EventType> eventTypes;
    private int kleeneIndex;
    private String patternString;


    /**
     * read in a patternString and transform it into a pattern
     * @param patternString a pattern string
     * @param schema schema of a dataset
     */
    public Pattern(String patternString, DatasetSchema schema, Set<EventType> existedEventTypes){
        this.eventTypes = new ArrayList<>();
        this.patternString = patternString;
        this.kleeneIndex = -1;

        String[] record = patternString.split(",");

        for (int i = 0; i< record.length; i++){

            kleeneIndex = record[i].endsWith("+")? i: kleeneIndex;
            String name = record[i].trim().split("\\+")[0];

            EventType et = createEventType(name, schema, record[i].endsWith("+"), existedEventTypes);
            this.eventTypes.add(et);

        }
    }

    public EventType createEventType(String etName,
                                     DatasetSchema schema,
                                     boolean isKleene,
                                     Set<EventType> existedEventTypes){
        for (EventType eventType: existedEventTypes){
            if (eventType.getName().equals(etName)){
                return eventType;
            }
        }
        return new EventType(etName, schema, isKleene);
    }

    public EventType getKleeneEventType(){
        return eventTypes.get(this.kleeneIndex);
    }

    public EventType getEventTypeByName(String name){
        for (EventType et: eventTypes){
            if (et.getName().equals(name)){
                return et;
            }
        }
        return null;
    }

    public EventType getNoneKleenePredecessor(EventType eventType){
        int currentIndex = this.eventTypes.indexOf(eventType);
        return currentIndex==0?null: this.eventTypes.get(currentIndex-1);
    }


    public ArrayList<EventType> getAllPredecessors(EventType eventType){
        ArrayList<EventType> preds = new ArrayList<>();
        if (eventType.isKleene()){
            preds.add(eventType);
        }
        preds.add(getNoneKleenePredecessor(eventType));
        return preds;
    }

    @Override
    public String toString(){
        return eventTypes.size()==1?patternString: "SEQ("+patternString+")";
    }


}
