package hamlet.query.pattern;

import hamlet.base.DatasetSchema;
import hamlet.base.EventType;
import lombok.Data;

import java.util.ArrayList;
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
    public Pattern(String patternString, DatasetSchema schema){
        this.eventTypes = new ArrayList<>();
        this.patternString = patternString;
        this.kleeneIndex = -1;

        String[] record = patternString.split(",");

        for (int i = 0; i< record.length; i++){

            kleeneIndex = record[i].endsWith("+")? i: kleeneIndex;
            String name = record[i].trim().split("\\+")[0];

            //todo: not checking the event types in the template yet, directly create a new one
            EventType et = new EventType(name, schema, record[i].endsWith("+"));
            this.eventTypes.add(et);

        }
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

    @Override
    public String toString(){
        return eventTypes.size()==1?patternString: "SEQ("+patternString+")";
    }


}
