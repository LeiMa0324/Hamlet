package hamlet.query.predicate;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.EventType;
import lombok.Data;

import java.util.ArrayList;

/**
 * the class of predicate
 */
@Data
public abstract class Predicate {
    protected ArrayList<EventType> eventTypes;
    protected String operator;
    protected ArrayList<Attribute> predAttributes;

    public Predicate(){}

    public Predicate(ArrayList<EventType> eventTypes, String operator, ArrayList<String> attributeNames,
                     DatasetSchema schema){

        this.eventTypes = eventTypes;
        this.operator = operator;
        this.predAttributes = new ArrayList<>();
        for (String attrName: attributeNames){
            predAttributes.add(schema.getAttributeByName(attrName));
        }
    }

    /**
     * verify if an event satisfy the predicate
     * @param events
     * @return
     */

    public boolean verify(ArrayList<Event> events){
        switch (operator){
            case ">":
                greater(events);
                break;
            case "=":
                equal(events);
                break;
            case "<":
                less(events);
                break;
        }

        return false;
    };
    protected abstract boolean greater(ArrayList<Event> events);
    protected abstract boolean equal(ArrayList<Event> events);
    protected abstract boolean less(ArrayList<Event> events);
    public abstract String toString();

}
