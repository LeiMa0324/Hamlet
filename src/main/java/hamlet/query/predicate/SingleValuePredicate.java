package hamlet.query.predicate;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.EventType;

import java.util.ArrayList;

/**
 * the local predicate
 */
public class SingleValuePredicate extends Predicate{

    //the condition value
    //could be a number or a string
    private Object value;
    private Attribute singleAttribute;

    public SingleValuePredicate(ArrayList<EventType> eventTypes,
                                String operator,
                                ArrayList<String> attributeNames,
                                DatasetSchema schema,
                                Float value){

        super(eventTypes, operator, attributeNames, schema);
        this.value = value;
        this.singleAttribute = this.predAttributes.get(0);
    }

    protected boolean greater(ArrayList<Event> events){
        Event event = events.get(0);

        return (Float)event.getAttributeValueByName(singleAttribute.getName())>(Float) value;

    }

    protected boolean less(ArrayList<Event> events){
        Event event = events.get(0);

        return (Float)event.getAttributeValueByName(singleAttribute.getName())<(Float) value;

    }

    protected boolean equal(ArrayList<Event> events){
        Event event = events.get(0);

        if (value.getClass()==String.class){
            return event.getAttributeValueByName(singleAttribute.getName()).equals(value);
        }else {
            return event.getAttributeValueByName(singleAttribute.getName())==value;
        }
    }

    public String toString(){
        return singleAttribute.getName()+ this.operator + value;
    }
}
