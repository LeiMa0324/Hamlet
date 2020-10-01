package hamlet.query.predicate;

import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.EventType;
import lombok.Data;

import java.util.ArrayList;

/**
 * Attribute Value comparison predicate
 * a.price > a.price
 */
@Data
public class AttributeComparisonPredicate extends Predicate{

    public AttributeComparisonPredicate(ArrayList<EventType> eventTypes,
                                        String operator,
                                        ArrayList<String> attributeNames,
                                        DatasetSchema schema){
        super(eventTypes, operator, attributeNames, schema);
    }


    protected boolean greater(ArrayList<Event> events){
        int[] indices = getAttributeIndices();
        return (Float)events.get(0).getAttributeValueByName(predAttributes.get(indices[0]).getName()) >
                (Float) events.get(1).getAttributeValueByName(predAttributes.get(indices[1]).getName());
    }

    protected boolean less(ArrayList<Event> events){
        int[] indices = getAttributeIndices();

        return (Float)events.get(0).getAttributeValueByName(predAttributes.get(indices[0]).getName()) <
                (Float) events.get(1).getAttributeValueByName(predAttributes.get(indices[1]).getName());
    }

    protected boolean equal(ArrayList<Event> events){
        int[] indices = getAttributeIndices();
        //String comparison
        if (events.get(0).getAttributeValueByName(predAttributes.get(indices[0]).getName()).getClass()==String.class){
            return events.get(0).getAttributeValueByName(predAttributes.get(indices[0]).getName()).equals(
                    events.get(1).getAttributeValueByName(predAttributes.get(indices[1]).getName()));

            //value comparision
        }else {
            return  ((Float)events.get(0).getAttributeValueByName(predAttributes.get(indices[0]).getName())).equals(
                    (Float)events.get(1).getAttributeValueByName(predAttributes.get(indices[1]).getName()))
                    ;
        }
    }

    /**
     * check how many attributes in there and return the attribute indices
     * @return
     */
    private int[] getAttributeIndices(){
        int i = 0;
        int j = predAttributes.size()>1 ? 1:0;
        return new int[]{i, j};
    }

    private int[] getEventtypeIndices(){
        int i = 0;
        int j = eventTypes.size()>1 ? 1:0;
        return new int[]{i, j};
    }

    @Override
    public String toString(){
        int[] eventTypeIndices = getEventtypeIndices();
        int[] attributeIndices = getAttributeIndices();
        return eventTypes.get(eventTypeIndices[0]).getName()+"."+predAttributes.get(attributeIndices[0]).getName()+operator+
                eventTypes.get(eventTypeIndices[1]).getName()+"."+predAttributes.get(attributeIndices[1]).getName();
    }
}
