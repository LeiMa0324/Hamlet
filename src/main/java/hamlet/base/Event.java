package hamlet.base;

import lombok.Data;

import java.math.BigInteger;

/**
 * the event class
 */
@Data
public class Event {

    private EventType type;
    private Object[] tuple;
    private long timeStamp;
    private BigInteger count;

    public Event(EventType type, Object[] tuple){
        this.type = type;
        this.tuple = tuple;
        this.timeStamp = System.currentTimeMillis();
        this.count =  BigInteger.ZERO;

    }

    /**
     * find the index of an attribute
     * @param attributeName the name of an attribute
     * @return the index
     */
    private int findAttributeIndexByName(String attributeName){
        for (int i=0; i< type.getSchema().getAttributes().size(); i++){
            if (type.getSchema().getAttributes().get(i).getName().equals(attributeName)){
                return i;
            }
        }
        System.out.printf("Cannot find the attribute "+attributeName);
        return  -1;
    }

    /**
     * get the value of a given attribute
     * @param attributeName a given attribute
     * @return the value
     */
    public Object getAttributeValueByName(String attributeName){
        int index = this.findAttributeIndexByName(attributeName);

        return index == -1? null: this.tuple[index];

    }
}
