package hamlet.base;

import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * the event class
 */
@Data
public class Event {

    private EventType type;
    private Object[] tuple;
    private long timeStamp;
    private ArrayList<Integer> validQueries;

    // each query could have different list of snapshots
    // <snapshotId1, snapshotId2...>
    private ArrayList<Integer> snapshotIds;
    //<qid: <snapshot id, coeff>>

    private  HashMap<Integer, BigInteger> snapIdTocoeffs;
    private HashMap<Integer, Value> values;
    private Metric metric;
    private Integer eventIndex;
    private boolean hasSnapshot= false;

    public Event(EventType type, Object[] tuple) throws ParseException {

        SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyyMMddhhmm");//如2016-08-10 20:40

        this.type = type;
        this.tuple = tuple;
        this.timeStamp = simpleFormat.parse((String)tuple[1]).getTime();
        this.validQueries = new ArrayList<>();
        this.snapshotIds =  new ArrayList<>();
        this.snapIdTocoeffs = new HashMap<>();
        this.values = new HashMap<>();

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


    /**
     * after evaluation, set the event into a independent event-level snapshot
     * @param snapshotId
     */
    public void setSingleSnapshot(Integer snapshotId){
        this.snapshotIds.clear();
        this.snapIdTocoeffs.clear();

        this.snapshotIds.add(snapshotId);
        this.snapIdTocoeffs.put(snapshotId, BigInteger.ONE);
    }


    public enum Metric{
        SNAPSHOT,
        VAL
    }
}
