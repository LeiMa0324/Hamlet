package hamlet.stream;

import hamlet.base.Event;
import hamlet.query.GroupBy;
import hamlet.workload.Workload;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * partition the stream into sub streams for each mini-workload
 */
@Data
public class streamPartitioner {

    private Workload miniWorkload;
    private ArrayList<Event> events;
    private GroupBy groupBy;
    private HashMap<Object, ArrayList<Event>> subStreams;

    public streamPartitioner(Workload miniWorkload, ArrayList<Event> events){
        this.miniWorkload = miniWorkload;
        this.events = events;
        this.groupBy = miniWorkload.getQueries().get(0).getGroupBy();
        ArrayList<Object> groupValues = findAllGroupByValues();
        //initialize the hashmap by keys
        subStreams = new HashMap<>();
        for (Object v: groupValues){
            this.subStreams.put(v, new ArrayList<Event>());
        }
    }

    /**
     * partition the stream into substreams according to the groupby
     * @return a hashmap of attrValue and substream
     */
    public HashMap<Object, ArrayList<Event>> partition(){


        for (Event e: events){
            // if e is not relevant in this mini-workload
            if (miniWorkload.getEventTypeByName(e.getType().getName())==null){
                continue;
            }
            //if e is a relevant event but not group-by event
            if (!e.getType().equals(groupBy.getEventType())){
                addCommonEventIntoAllSubStreams(e);

            }else {
                //if it's a group-by event
                addGroupByEventIntoSubStream(e);
            }
        }
        return this.subStreams;
    }

    /**
     * scan the stream to find all group-by values
     * @return list of values
     */
    private ArrayList<Object> findAllGroupByValues(){
        ArrayList<Object> values = new ArrayList<>();
        for (Event e: this.events){
            if (e.getType().equals(groupBy.getEventType())&&
                    !(values.contains(e.getAttributeValueByName(groupBy.getAttributeName())))){
                values.add(e.getAttributeValueByName(groupBy.getAttributeName()));
            }
        }
        return values;
    }

    /**
     * add a common event into all substreams
     * @param e a common event
     */
    private void addCommonEventIntoAllSubStreams(Event e){
        for (Object k: this.subStreams.keySet()){
            ArrayList<Event> tmp = subStreams.get(k);
            tmp.add(e);
            subStreams.put(k, tmp);
        }

    }

    /**
     * add a group-by event into a specific sub stream
     * @param e
     */
    private void addGroupByEventIntoSubStream(Event e){
        Object k = e.getAttributeValueByName(groupBy.getAttributeName());
        ArrayList<Event> tmp = this.subStreams.get(k);
        tmp.add(e);
        this.subStreams.put(k, tmp);
    }

}
