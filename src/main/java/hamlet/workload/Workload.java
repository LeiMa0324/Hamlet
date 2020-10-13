package hamlet.workload;

import hamlet.base.DatasetSchema;
import hamlet.base.EventType;
import hamlet.query.Query;
import lombok.Data;

import java.util.ArrayList;

@Data
/**
 * generate the workload
 */
public class Workload {

    private ArrayList<Query> queries;
    private DatasetSchema schema;

    /**
     * the default constructor
     */
    public Workload(DatasetSchema schema){
        this.queries = new ArrayList<>();
        this.schema = schema;
    }

    public Workload(DatasetSchema schema, ArrayList<Query> queries){
        this.queries = queries;
        this.schema = schema;
    }


    /**
     * add a query into the workload
     * for workload Analyzer to create a mini-workload
     * @param query a given query
     */
    public void addQuery(Query query){
        this.queries.add(query);
    }

    /**
     * get all the event types in the workload
     * @return
     */
    public ArrayList<EventType> getAllEventTypes(){
        ArrayList<EventType> eventTypes = new ArrayList<>();

        for (Query q: this.queries){
                for (EventType et: q.getPattern().getEventTypes()){
                    if (eventTypes.contains(et)){
                        continue;
                    }
                    eventTypes.add(et);
                }
        }

        return eventTypes;
    }

    public EventType getEventTypeByName(String etName){
        ArrayList<EventType> eventTypes = getAllEventTypes();
        for (EventType et: eventTypes){
            if (et.getName().equals(etName)){
                return et;
            }
        }
        return null;

    }



}

