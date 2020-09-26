package hamlet.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Template reads the query files and
 * 1. find the shared event types in all queries, mark them as shared
 * 2. generate an Event Type instance when meets a new Event Type
 * 3. for event type et:
 *      for each query et is in:
 *          3.1 Maintain its predecessors by looking at the previous event type in the query, if with +, then it's self-predecessor
 *          3.2 maintain its type
 */
@Data
public class Template {

    //all the queries in the Workload
    private ArrayList<String> queries;

    //the Shared event types in all queries
    private ArrayList<String> sharedEvents;

    //a hashmap from a event string to an event type
    private HashMap<String, EventType> eventTypes;

    //the start events in all queries
    private ArrayList<String> startEvents;

    /**
     * read the queries, creating event types and building the connections between event types
     * @param queries the queries in the Workload
     */
    public Template(ArrayList<String> queries) {
        this.queries = queries;
        this.eventTypes = new HashMap<>();
        this.sharedEvents = new ArrayList<>();
        this.startEvents = new ArrayList<>();

        //qid starts with 1
        int qid = 1;

        //find the sharable event types
        findSharedEvents();

        //for each query
        for (String q:queries) {

            // a sequence of event types in a query
            List<String> records = Arrays.asList(q.split(","));

            //an temporary vendorID list to find the predecessor
            ArrayList<EventType> eventTypeList = new ArrayList<EventType>();

            // iterate over all event types
            for(int i=0;i<records.size();i++){
                String e = records.get(i).replace("+","");
                String type="";

                //if length is 1, it's both START and END type
                if (i==0&&records.size()==1){
                    type = "START|END";

                    // add the event type into start event list
                    startEvents.add(e);

                }

                //decide the type for other cases
                else {
                    type = i == 0 ? "START" : "REGULAR";
                    if (type.equals("START")) {
                        startEvents.add(e);
                    }

                    type = i == records.size() - 1 ? "END" : type;
                }

                //if event type exists
                if (eventTypeExists(e)){
                    //set its type
                    EventType et = getEventTypebyString(e);

                    //set end queries
                    et.addType(qid,type);
                    if (type.equals("END")||type.equals("START|END")){
                        et.addEndQuery(qid);

                    }

                    //maintain its predecessors
                    et = maintainPreds(et,qid, eventTypeList);

                    //maintain event type list
                    eventTypeList.add(et);
                    eventTypes.put(e, et);

                //if the event type doesn not exist
                }else {
                    //new an event type
                    EventType et = new EventType(e,sharedEvents.contains(e),qid);
                    //set its type
                    et.addType(qid, type);
                    //set end queries
                    if (type.equals("END")||type.equals("START|END")){
                        et.addEndQuery(qid);

                    }
                    //maintain its predecessors
                    maintainPreds(et, qid, eventTypeList);

                    //put it into hash map
                    eventTypes.put(e, et);

                    //maintain event type list
                    eventTypeList.add(et);
                }
            }
            qid++;
        }

        }

    /**
     * find the predecessor of an event type
     * @param et a given event type
     * @param qid   a query id
     * @param eventTypeList the event type list needed to be searched
     */
        private EventType maintainPreds(EventType et, Integer qid, ArrayList<EventType> eventTypeList){
            if (et.isShared()){     //if et is a shared event type, add itself as a predecessor
                et.addEdges(qid,et);
            }

            if (!et.getTypebyQid(qid).equals("START")&&(!et.getTypebyQid(qid).equals("START|END"))){    // if e is not a start event type, find it's immediate predecessor
                et.addEdges(qid,eventTypeList.get(eventTypeList.size()-1));    // add the immediate predecessor into the edge list
            }
            return et;

        }

    /**
     * get the event type by an event string
     * @param string the event string
     * @return a event type of this string
     */
    public EventType getEventTypebyString(String string){
        return this.eventTypes.get(string);

    }

    /**
     * check if an event type of string exists in the template
     * @param String the event string
     * @return true or false
     */
    public boolean eventTypeExists(String String){
        return eventTypes.keySet().contains(String);
    }

    /**
     *Find the shared events(with Kleene plus) of several queries
     */
    private void findSharedEvents(){
        String firstQuery = queries.get(0);
        String[] events_firstQuery = firstQuery.split(",");

        //a candidate list of shared event types
        ArrayList<String> candidates = new ArrayList<>();

        //add a candidate if it has +
        for (String e: events_firstQuery){
            if (e.contains("+")){
                candidates.add(e);
            }
        }

        //if this candidate is contained by all queries
        for (String c: candidates){
            boolean shared= true;
            for (String q: queries){
            String[] events = q.split(",");
                if (!Arrays.asList(events).contains(c)){
                    shared=false;
                }
            }
            if (shared){
                this.sharedEvents.add(c.replace("+",""));
            }
        }

    }

}
