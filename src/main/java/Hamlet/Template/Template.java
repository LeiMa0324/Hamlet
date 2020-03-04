package Hamlet.Template;

import lombok.Data;

import java.util.*;

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
    private ArrayList<String> queries;
    private ArrayList<String> sharedEvents;     //the Shared event types in all queries
    private HashMap<String, EventType> eventTypes;   //can find a Hamlet.Event Type by a string

    public Template(ArrayList<String> queries) {
        this.queries = queries;
        this.eventTypes = new HashMap<>();
        this.sharedEvents = new ArrayList<>();
        int qid = 1;
        findSharedEvents();

        for (String q:queries) {
            List<String> records = Arrays.asList(q.split(","));
            ArrayList<EventType> eventTypeList = new ArrayList<EventType>();    //an eventType list to find the predecessor
            for(int i=0;i<records.size();i++){
                String e = records.get(i).replace("+","");
                String type="";
                type = i==0?"START":"REGULAR";
                type = i==records.size()-1?"END":type;
                if (eventTypeExists(e)){        //if event type exists
                    getEventTypebyString(e).addType(qid,type);      //set its type
                    MaintainPreds(getEventTypebyString(e),qid, eventTypeList);  //maintain its predecessors
                    eventTypeList.add(getEventTypebyString(e));        //maintain event type list

                }else {     //if not exists
                    EventType et = new EventType(e,sharedEvents.contains(e),qid);   //new an event type
                    et.addType(qid, type);          //set its type
                    MaintainPreds(et, qid, eventTypeList);   //maintain its predecessors
                    eventTypes.put(e, et);       //put it into hash map
                    eventTypeList.add(et);       //maintain event type list
                }
            }
            qid++;
        }
        }

    /**
     * find the predecessor of an event type
     * @param et a given event type
     * @param qid   a query id
     * @param eventTypeList the event type list needed to be search
     */
        private void MaintainPreds(EventType et, Integer qid,  ArrayList<EventType> eventTypeList){
            if (et.isShared()){     //if et is a shared event type, add itself as a predecessor
                et.addEdges(qid,et);
            }

            if (!et.getTypebyQid(qid).equals("START")){    // if e is not a start event type, find it's immediate predecessor
                et.addEdges(qid,eventTypeList.get(eventTypeList.size()-1));    // add the immediate predecessor into the edge list
            }

        }

    public EventType getEventTypebyString(String string){
        return this.eventTypes.get(string);

    }

    public boolean eventTypeExists(String etString){
        return eventTypes.keySet().contains(etString);
    }

    /**
     *Find the shared events(with Kleene plus) of several queries
     */
    private void findSharedEvents(){
        String firstQuery = queries.get(0);
        String[] events_firstQuery = firstQuery.split(",");
        ArrayList<String> candidates = new ArrayList<>();
        for (String e: events_firstQuery){  //如果第一个字符串含有+，则加入candidates
            if (e.contains("+")){
                candidates.add(e);
            }
        }
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
