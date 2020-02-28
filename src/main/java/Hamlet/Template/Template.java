package Hamlet.Template;

import lombok.Data;

import java.util.*;

/**
 * Template is consisted of a list of Template Nodes
 */
@Data
public class Template {
    private ArrayList<String> queries;
    private String SharedEvent;     //the Shared event type string common in all queries
    private HashMap<String, EventType> strToEventTypeHashMap;   //can find a Hamlet.Event Type by a string

    //
    public Template(ArrayList<String> queries) {
        this.queries = queries;
        strToEventTypeHashMap = new HashMap<String, EventType>();
        int qid = 1;
        SharedEvent = FindSharedEvents();

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
                    EventType et = new EventType(e,e.equals(FindSharedEvents()),qid);   //new an event type
                    et.addType(qid, type);          //set its type
                    MaintainPreds(et, qid, eventTypeList);   //maintain its predecessors
                    strToEventTypeHashMap.put(e, et);       //put it into hash map
                    eventTypeList.add(et);       //maintain event type list
                }
            }
            qid++;
        }
        }

        private void MaintainPreds(EventType et, Integer qid,  ArrayList<EventType> eventTypeList){
            if (et.isShared()){     //if et is a shared event type, add itself as a predecessor
                et.addEdges(qid,et);
            }

            if (!et.getTypebyQid(qid).equals("START")){    // if e is not a start event type, find it's immediate predecessor
                et.addEdges(qid,eventTypeList.get(eventTypeList.size()-1));    // add the immediate predecessor into the edge list
            }

        }

    public EventType getEventTypebyString(String string){
        return this.strToEventTypeHashMap.get(string);

    }

    public boolean eventTypeExists(String etString){
        return strToEventTypeHashMap.keySet().contains(etString);
    }

    // TODO: 2020/2/27 自动检查substring而不是character 

    /**
     *Find the shared part(with Kleene plus) of several queries
     * @return the shared event
     */
    private String FindSharedEvents(){
        String firstQuery = queries.get(0);
        String[] events_firstQuery = firstQuery.split(",");
        String CommonEvent = null;
        for (String e: events_firstQuery){
            if (e.contains("+")){
                CommonEvent = e;
            }
        }
        boolean hasCommonEvent = true;
        for (String q: queries){
            String[] events = q.split(",");
            if (!Arrays.asList(events).contains(CommonEvent)){
                hasCommonEvent = false;
            }
        }
        return hasCommonEvent?CommonEvent.replace("+",""):null;

    }



}
