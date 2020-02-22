package HamletTemplate;

import Event.Event;
import lombok.Data;

import java.util.*;

/**
 * Template is consisted of a list of Template Nodes
 */
@Data
public class Template {
    private ArrayList<String> queries;
    private ArrayList<TemplateNode> NodeList;   //all Nodes in the Template
    private String SharedEvent;     //the Shared event type string common in all queries
    private HashMap<String, EventType> strToEventTypeHashMap;   //can find a Event Type by a string

    //
    public Template(ArrayList<String> queries) {
        this.queries = queries;
        NodeList = new ArrayList<TemplateNode>();
        strToEventTypeHashMap = new HashMap<String, EventType>();
        int qid = 1;
        SharedEvent = FindSharedEvents();

        for (String q:queries) {
            List<String> events = Arrays.asList(q.split(","));
            ArrayList<EventType> eventTypeList = new ArrayList<EventType>();    //an eventType list to find the predecessor
            for(int i=0;i<events.size();i++){
                EventType et = new EventType(qid, events.get(i));
                //only add predecessor for shared event type
                // TODO: 2020/2/21 only shared event has predecessor here.
                if (events.get(i).equals(SharedEvent))
                {
                    et.addpredEventTypes(eventTypeList.get(i-1));    //only has the immediate predecessor
                    et.setSelfPred(true);
                }
                eventTypeList.add(et);

                if (NodeList.size()>i){
                    NodeList.get(i).addEventType(qid, et);
                }
                else {
                    NodeList.add(new TemplateNode(events.get(i).equals(SharedEvent),i));
                    NodeList.get(i).addEventType(qid, et);
                }
                //maintain string to event type Hash Map
                strToEventTypeHashMap.put(et.getEventTypeStr(),et);
            }
            qid++;
        }
        }

    /**
     *Find the shared part(with Kleenes) of several queries
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
        return hasCommonEvent?CommonEvent:null;

    }


}
