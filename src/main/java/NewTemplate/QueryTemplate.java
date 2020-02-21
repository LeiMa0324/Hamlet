package NewTemplate;

import lombok.Data;
import template.MultiQueryType;

import java.sql.Struct;
import java.util.*;

/**
 * query of length of two
 */
@Data
public class QueryTemplate {
    private ArrayList<String> queries;
    private ArrayList<TemplateNode> NodeList;
    private String SharedEvent;

    //
    public QueryTemplate(ArrayList<String> queries) {
        this.queries = queries;
        NodeList = new ArrayList<TemplateNode>();
        int qid = 1;	//每一个query都有一个qid
        SharedEvent = FindShareEvents();

        for (String q:queries) {
            List<String> events = Arrays.asList(q.split(","));
            //保存一个query中所有的event types
            ArrayList<EventType> eventTypeList = new ArrayList<EventType>();
            for(int i=0;i<events.size();i++){
                EventType et = new EventType(qid, events.get(i));
                //only add predecessor for shared event type
                if (events.get(i).equals(SharedEvent))
                {
                    ArrayList<EventType> deepCopy = new ArrayList<EventType>();
                    deepCopy.addAll(eventTypeList);
                    et.setPredEventTypes(deepCopy);
                    et.setSelfPred(true);
                }
                eventTypeList.add(et);  //将et加入event list中，好找到predecessor
                //如果nodelist有了该node
                if (NodeList.size()>i){
                    NodeList.get(i).addEventType(qid, et);
                }
                else {
                    NodeList.add(new TemplateNode(events.get(i).equals(SharedEvent)));
                    NodeList.get(i).addEventType(qid, et);
                }
            }
            qid++;
        }
        }

    /**
     *Find the shared part(with Kleenes) of several queries
     * @return the shared event
     */
    public String FindShareEvents(){
        //找到kleene，确保其他query也有
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
