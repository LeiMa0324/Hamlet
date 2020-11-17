package hamlet.Graph.tools;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.Graph.Graphlet.Graphlet;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class PredecessorManager {

    public PredecessorManager(){}

    /**
     * check if one event have different predecessors for different queries
     * if have, then create a new event-level snapshot
     * @return
     */
    public boolean hasSamePredecessorsForValidQueries(Event event){

        ArrayList<Event> predsForFirstQuery = getPredecessorsAfterLastGraphletSnapshotForQuery(event, event.getValidQueries().get(0));

        for (Integer qid: event.getValidQueries()){
            if (!predsForFirstQuery.equals(getPredecessorsAfterLastGraphletSnapshotForQuery(event, qid))){
                return false;
            }
        }
        return true;
    }

    /**
     * get the predecessors in a certain range of stream
     * @param event

     * @return
     */
    public ArrayList<Event> getPredecessorsAfterLastGraphletSnapshotForQuery(Event event,
                                                                             int qid){

        int lastGraphletSnapshotEventIndex = Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot()==null?
                0:
                Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot().getEventIndex();

        ArrayList<Event> eventsAfterSnapshot = lastGraphletSnapshotEventIndex>Utils.getInstance().getEvents().indexOf(event)?
                new ArrayList<Event>():
                new ArrayList<>(Utils.getInstance().getEvents().subList(lastGraphletSnapshotEventIndex, Utils.getInstance().getEvents().indexOf(event)));

        //preceding event types
        ArrayList<EventType> predETs = Utils.getInstance().getTemplate().getAllPredecessorsByEventTypeAndQueryId(event.getType(), qid);

        ArrayList<Event> preds = new ArrayList<>();

        if (!eventsAfterSnapshot.isEmpty()) {
            Event.Metric metric = eventsAfterSnapshot.get(0).getMetric();
            for (Event e : eventsAfterSnapshot) {
                //find the events that are:
                //1. the preceding type
                //2. valid for this query
                if (predETs.contains(e.getType()) && e.getValidQueries().contains(qid)) {

                    if (e.getMetric() == metric) {
                        preds.add(e);

                    } else {
                        System.out.printf("Have predecessors with different metrics!\n");
                        System.out.printf("Qid: " + qid + "\n");
                    }
                }
            }
        }
        return preds;

    }



    /**
     * count all the prefix predecessors between two shared graphlets, add with the old snapshot to create a new graphlet snapshot
     * @return
     */
    public HashMap<Integer, Value> sumPrefixEventValuesForKleeneEventTypeForAllQueries(EventType eventType,
                                                                                       HashMap<EventType,ArrayList<Graphlet>> noneKleeneGraphlets){
        //counts
        HashMap<Integer, Value> prefixValues = new HashMap<>();
        //all the queries
        ArrayList<Integer> queries = eventType.getQueries();


        //find and sum the prefix after last shared graphlet
        for (Integer qid: queries){
            EventType predET = Utils.getInstance().getTemplate().getNoneKleenePredecessorByEventTypeAndQueryId(eventType, qid);
            Value countForQuery = Value.ZERO;
            if (predET!=null){
                if (noneKleeneGraphlets.containsKey(predET)){
                    for (Graphlet g: noneKleeneGraphlets.get(predET)){
                        countForQuery = countForQuery.add(g.getGraphletValues().get(qid));
                    }
                }
            }
            prefixValues.put(qid, countForQuery);
        }

        return prefixValues;
    }
}


