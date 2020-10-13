package hamlet.executor;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.base.Template;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class PredecessorManager {

    private ArrayList<Event> events;
    private Template template;
    private SnapshotManager snapshotManager;

    public PredecessorManager(ArrayList<Event> events,
                              Template template,
                              SnapshotManager snapshotManager){
        this.events = events;
        this.template = template;
        this.snapshotManager = snapshotManager;

    }

    /**
     * check if one event have different predecessors for different queries
     * if have, then create a new event-level snapshot
     * @return
     */
    public boolean hasSamePredecessorsForValidQueries(Event event){
        if (event.getValidQueries().isEmpty()){
            System.out.printf("");
        }
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

        int lastGraphletSnapshotEventIndex = this.getSnapshotManager().getLastGraphletSnapshot()==null?
                -1:
                this.getSnapshotManager().getLastGraphletSnapshot().getEventIndex();

        ArrayList<Event> eventsBetweenSnapshots = lastGraphletSnapshotEventIndex+1>this.events.indexOf(event)?
                new ArrayList<Event>():
                new ArrayList<>(this.events.subList(lastGraphletSnapshotEventIndex+1, this.events.indexOf(event)));

        //preceding event types
        ArrayList<EventType> predETs = this.template.getAllPredecessorsByEventTypeAndQueryId(event.getType(), qid);

        ArrayList<Event> preds = new ArrayList<>();

        if (!eventsBetweenSnapshots.isEmpty()) {
            Event.Metric metric = eventsBetweenSnapshots.get(0).getMetric();
            for (Event e : eventsBetweenSnapshots) {
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
    public HashMap<Integer, BigInteger> sumPrefixEventCountsForKleeneEventTypeForAllQueries(EventType eventType,
                                                                                               HashMap<EventType,ArrayList<Graphlet>> noneKleeneGraphlets){
        //counts
        HashMap<Integer, BigInteger> prefixCounts = new HashMap<>();
        //all the queries
        ArrayList<Integer> queries = eventType.getQueries();


        //find and sum the prefix after last shared graphlet
        for (Integer qid: queries){
            EventType predET = this.template.getNoneKleenePredecessorByEventTypeAndQueryId(eventType, qid);
            BigInteger countForQuery = BigInteger.ZERO;
            if (predET!=null){
                if (noneKleeneGraphlets.containsKey(predET)){
                    for (Graphlet g: noneKleeneGraphlets.get(predET)){
                        countForQuery = countForQuery.add(g.getTotalCount().get(qid));
                    }
                }
            }


            prefixCounts.put(qid, countForQuery);

        }

        return prefixCounts;
    }


}


