package hamlet.executor;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.executor.countManager.KeeneEventCountManager;
import hamlet.executor.countManager.NoneKleeneEventCountManager;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class Graphlet {
    private NoneKleeneEventCountManager noneKleeneEventCountManager;
    private KeeneEventCountManager kleeneEventCountManager;
    private ArrayList<Event> events;
    private GraphletType type;
    private EventType eventType;
    private HashMap<Integer, BigInteger> totalCount; //the sum of counts of all the events
    private ArrayList<Integer> snapshotIds;
    private HashMap<Integer, BigInteger> snapshotToCoeffs;
    private Event.Metric metric;
    private ArrayList<Integer> allqids;

    public enum GraphletType {
        Kleene,
        NoneKleene
    }

    public Graphlet(ArrayList<Integer> allqids) {
        this.events = new ArrayList<>();
        this.allqids = allqids;
    }

    public Graphlet(ArrayList<Event> events,
                    NoneKleeneEventCountManager noneKleeneEventCountManager,
                    KeeneEventCountManager keeneEventCountManager,
                    ArrayList<Integer> allqids) {
        this.events = events;
        this.type = this.events.get(0).getType().isKleene() ? GraphletType.Kleene : GraphletType.NoneKleene;
        this.eventType = events.get(0).getType();
        this.snapshotToCoeffs = new HashMap<>();

        this.noneKleeneEventCountManager = noneKleeneEventCountManager;
        this.kleeneEventCountManager = keeneEventCountManager;
        this.totalCount = new HashMap<>();
        this.allqids = allqids;


    }

    /**
     * propagate the count or snapshots in the graphlet
     */
    public void propagate() {

        if (type == GraphletType.Kleene) {
            kleeneGraphletPropagate();

        } else {
            noneKleeneEventCountManager.update(events.get(0));
            Event.Metric metric = events.get(0).getMetric();

            //prefix graphlet graphlet.count = size *count
            if (metric == Event.Metric.COUNT) {
                getGraphletCounts();
            }

            //suffix graphlet graphlet.coeff = size* coeff
            if (metric == Event.Metric.SNAPSHOT) {
                getGraphletSnapshots();
            }

            this.metric = metric;

        }
        printGraphletInfo();

    }

    private void kleeneGraphletPropagate() {
        for (Event event : this.events) {
            kleeneEventCountManager.update(event);
            updateTotalCountByEvent(event);
        }
    }

    private void getGraphletCounts(){
        for (Integer qid : events.get(0).getValidQueries()) {
            this.totalCount.put(qid, events.get(0).getCounts().get(qid).multiply(
                    new BigInteger(this.events.size() + "")));
        }
    }

    private void getGraphletSnapshots(){
        this.snapshotIds = this.events.get(0).getSnapshotIds();
        for (Integer snapid : this.snapshotIds) {
            this.snapshotToCoeffs.put(snapid, this.events.get(0).getSnapIdTocoeffs().get(snapid).multiply(
                    new BigInteger(this.events.size() + "")
            ));
        }
        for (int qid: this.allqids) {
            BigInteger count = kleeneEventCountManager.getPredecessorManager().getSnapshotManager().evaluateSnapshotExpressionForQuery(this.snapshotToCoeffs,qid);
            this.totalCount.put(qid, count);

        }
    }

    /**
     * update the total count by event and query
     * @param event

     */
    private void updateTotalCountByEvent(Event event){

        for (int qid: allqids) {
            SnapshotManager snapshotManager = this.kleeneEventCountManager.getPredecessorManager().getSnapshotManager();
            BigInteger countForQuery = snapshotManager.evaluateSnapshotExpressionForQuery(event.getSnapIdTocoeffs(), qid);
            BigInteger newGpraghletCountForQuery = this.totalCount.containsKey(qid) ? this.totalCount.get(qid) : BigInteger.ZERO;
            newGpraghletCountForQuery = newGpraghletCountForQuery.add(countForQuery);

            this.totalCount.put(qid, newGpraghletCountForQuery);
        }
    }

    private void printGraphletInfo(){
        System.out.printf("\n\n=========Graphlet Info=======\n");

        System.out.printf("Graphlet finished!\n");
        System.out.printf("Event type: "+this.eventType.getName()+"\n");
        System.out.printf("number of events:" +this.events.size()+"\n");
        for (int qid: this.totalCount.keySet()){
            System.out.printf("query "+qid);
            System.out.printf("     count: "+totalCount.get(qid)+"\n");
        }
        System.out.printf("=========End Graphlet Info=======\n\n");

    }
}


