package hamlet.executor.countManager;

import hamlet.base.Event;
import hamlet.executor.PredecessorManager;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class KeeneEventCountManager extends EventCountManager {

    public KeeneEventCountManager(PredecessorManager predecessorManager){
        super(predecessorManager);
    }

    //todo: such a mess
    public void update(Event event){
        ArrayList<Integer> startQueries = event.getType().getQueriesStartWith();
        ArrayList<Integer> endQueries = event.getType().getQueriesEndWith();

        //update start events

        for (int qid: startQueries){
            //B+ with start type process
            updateStartEventForQuery(event, qid);
        }

        //check if has the same predecessors for all the queries
        //if does, got the coeff and update the event.snapshot list and coeff hashmap
        if (this.predecessorManager.hasSamePredecessorsForValidQueries(event)){
            ArrayList<Event> preds = this.predecessorManager.getPredecessorsAfterLastGraphletSnapshotForQuery(event, event.getValidQueries().get(0));
            HashMap<Integer, BigInteger> coeffsum = new HashMap<>();

            if (!preds.isEmpty()){
                coeffsum = sumPredSnapshots(event, preds);
                event.setSnapshotIds(new ArrayList<Integer>(coeffsum.keySet()));
                for (Integer snapid: coeffsum.keySet()){
                    BigInteger coeff = coeffsum.get(snapid).add(BigInteger.ONE);
                    coeffsum.put(snapid, coeff);
                }
                event.setSnapIdTocoeffs(coeffsum);
                event.setMetric(Event.Metric.SNAPSHOT);
            }else {
                ArrayList<Integer> snapshotIds = new ArrayList<>();
                //the last graphlet snapshot
                snapshotIds.add(this.predecessorManager.getSnapshotManager().getSnapshots().indexOf(
                        this.predecessorManager.getSnapshotManager().getLastGraphletSnapshot()));
                //coeff = 1
                coeffsum.put(snapshotIds.get(0), BigInteger.ONE);

                event.setSnapshotIds(snapshotIds);
                event.setSnapIdTocoeffs(coeffsum);
                event.setMetric(Event.Metric.SNAPSHOT);

            }

        }else {
            HashMap<Integer, BigInteger> counts = new HashMap<>();

            // get the actual count for each query
            for (Integer qid: event.getValidQueries()){

                //evaluate actual count for a query
                ArrayList<Event> preds = this.predecessorManager.getPredecessorsAfterLastGraphletSnapshotForQuery(event, qid);
                HashMap<Integer, BigInteger> coeffsum= sumPredSnapshots(event, preds);
                BigInteger countForQuery = this.predecessorManager.getSnapshotManager().evaluateSnapshotExpressionForQuery(coeffsum, qid);
                counts.put(qid, countForQuery);

            }

            // create a event level snapshot
            this.predecessorManager.getSnapshotManager().createEventSnapshot(event, counts);
        }

    }

    /**
     * for the start query, the corresponding count in the graphlet snapshot + 1
     * @param event
     * @param qid
     */
    public void updateStartEventForQuery(Event event, Integer qid){

        BigInteger oldCount = this.predecessorManager.getSnapshotManager().getLastGraphletSnapshot().getCounts().containsKey(qid)?
                this.predecessorManager.getSnapshotManager().getLastGraphletSnapshot().getCounts().get(qid):
                BigInteger.ZERO;

        this.predecessorManager.getSnapshotManager().getLastGraphletSnapshot().getCounts().put(qid, oldCount.add(BigInteger.ONE));

    }

}
