package hamlet.executor.tools.countManager;

import hamlet.base.Event;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Aggregator;
import hamlet.query.aggregator.Value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class KleeneEventCountManager extends EventCountManager {

    public KleeneEventCountManager(){
        super();
    }

    //todo: such a mess
    public void update(Event event){
        ArrayList<Integer> startQueries = event.getType().getQueriesStartWith();
        ArrayList<Integer> endQueries = event.getType().getQueriesEndWith();

        //update start events

        for (int qid: startQueries){
            //B+ with start type process
            updateSnapshotForStartEventPerQuery(event, qid);
        }

        // have the same predecessors for all queries
        if (Utils.getInstance().getPredecessorManager().hasSamePredecessorsForValidQueries(event)){
            ArrayList<Event> preds = Utils.getInstance().getPredecessorManager().getPredecessorsAfterLastGraphletSnapshotForQuery(event, event.getValidQueries().get(0));
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
                snapshotIds.add(Utils.getInstance().getSnapshotManager().getSnapshots().indexOf(
                        Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot()));
                //coeff = 1
                coeffsum.put(snapshotIds.get(0), BigInteger.ONE);

                event.setSnapshotIds(snapshotIds);
                event.setSnapIdTocoeffs(coeffsum);
                event.setMetric(Event.Metric.SNAPSHOT);

            }
        // have different predecessors for queries, create a event-level snapshot
        }else {
            HashMap<Integer, Value> values = new HashMap<>();

            // get the actual count for each query
            for (Integer qid: event.getValidQueries()){

                //evaluate actual count for a query
                ArrayList<Event> preds = Utils.getInstance().getPredecessorManager().getPredecessorsAfterLastGraphletSnapshotForQuery(event, qid);
                HashMap<Integer, BigInteger> coeffsum= sumPredSnapshots(event, preds);

                //evaluate the expression of snapshots
                Value countForQuery = Utils.getInstance().getSnapshotManager().evaluateSnapshotExpressionForQuery(coeffsum, qid);

                values.put(qid, countForQuery);
            }

            // create a event level snapshot
            Utils.getInstance().getSnapshotManager().createEventSnapshot(event, values);
        }

    }

    /**
     * for the start query, the corresponding count in the graphlet snapshot + 1
     * @param event
     * @param qid
     */
    public void updateSnapshotForStartEventPerQuery(Event event, Integer qid){

        Value oldValue = Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot().getValues().containsKey(qid)?
                Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot().getValues().get(qid):
                Value.ZERO;

        Value added = new Value(BigInteger.ONE);
        Aggregator aggregator = Utils.getInstance().getAggregator();
        String attrValueStr = (String)event.getAttributeValueByName(aggregator.getAttributeName());
        added.setSum(new BigDecimal(attrValueStr));

        // count +=1, sum += attribute value
        Value newValue = oldValue.add(added);

        Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot().getValues().put(qid, oldValue.add(newValue));

    }

}
