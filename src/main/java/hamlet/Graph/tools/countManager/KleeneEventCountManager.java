package hamlet.Graph.tools.countManager;

import hamlet.base.Event;
import hamlet.base.Snapshot;
import hamlet.Graph.tools.Utils;
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



        // have the same predecessors for all queries
        if (Utils.getInstance().getPredecessorManager().hasSamePredecessorsForValidQueries(event)&&
        !event.isHasSnapshot()){
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

            ArrayList<Integer> validStartQueries = event.getValidQueries();
            validStartQueries.retainAll(event.getType().getQueriesStartWith());

            // get the actual count for each query
            for (Integer qid: event.getValidQueries()){

                //evaluate actual count for a query
                ArrayList<Event> preds = Utils.getInstance().getPredecessorManager().getPredecessorsAfterLastGraphletSnapshotForQuery(event, qid);
                HashMap<Integer, BigInteger> coeffsum= sumPredSnapshots(event, preds);

                //evaluate the expression of snapshots
                Value countForQuery = Utils.getInstance().getSnapshotManager().evaluateSnapshotExpressionForQuery(coeffsum, qid);


                //increment the graphlet snapshot to the value for start queries
                if (validStartQueries.contains(qid)){
                    Snapshot lastGraphletSnapshot = Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot();
                    BigInteger newCount = countForQuery.getCount().add(lastGraphletSnapshot.getValues().get(qid).getCount());
                    BigDecimal newSum = countForQuery.getSum().add(lastGraphletSnapshot.getValues().get(qid).getSum());

                    countForQuery.setCount(newCount);
                    countForQuery.setSum(newSum);
                }

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
        if (aggregator.getFunc()== Aggregator.Aggregfunctions.COUNT){
            added.setSum(BigDecimal.ZERO);

        }else {
            String attrValueStr = (String) event.getAttributeValueByName(aggregator.getAttributeName());
            added.setSum(new BigDecimal(attrValueStr));
        }

        // count +=1, sum += attribute value
        Value newValue = oldValue.add(added);

        Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot().getValues().put(qid, newValue);

    }

}
