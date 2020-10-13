package hamlet.executor.countManager;

import hamlet.base.Event;
import hamlet.executor.PredecessorManager;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * this class manages the intermediate counts of a none-kleene event
 */
@Data
public class NoneKleeneEventCountManager extends EventCountManager{

    public NoneKleeneEventCountManager(PredecessorManager predecessorManager){
        super(predecessorManager);
    }


    public void update(Event event){
        ArrayList<Integer> startQueries = event.getType().getQueriesStartWith();
        ArrayList<Integer> endQueries = event.getType().getQueriesEndWith();


        for (Integer qid: event.getValidQueries()){
            if (startQueries.contains(qid)){
                //prefix process
                updateStartEventForQuery(event, qid);
            }
            else{
                //suffix process
                ArrayList<Event> predsForQuery = this.predecessorManager.getPredecessorsAfterLastGraphletSnapshotForQuery(event, qid);
                HashMap<Integer, BigInteger> coeffs = sumPredSnapshots(event, predsForQuery);
                //set the snapshot id list
                event.setSnapshotIds(new ArrayList<Integer>(coeffs.keySet()));
                //set the coefficient hash map
                event.setSnapIdTocoeffs(coeffs);

                event.setMetric(Event.Metric.SNAPSHOT);

            }
        }
    }

    /**
     * none klene start events are always in metric count
     * @param event
     * @param qid
     */
    public void updateStartEventForQuery(Event event, Integer qid){

        event.getCounts().put(qid, BigInteger.ONE);
        event.setMetric(Event.Metric.COUNT); //set the metric to count

    }


}
