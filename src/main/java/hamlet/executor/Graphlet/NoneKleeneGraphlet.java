package hamlet.executor.Graphlet;

import hamlet.base.Event;

import java.math.BigInteger;
import java.util.ArrayList;

public class NoneKleeneGraphlet extends Graphlet{

    public NoneKleeneGraphlet(ArrayList<Event> events){
        super(events);
    }

    public void propagate(){

        noneKleeneEventCountManager.firstUpdate(events.get(0));
        Event.Metric metric = events.get(0).getMetric();

        //prefix graphlet graphlet.count = size *count
        if (metric == Event.Metric.VAL) {
            updateNoneKleeneGraphletValuesByValues();
        }

        this.metric = metric;

        printGraphletInfo();

    }

    /**
     * if using values
     * update only the count for prefix
     */
    private void updateNoneKleeneGraphletValuesByValues(){
        for (Integer qid : events.get(0).getValidQueries()) {
            this.graphletValues.put(qid, events.get(0).getValues().get(qid).multiply(
                    new BigInteger(this.events.size() + "")));
        }
    }

//    /**
//     * if using snapshots
//     * update the count, sum for suffix
//     */
//    private void updateNoneKleeneGraphletValuesBySnapshots(){
//
//        this.snapshotIds = this.events.get(0).getSnapshotIds();
//
//        // get the coeff list
//        for (Integer snapid : this.snapshotIds) {
//            this.snapshotToCoeffs.put(snapid, this.events.get(0).getSnapIdTocoeffs().get(snapid).multiply(
//                    new BigInteger(this.events.size() + "")
//            ));
//        }
//
//        //evaluate snapshot list for both count and sum
//        for (int qid: this.events.get(0).getValidQueries()) {
//            Value value = Utils.getInstance().getSnapshotManager().evaluateSnapshotExpressionForQuery(this.snapshotToCoeffs,qid);
//            this.graphletValues.put(qid, value);
//
//        }
//    }
}
