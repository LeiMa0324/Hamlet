package hamlet.executor.tools;

import hamlet.base.Event;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.Snapshot;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class SnapshotManager {

    private ArrayList<Snapshot> snapshots;
    private ArrayList<Snapshot> graphletSnapshots;
    private ArrayList<Integer> allqids;

    public SnapshotManager(){
        this.snapshots = new ArrayList<>();
        this.graphletSnapshots = new ArrayList<>();
        this.allqids = Utils.getInstance().getQueryIds();
    }


    public Snapshot getLastGraphletSnapshot(){
        return graphletSnapshots.size()>0?graphletSnapshots.get(graphletSnapshots.size()-1): null;
    }


    /**
     * create a new event-level snapshot
     * @param event an event
     */
    public void createEventSnapshot(Event event,HashMap<Integer, Value> counts){

        //create event snapshot
        Snapshot eventSnapshot = new Snapshot(event, counts);

        //set event metric
        event.setMetric(Event.Metric.SNAPSHOT);

        //set snapshot level
        eventSnapshot.setSnapshotLevel(Snapshot.Levels.EVENT);

        //add to list
        this.snapshots.add(eventSnapshot);

        //set event's snapshot
        event.setSingleSnapshot(this.snapshots.indexOf(eventSnapshot));

    }

    /**
     * create a new graphlet-level snapshot
     */
    public void createGraphletSnapshot(Graphlet lastKleeneGraphlet, int eventIndex, HashMap<Integer, Value> prefixCounts){

        Snapshot newGraphletSnapshot = new Snapshot(lastKleeneGraphlet,eventIndex, prefixCounts);
        this.snapshots.add(newGraphletSnapshot);
        this.graphletSnapshots.add(newGraphletSnapshot);

    }

    /**
     * evaluate an expression of snapshots with coefficients
     * c1*x1+c2*x2... to an BigInteger
     * @param coeffsum
     * @return
     */
    public Value evaluateSnapshotExpressionForQuery(HashMap<Integer, BigInteger> coeffsum, int qid){

        Value evaluatedValue = new Value();

        for (Integer snapshotid: coeffsum.keySet()){
            Value value = this.snapshots.get(snapshotid).getValues().get(qid);

            //newcount = snap.count.qid * snap.coeff
            //newsum = snap.sum.qid * snap.coeff + event.attr* newcount
            evaluatedValue = evaluatedValue.add(value.multiply(coeffsum.get(snapshotid)));
        }

        return evaluatedValue;
    }

    /**
     * reset the counts of all snapshots to zero, because of the expired queries
     * @param
     */
    public void resetCountForExpiredQuery(int qid){
        for (Snapshot snapshot: this.snapshots){
            snapshot.getValues().put(qid, Value.ZERO);
        }
    }
}
