package hamlet.executor;

import hamlet.base.Event;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class SnapshotManager {

    private ArrayList<Snapshot> snapshots;
    private ArrayList<Snapshot> graphletSnapshots;
    private ArrayList<Integer> allqids;

    public SnapshotManager(ArrayList<Integer> allqids){
        this.snapshots = new ArrayList<>();
        this.graphletSnapshots = new ArrayList<>();
        this.allqids = allqids;
    }


    public Snapshot getLastGraphletSnapshot(){
        return graphletSnapshots.size()>0?graphletSnapshots.get(graphletSnapshots.size()-1): null;
    }


    /**
     * create a new event-level snapshot
     * @param event an event
     */
    public void createEventSnapshot(Event event,HashMap<Integer, BigInteger> counts){

        //create event snapshot
        Snapshot eventSnapshot = new Snapshot(event, counts, this.allqids);

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
    public void createGraphletSnapshot(Graphlet lastKleeneGraphlet, int eventIndex, HashMap<Integer, BigInteger> prefixCounts){

        Snapshot newGraphletSnapshot = new Snapshot(lastKleeneGraphlet,eventIndex, prefixCounts, this.allqids);
        this.snapshots.add(newGraphletSnapshot);
        this.graphletSnapshots.add(newGraphletSnapshot);

    }

    /**
     * evaluate an expression of snapshots with coefficients
     * c1*x1+c2*x2... to an BigInteger
     * @param coeffsum
     * @return
     */
    public BigInteger evaluateSnapshotExpressionForQuery(HashMap<Integer, BigInteger> coeffsum, int qid){

        BigInteger sum = BigInteger.ZERO;
        for (Integer snapshotid: coeffsum.keySet()){
            BigInteger count = this.snapshots.get(snapshotid).getCounts().get(qid);
            if (count==null){
                System.out.printf("find it");
            }
            //snap.count.qid * snap.coeff
            sum = sum.add(count.multiply(coeffsum.get(snapshotid)));
        }

        return sum;
    }

    /**
     * reset the counts of all snapshots to zero, because of the expired queries
     * @param
     */
    public void resetCountForExpiredQuery(int qid){
        for (Snapshot snapshot: this.snapshots){
            snapshot.getCounts().put(qid, BigInteger.ZERO);

        }
    }
}
