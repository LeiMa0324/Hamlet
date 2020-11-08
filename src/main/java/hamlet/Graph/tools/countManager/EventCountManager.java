package hamlet.Graph.tools.countManager;

import hamlet.base.Event;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Data
public abstract class EventCountManager {

    public EventCountManager(){}

    public abstract void updateSnapshotForStartEventPerQuery(Event event, Integer qid);
    /**
     * only when an event has same predecessors for different queries, call this method
     * get the unique set of snapshots for all queries, and the coeff
     * @param event a given event
     * @param preds the preds for qid
     */

    protected HashMap<Integer, BigInteger> sumPredSnapshots(Event event, ArrayList<Event> preds){
        Set<Integer> snapshotIdSet = new HashSet<>();
        HashMap<Integer, BigInteger> coeffSum = new HashMap<>();

        if (!preds.isEmpty()) {
            for (Event p : preds) {
                snapshotIdSet.addAll(p.getSnapshotIds());

                for (Integer snapid : p.getSnapshotIds()) {
                    if (!coeffSum.containsKey(snapid)) {
                        coeffSum.put(snapid, p.getSnapIdTocoeffs().get(snapid));
                    } else {
                        BigInteger newCoeff = coeffSum.get(snapid).add(p.getSnapIdTocoeffs().get(snapid));
                        coeffSum.put(snapid, newCoeff);
                    }
                }
            }
        }

        return coeffSum;
    }


}
