package Graph;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@Data
@NoArgsConstructor
public class Snapshot {
    // qid: count
    private HashMap<Integer, Integer> snapshotHashMap;

    /**
     * for each query
     * snapshot = previous_snapshot + sharedG.coeff*previsou_snapshot + nonsharedG.count
     * @param predSnapshot the previous snapshot
     * @param coeff the computed shared Graphlet's coefficient
     * @param countPerQueryHashMap the hash map of <queryid, count> in non shared graphlet
     */
    public Snapshot(Snapshot predSnapshot, int coeff, HashMap<Integer, Integer> countPerQueryHashMap){
        this.snapshotHashMap = new HashMap<Integer, Integer>();
        if (predSnapshot.snapshotHashMap!=null){    //if previous snapshot existed，update the snapshot by formula
            for (int qid: predSnapshot.snapshotHashMap.keySet()){
                this.snapshotHashMap.put(qid, predSnapshot.snapshotHashMap.get(qid)*(coeff+1)+countPerQueryHashMap.get(qid));
            }
        }
        else {      //if this is the first snapshot, the snapshot should be the nonshared graphlet's count hashmap
            this.snapshotHashMap= countPerQueryHashMap;
        }

    }

}
