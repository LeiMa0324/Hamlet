package Hamlet.Graph;

import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigInteger;


import java.util.HashMap;

@Data
@NoArgsConstructor
public class Snapshot {
    // qid: count
    private HashMap<Integer, BigInteger> snapshotHashMap;

    /**
     * for each query
     * snapshot = previous_snapshot + sharedG.coeff*previsou_snapshot + nonsharedG.count
     * @param predSnapshot the previous snapshot
     * @param coeff the computed shared Hamlet.Graphlet's coefficient
     * @param countPerQueryHashMap the hash map of <queryid, count> in non shared graphlet
     */
    public Snapshot(Snapshot predSnapshot, BigInteger coeff, HashMap<Integer, BigInteger> countPerQueryHashMap){
        this.snapshotHashMap = new HashMap<Integer, BigInteger>();
        if (predSnapshot.snapshotHashMap!=null){    //if previous snapshot existed，update the snapshot by formula
            for (int qid: predSnapshot.snapshotHashMap.keySet()){
                BigInteger count = predSnapshot.snapshotHashMap.get(qid).multiply(coeff.add(new BigInteger("1"))).add(countPerQueryHashMap.get(qid));
                this.snapshotHashMap.put(qid, count);
            }
        }
        else {      //if this is the first snapshot, the snapshot should be the nonshared graphlet's count hashmap
            this.snapshotHashMap= countPerQueryHashMap;
        }

    }

}
