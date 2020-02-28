package Hamlet.Graph;

import Hamlet.Graphlet.NonSharedGraphlet;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigInteger;


import java.util.ArrayList;
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
     * @param Gs
     */
    public Snapshot(Snapshot predSnapshot, BigInteger coeff, HashMap<String,NonSharedGraphlet> Gs){
        this.snapshotHashMap = new HashMap<Integer, BigInteger>();
            for (int qid: predSnapshot.snapshotHashMap.keySet()){ // for each query
                /**
                 * 如果某个non shared G 包含该qid，则将其count加入snapshot中对应的qid中
                 */
                for (NonSharedGraphlet g : Gs.values()){
                    if (g.eventType.getQids().contains(qid)) {
                        BigInteger count = predSnapshot.snapshotHashMap.get(qid).multiply(coeff.add(new BigInteger("1"))).add(g.getCounts().get(qid));
                        this.snapshotHashMap.put(qid, count);
                    }
                }
            }

    }

    /**
     * 如果没有predsnapshot的话
      * @param Gs
     */
    public Snapshot(HashMap<String,NonSharedGraphlet> Gs){
        this.snapshotHashMap = new HashMap<Integer, BigInteger>();

        for (NonSharedGraphlet g : Gs.values()) {
            for (int q: g.eventType.getQids()) {
                BigInteger count = g.getCounts().get(q);
                this.snapshotHashMap.put(q, count);
            }
        }
    }
}

