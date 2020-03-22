package hamlet.graph;

import hamlet.graphlet.NonSharedGraphlet;
import lombok.Data;

import java.math.BigInteger;


import java.util.HashMap;

/**
 * Snapshot only maintains a hashmap for counts in each query
 * two kinds of updates:
 *      1. if this is the first snapshot this Graph has, snapshot <- unshared graphlet's counts
 *      2. if this is not the first snapshot, new snapshot = old snapshot*coeff + predecessors'
 *      .counts
 */
@Data
public class Snapshot {
    // qid: counts
    private HashMap<Integer, BigInteger> counts;

    public Snapshot(){
        this.counts = new HashMap<>();

    }

    /**
     * for certain qid, update the counts in the snapshot with the shared Graphlet and the non-shared Graphlet
     * @param coeff the coefficient from the shared Graphlet
     * @param predG a pred Graphlet of query qid
     * @param qid   the given query id
     */
    public void update(BigInteger coeff, NonSharedGraphlet predG, Integer qid){
        BigInteger old_count = this.counts.get(qid);

        this.counts.put(qid,old_count.multiply(coeff.add(new BigInteger("1"))).add(predG.getCounts().get(qid)));
    }

    /**
     * the first snapshot has no shared Graphlet
     * pass non-shared G's count into the snapshot
     * @param predG the predecessor non-shared Graphlet
     * @param qid   the given query id
     */
    public void update(NonSharedGraphlet predG, Integer qid){

        this.counts.put(qid,predG.getCounts().get(qid));
    }
}

