package hamlet.graph;

import hamlet.graphlet.NonSharedGraphlet;
import lombok.Data;

import java.math.BigInteger;


import java.util.HashMap;

/**
 * Snapshot only maintains a hashmap for interCounts in each query
 * two kinds of updates:
 *      1. if this is the first snapshot this Graph has, snapshot <- unshared graphlet's interCounts
 *      2. if this is not the first snapshot, new snapshot = old snapshot*coeff + predecessors'
 *      .interCounts
 */
@Data
public class Snapshot {
    // qid: interCounts
    private HashMap<Integer, BigInteger> counts;

    public Snapshot(){
        this.counts = new HashMap<>();

    }

    /**
     * for certain qid, update the interCounts in the snapshot with the shared Graphlet and the non-shared Graphlet
     * @param coeff the coefficient from the shared Graphlet
     * @param predG a pred Graphlet of query qid
     * @param qid   the given query id
     */
    public void update(BigInteger coeff, NonSharedGraphlet predG, Integer qid){
        if (!counts.isEmpty()){
            BigInteger old_count = this.counts.get(qid);
            this.counts.put(qid,old_count.multiply(coeff.add(new BigInteger("1"))).add(predG.interCounts.get(qid)));
        }

    }

    /**
     * the first snapshot has no shared Graphlet
     * pass non-shared G's count into the snapshot
     * @param predG the predecessor non-shared Graphlet
     * @param qid   the given query id
     */
    public void update(NonSharedGraphlet predG, Integer qid){

        this.counts.put(qid,predG.interCounts.get(qid));
    }

    /**
     * create a snapshot in the shared graphlet
     *
     * @param qid   the given query id
     */
    public void updatewithPredicate(BigInteger coeff,Integer qid){

        BigInteger old_count = this.counts.get(qid);
        //for first snap shot = coeff
        old_count = old_count.equals(new BigInteger("0"))?coeff:old_count;
        this.counts.put(qid,old_count.multiply(coeff.add(new BigInteger("1"))));
    }
}

