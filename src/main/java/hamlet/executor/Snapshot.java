package hamlet.executor;

import hamlet.base.Event;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public class Snapshot {
    private HashMap<Integer, BigInteger> counts;
    private Levels snapshotLevel;
    private Event event;
    private ArrayList<Integer> allqids;

    private int eventIndex;


    public Snapshot(Event event, HashMap<Integer, BigInteger> counts, ArrayList<Integer> allqids){
        this.counts = counts;
        for (Integer qid: allqids){
            if (!counts.containsKey(qid)){
                counts.put(qid, BigInteger.ZERO);
            }
        }
        this.event = event;
        this.eventIndex = event.getEventIndex();
    }

    //add the prefix counts to the old graphlet's total count to get a new graphlet snapshot
    public Snapshot(Graphlet lastKleeneGraphlet, int eventIndex, HashMap<Integer, BigInteger> prefixCounts,  ArrayList<Integer> allqids){
        this.counts = new HashMap<>();
        this.eventIndex = eventIndex;
        this.allqids = allqids;
        if (lastKleeneGraphlet == null){
            this.counts = prefixCounts;
        }else {

            for (Integer qid : prefixCounts.keySet()) {
                BigInteger count = lastKleeneGraphlet.getTotalCount().containsKey(qid)?
                        lastKleeneGraphlet.getTotalCount().get(qid).add(prefixCounts.get(qid)):
                        prefixCounts.get(qid);
                this.counts.put(qid, count);
            }
        }

        for (Integer qid: allqids){
            if (!counts.containsKey(qid)){
                counts.put(qid, BigInteger.ZERO);
            }
        }

        snapshotLevel = Levels.GRAPHLET;
    }


   public enum Levels{
        EVENT,
        GRAPHLET
    }
}
