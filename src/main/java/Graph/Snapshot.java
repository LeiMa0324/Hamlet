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
     * snapshot = previous_snapshot + sharedG.coeff*previsou_snapshot + nonsharedG.count
     * @param predSnapshot the previous snapshot
     * @param coeff the computed shared Graphlet's coefficient
     * @param countPerQueryHashMap the hash map of <queryid, count> in non shared graphlet
     */
    public Snapshot(Snapshot predSnapshot, int coeff, HashMap<Integer, Integer> countPerQueryHashMap){
        this.snapshotHashMap = new HashMap<Integer, Integer>();
        if (predSnapshot.snapshotHashMap!=null){    //如果已有snapshot，直接根据公式更新
            for (int qid: predSnapshot.snapshotHashMap.keySet()){
                this.snapshotHashMap.put(qid, predSnapshot.snapshotHashMap.get(qid)*(coeff+1)+countPerQueryHashMap.get(qid));
            }
        }
        else {      //第一个snapshot即为non shared graphlet的count hashmap
            this.snapshotHashMap= countPerQueryHashMap;
        }

    }

}
