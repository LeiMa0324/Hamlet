package baselines.mcep;

import hamlet.event.Event;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@NoArgsConstructor
public class McepNode {

    public Event event;
    public HashMap<Integer, ArrayList<McepNode>> previous; //all predecessors according to the query
    public HashMap<Integer, BigInteger> intercount;
    public McepNode latestSharedPred;  //maintain the last predecessor
    public HashMap<Integer, Boolean> traversed;



    public McepNode (Event e) {
        event = e;
        previous = new HashMap<>();
        intercount = new HashMap<>();
        traversed = new HashMap<>();

        for (Integer qid: event.eventType.getQids()){
            intercount.put(qid,BigInteger.ZERO);
            traversed.put(qid, false);
            previous.put(qid, new ArrayList<McepNode>());  //-1 means shared by all queries

        }
        latestSharedPred = new McepNode();

    }

    /**
     * connect a list of predcessors for a qid
     * @param preds
     * @param qid
     */
    public void connectPreds ( Integer qid,ArrayList<McepNode> preds) {

        int secDiff = this.event.getSec();

        for (McepNode pred: preds) {

            //for shared events, only connect once
            if (pred.event.string.equals(event.string)) {

                ArrayList<McepNode> predlist = new ArrayList<>();

                if (previous.keySet().contains(-1) && (!previous.get(-1).contains(pred))) {   //if -1 exists but doesn't contain this pred
                    predlist = previous.get(-1);

                }
                predlist.add(pred);
                previous.put(-1, predlist);

            } else {

                if ((!previous.get(qid).contains(pred)) && pred.event.getSec() < this.event.getSec()) {

                    if (pred.event.eventType.isShared && secDiff>(this.event.getSec()-pred.event.getSec())){
                        this.latestSharedPred = pred;
                        secDiff = this.event.getSec()-pred.event.getSec();
                    }

                    ArrayList<McepNode> predlist = previous.get(qid);
                    predlist.add(pred);
                    previous.put(qid, predlist);
                }
            }
        }

    }

//    public String toString() {
//        return ""+event.string;
//    }
}
