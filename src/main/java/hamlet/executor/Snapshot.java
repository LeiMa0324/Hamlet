package hamlet.executor;

import hamlet.base.Event;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class Snapshot {
    private HashMap<Integer, Value> values;
    private Levels snapshotLevel;
    private Event event;
    private ArrayList<Integer> allqids = Utils.getInstance().getQueryIds();

    private int eventIndex;

    //create an event-level snapshot
    public Snapshot(Event event, HashMap<Integer, Value> values){
        //pass the values to the event snapshot
        this.values = values;

        for (Integer qid: allqids){
            if (!values.containsKey(qid)){
                this.values.put(qid, Value.ZERO);
            }
        }
        this.event = event;
        this.eventIndex = event.getEventIndex();
    }

    //add the prefix counts to the old graphlet's total count to get a new graphlet snapshot
    public Snapshot(Graphlet lastKleeneGraphlet, int eventIndex, HashMap<Integer, Value> prefixValues){

        this.values = new HashMap<>();
        this.eventIndex = eventIndex;
        if (lastKleeneGraphlet == null){
            this.values = prefixValues;
        }else {

            for (Integer qid : prefixValues.keySet()) {
                Value values = lastKleeneGraphlet.getGraphletValues().containsKey(qid)?
                        lastKleeneGraphlet.getGraphletValues().get(qid).add(prefixValues.get(qid)):
                        prefixValues.get(qid);
                this.values.put(qid, values);
            }
        }

        for (Integer qid: allqids){
            if (!values.containsKey(qid)){
                values.put(qid, Value.ZERO);
            }
        }

        snapshotLevel = Levels.GRAPHLET;
    }


   public enum Levels{
        EVENT,
        GRAPHLET
    }
}
