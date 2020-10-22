package hamlet.executor.tools.countManager;

import hamlet.base.Event;
import hamlet.executor.Graphlet.KleeneGraphlet;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;

/**
 * this class manages the intermediate counts of a none-kleene event
 */
@Data
public class NoneKleeneEventCountManager extends EventCountManager{

    public NoneKleeneEventCountManager(){
        super();
    }


    public void firstUpdate(Event event){
        ArrayList<Integer> startQueries = event.getType().getQueriesStartWith();
        ArrayList<Integer> endQueries = event.getType().getQueriesEndWith();


        for (Integer qid: event.getValidQueries()){
            if (startQueries.contains(qid)){
                //prefix process
                updateSnapshotForStartEventPerQuery(event, qid);
            }
            else{
                if (event.getType().getName().equals("XTLB")){
                    System.out.printf("find it!");
                }
                //suffix process, get the last kleene graphlet's values
                KleeneGraphlet kleeneGraphlet = Utils.getInstance().getGraphletManagerStaticHamlet().getLastKleeneGraphlet();
                event.getValues().put(qid, kleeneGraphlet.getGraphletValues().get(qid));
                event.setMetric(Event.Metric.VAL);

            }
        }
    }

    /**
     * none klene start events are always in metric count
     * @param event
     * @param qid
     */
    public void updateSnapshotForStartEventPerQuery(Event event, Integer qid){

        event.getValues().put(qid, new Value(BigInteger.ONE));
        event.setMetric(Event.Metric.VAL); //set the metric to count

    }


}

