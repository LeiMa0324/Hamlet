package hamlet.Graph.tools.countManager;

import hamlet.base.Event;
import hamlet.Graph.Graphlet.Graphlet;
import hamlet.Graph.tools.GraphletManager.GraphletManager_DynamicHamlet;
import hamlet.Graph.tools.GraphletManager.GraphletManager_StaticHamlet;
import hamlet.Graph.tools.Utils;
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


        for (Integer qid: event.getValidQueries()){
            if (startQueries.contains(qid)){
                //prefix process
                updateSnapshotForStartEventPerQuery(event, qid);
            }
            else{
                Graphlet lastKleene;
                //suffix process, get the last kleene graphlet's values
                //static graphlet manager
                if (Utils.getInstance().getGraphType()== Utils.GraphType.STATIC){
                    GraphletManager_StaticHamlet staticGraphletManager = (GraphletManager_StaticHamlet) Utils.getInstance().getGraphletManager();
                    lastKleene = staticGraphletManager.getLastKleeneGraphlet();

                }
                else {
                    //dynamic graphlet manager
                    GraphletManager_DynamicHamlet dynamicGraphletManager = (GraphletManager_DynamicHamlet) Utils.getInstance().getGraphletManager();
                    //could be split or merged
                    lastKleene = dynamicGraphletManager.getLastKleeneGraphlet();
                }
                if (lastKleene==null){
                    event.getValues().put(qid, Value.ZERO);

                }else {
                    event.getValues().put(qid, lastKleene.getGraphletValues().get(qid));

                }
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

