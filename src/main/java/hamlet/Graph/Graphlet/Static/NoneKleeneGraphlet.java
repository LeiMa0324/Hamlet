package hamlet.Graph.Graphlet.Static;

import hamlet.base.Event;
import hamlet.Graph.Graphlet.Graphlet;

import java.math.BigInteger;
import java.util.ArrayList;

public class NoneKleeneGraphlet extends Graphlet {

    public NoneKleeneGraphlet(ArrayList<Event> events){
        super(events);
    }

    public void propagate(){

        noneKleeneEventCountManager.firstUpdate(events.get(0));
        Event.Metric metric = events.get(0).getMetric();

        //prefix graphlet graphlet.count = size *count
        if (metric == Event.Metric.VAL) {
            updateNoneKleeneGraphletValuesByValues();
        }

        this.metric = metric;

        printGraphletInfo(this.events);

    }

    /**
     * if using values
     * update only the count for prefix
     */
    private void updateNoneKleeneGraphletValuesByValues(){
        for (Integer qid : events.get(0).getValidQueries()) {
            this.graphletValues.put(qid, events.get(0).getValues().get(qid).multiply(
                    new BigInteger(this.events.size() + "")));
        }
    }


}
