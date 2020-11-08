package hamlet.Graph.Graphlet.Static;

import hamlet.base.Event;
import hamlet.Graph.Graphlet.Graphlet;
import hamlet.base.Snapshot;
import hamlet.Graph.tools.SnapshotManager;
import hamlet.Graph.tools.Utils;
import hamlet.query.aggregator.Value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

public class KleeneGraphlet extends Graphlet {

    public KleeneGraphlet(ArrayList<Event> events){
        super(events);
    }

    public void propagate(){

        for (Event event : this.events) {
            kleeneEventCountManager.update(event);
            updateKleeneGraphletValuesBySnapshots(event);
        }
        VanishingSnapshotProcess();
        printGraphletInfo(this.events);
    }

    protected void updateKleeneGraphletValuesBySnapshots(Event event){



        for (int qid: event.getValidQueries()) {

            SnapshotManager snapshotManager = Utils.getInstance().getSnapshotManager();

            //the actual values of an event after evaluation
            Value eventValuesForQuery = snapshotManager.evaluateSnapshotExpressionForQuery(event.getSnapIdTocoeffs(), qid);

            // if the sum =0, but the count !=0, sum = this event's attr* count
            if (eventValuesForQuery.getSum().equals(BigDecimal.ZERO)&& !eventValuesForQuery.getCount().equals(BigInteger.ZERO)){

                String attrValueStr = (String) event.getAttributeValueByName(Utils.getInstance().getAggregator().getAttributeName());
                BigDecimal arrtValue = attrValueStr==null? BigDecimal.ZERO: new BigDecimal(attrValueStr);
                eventValuesForQuery.setSum(arrtValue.multiply(new BigDecimal(eventValuesForQuery.getCount())));
            }

            Value oldGpraghletValuesForQuery = this.graphletValues.containsKey(qid) ? this.graphletValues.get(qid) : Value.ZERO;
            Value newGpraghletValuesForQuery = oldGpraghletValuesForQuery.add(eventValuesForQuery);

            // add the event's values to the graphlt values
            this.graphletValues.put(qid, newGpraghletValuesForQuery);

        }

    }

    /**
     * this method deals with the snapshot vanishing problem
     * for a query qi: if the currrent graphlet has no valid events for that, after the propagation, the graphlet's count for qi = =0
     * when adding to the final, the previous snapshot vanishes
     */
    protected void VanishingSnapshotProcess(){
        //snapshot carries both count and sum, when passing an empty graphlet, the values shouldn't vanish
        //if all events are invalid for some query,
        // but the g-snapshot has the count for the query, keep it in the graphlet's count
        //also, add the sum of last kleene to this one
        Snapshot graphletSnapshot = Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot();

        for (int qid: this.allqids){
            // if the snapshot has count for query i, but the graphlet dont have the count for query i, pass the snapshot's count
            // to the graphlet, so the final count only needs to reach the last graphlet
            if (this.graphletValues.get(qid).equals(Value.ZERO)&& (!graphletSnapshot.getValues().get(qid).equals(Value.ZERO))){
                this.graphletValues.put(qid, graphletSnapshot.getValues().get(qid));

            }
        }
    }

}
