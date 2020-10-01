package hamlet.aoldgraphlet;

import hamlet.aoldevent.Event;
import hamlet.aoldgraph.Snapshot;
import lombok.Data;

/**
 * a graphlet splitted for a query
 */
@Data
public class SplittedGraphlet extends SharedGraphlet{


    private Integer qid;

    // copy every thing from the sharedG
    public SplittedGraphlet(Event e, Integer qid){
        super(e);
        this.qid = qid;

    }

    @Override
    public void updateCounts(Snapshot snapshot) {

        interCounts.put(qid, snapshot.getCounts().get(qid).multiply(coeff));

    }

}
