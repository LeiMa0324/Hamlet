package hamlet.graphlet;

import hamlet.event.Event;
import hamlet.graph.Snapshot;
import lombok.Data;

import java.math.BigInteger;
import java.util.HashMap;

/**
 * a graphlet splitted from a shared one
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
