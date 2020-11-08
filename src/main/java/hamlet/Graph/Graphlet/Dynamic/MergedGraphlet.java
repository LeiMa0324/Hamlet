package hamlet.Graph.Graphlet.Dynamic;

import hamlet.base.Event;
import hamlet.Graph.Graphlet.Static.KleeneGraphlet;
import hamlet.base.Snapshot;
import hamlet.Graph.tools.Utils;
import lombok.Data;

import java.util.ArrayList;
@Data
public class MergedGraphlet extends KleeneGraphlet {

    private int snapshotNum = 0;

    public MergedGraphlet(ArrayList<Event> events){
        super(events);
    }

    @Override

    public void propagate(){

        for (Event event : this.events) {
            kleeneEventCountManager.update(event);
            updateKleeneGraphletValuesBySnapshots(event);

            //keep track of # event snapshots in the graphlet
            int lastSnapshotIndex = Utils.getInstance().getSnapshotManager().getSnapshots().size()-1;
            Snapshot lastSnapshot = Utils.getInstance().getSnapshotManager().getSnapshots().get(lastSnapshotIndex);
            if (lastSnapshot.getEvent()==event){
                this.snapshotNum = this.snapshotNum + 1;
            }
        }
        VanishingSnapshotProcess();
        printGraphletInfo(this.events);
    }

    public void extend(ArrayList<Event> burst){

        for (Event event : burst) {
            kleeneEventCountManager.update(event);
            updateKleeneGraphletValuesBySnapshots(event);
        }
        VanishingSnapshotProcess();
        printGraphletInfo(burst);
    }


}
