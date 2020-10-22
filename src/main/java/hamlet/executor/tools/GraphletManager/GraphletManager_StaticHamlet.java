package hamlet.executor.tools.GraphletManager;

import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.Graphlet.KleeneGraphlet;
import lombok.Data;

import java.util.ArrayList;

@Data
public class GraphletManager_StaticHamlet extends GraphletManager{
    private ArrayList<Graphlet> graphlets;
    private int lastKleeneGraphletIndex;
    private ArrayList<Graphlet> kleeneGraphlets;

    public GraphletManager_StaticHamlet(){
        this.graphlets = new ArrayList<>();
        this.kleeneGraphlets = new ArrayList<>();
    }

    public void addGraphlet(Graphlet graphlet){
        this.graphlets.add(graphlet);

        if (graphlet.getEventType().isKleene()){
            this.lastKleeneGraphletIndex = this.graphlets.size()-1;
            this.kleeneGraphlets.add(graphlet);
        }
    }



    public KleeneGraphlet getLastKleeneGraphlet(){
        int gid = this.lastKleeneGraphletIndex;
        return (KleeneGraphlet) this.graphlets.get(gid);
    }
}
