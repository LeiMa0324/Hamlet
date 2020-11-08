package hamlet.Graph.tools.GraphletManager;

import hamlet.Graph.Graphlet.Graphlet;
import hamlet.Graph.Graphlet.Static.KleeneGraphlet;
import lombok.Data;

import java.util.ArrayList;

@Data
public class GraphletManager_StaticHamlet extends GraphletManager{


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

        return this.lastKleeneGraphletIndex==-1?null: (KleeneGraphlet) this.graphlets.get(this.lastKleeneGraphletIndex);
    }
}
