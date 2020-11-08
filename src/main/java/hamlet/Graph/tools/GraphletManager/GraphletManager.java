package hamlet.Graph.tools.GraphletManager;

import hamlet.base.EventType;
import hamlet.Graph.Graphlet.Graphlet;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public abstract class GraphletManager {
    protected ArrayList<Graphlet> graphlets;
    protected ArrayList<Graphlet> kleeneGraphlets;

    //todo: set last index to -1
    protected int lastKleeneGraphletIndex = -1;

    public GraphletManager(){
        this.graphlets = new ArrayList<>();
        this.kleeneGraphlets = new ArrayList<>();
    }

    public abstract void addGraphlet(Graphlet graphlet);
    public abstract Graphlet getLastKleeneGraphlet();
    /**
     * find graphlets in certain range
     * @param startIndex
     * @param endIndex
     * @return a hashmap of event type to graphlets
     */
    public HashMap<EventType,ArrayList<Graphlet>> getGraphletsInRange(int startIndex, int endIndex){
        ArrayList<Graphlet> gs =  new ArrayList<>(this.graphlets.subList(startIndex, endIndex));

        HashMap<EventType, ArrayList<Graphlet>> eventTypeToGraphlets = new HashMap<>();
        for (Graphlet g:gs){
            ArrayList<Graphlet> gsForET;
            if (!eventTypeToGraphlets.containsKey(g.getEventType())){
                gsForET = new ArrayList<>();
            }else {
                gsForET = eventTypeToGraphlets.get(g.getEventType());
            }

            gsForET.add(g);
            eventTypeToGraphlets.put(g.getEventType(), gsForET);

        }
        return eventTypeToGraphlets;
    }
}
