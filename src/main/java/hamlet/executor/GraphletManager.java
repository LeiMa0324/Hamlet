package hamlet.executor;

import hamlet.base.EventType;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class GraphletManager {
    private ArrayList<Graphlet> graphlets;
    private int lastKleeneGraphletIndex;

    public GraphletManager(){
        this.graphlets = new ArrayList<>();
    }

    public void addGraphlet(Graphlet graphlet){
        this.graphlets.add(graphlet);
    }

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
