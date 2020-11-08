package hamlet.Graph.Graphlet;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.Graph.tools.Utils;
import hamlet.Graph.tools.countManager.KleeneEventCountManager;
import hamlet.Graph.tools.countManager.NoneKleeneEventCountManager;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public abstract class Graphlet {
    protected NoneKleeneEventCountManager noneKleeneEventCountManager;
    protected KleeneEventCountManager kleeneEventCountManager;
    protected ArrayList<Event> events;
    protected GraphletType type;
    protected EventType eventType;
    protected HashMap<Integer, Value> graphletValues; //the sum of counts of all the events

    protected ArrayList<Integer> snapshotIds;
    protected HashMap<Integer, BigInteger> snapshotToCoeffs;
    protected Event.Metric metric;
    protected ArrayList<Integer> allqids;


    public enum GraphletType {
        Kleene,
        NoneKleene,
        splits
    }

    public Graphlet() {
        this.events = new ArrayList<>();
        this.allqids = Utils.getInstance().getQueryIds();
    }

    public Graphlet(ArrayList<Event> events) {
        this.events = events;
        this.type = this.events.get(0).getType().isKleene() ? GraphletType.Kleene : GraphletType.NoneKleene;
        this.eventType = events.get(0).getType();
        this.snapshotToCoeffs = new HashMap<>();

        this.noneKleeneEventCountManager = Utils.getInstance().getNoneKleeneEventCountManager();
        this.kleeneEventCountManager = Utils.getInstance().getKleeneEventCountManager();
        this.graphletValues = new HashMap<>();
        this.allqids = Utils.getInstance().getQueryIds();

        for (Integer qid: this.allqids){
            this.graphletValues.put(qid, Value.ZERO);
        }


    }

    /**
     * propagate the count or snapshots in the graphlet
     */
    public abstract void propagate();


    public void printGraphletInfo(ArrayList<Event> burst){

        if (burst.get(0).getValidQueries().contains(4)){
            System.out.printf("find it");
        }

        System.out.printf("\n\n=========Graphlet Info=======\n");

        System.out.printf("Graphlet finished!\n");
        System.out.printf("Event type: "+this.eventType.getName()+"\n");
        System.out.printf("number of events:" +this.events.size()+"\n");
        System.out.printf("valid queries for this burst :"+burst.get(0).getValidQueries()+"\n");
        for (int qid: this.graphletValues.keySet()){
            System.out.printf("query "+qid);
            System.out.printf("     count: "+ graphletValues.get(qid)+"\n");
        }
        System.out.printf("=========End Graphlet Info=======\n\n");

    }
}


