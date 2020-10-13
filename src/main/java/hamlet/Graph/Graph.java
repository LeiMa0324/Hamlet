package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.executor.*;
import hamlet.executor.countManager.KeeneEventCountManager;
import hamlet.executor.countManager.NoneKleeneEventCountManager;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public abstract class Graph {

    protected WindowManager windowManager;
    protected PredicateManager predicateManager;
    protected SnapshotManager snapshotManager;
    protected PredecessorManager predecessorManager;
    protected GraphletManager graphletManager;
    protected NoneKleeneEventCountManager noneKleeneEventCountManager;
    protected KeeneEventCountManager kleeneEventCountManager;


    protected Template template;
    protected ArrayList<Event> events;
    protected ArrayList<ArrayList<Event>> bursts;
    protected ArrayList<Graphlet> graphlets;
    protected HashMap<Integer, BigInteger> finalCounts;
    protected ArrayList<Integer> queryIds;

    public Graph(Template template, ArrayList<Event> events){
        this.windowManager = template.getWindowManager();
        this.predicateManager = template.getPredicateManager();
        this.graphletManager = new GraphletManager();


        this.template = template;
        this.bursts = new ArrayList<>();
        this.finalCounts = new HashMap<>();
        this.queryIds = new ArrayList<>();
        for (int i =0; i< this.template.getWorkload().getQueries().size(); i++){
            this.queryIds.add(i);
        }

        this.snapshotManager = new SnapshotManager(this.queryIds);

        BurstLoader burstLoader = new BurstLoader(predicateManager);
        this.bursts = burstLoader.load(events);
        this.events = burstLoader.getEvents();

        this.predecessorManager = new PredecessorManager(this.events, template, this.snapshotManager);
        this.noneKleeneEventCountManager = new NoneKleeneEventCountManager(predecessorManager);
        this.kleeneEventCountManager = new KeeneEventCountManager(predecessorManager);


    }

    public abstract void run();
    public abstract void kleeneBurstProcess(ArrayList<Event> burst);
    public abstract void noneKleeneBurstProcess(ArrayList<Event> burst);
    public abstract void updateFinalCount(Graphlet graphlet);

}
