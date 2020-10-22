package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.Graphlet.NoneKleeneGraphlet;
import hamlet.executor.Snapshot;
import hamlet.executor.tools.*;
import hamlet.executor.tools.GraphletManager.GraphletManager_StaticHamlet;
import hamlet.executor.tools.countManager.KleeneEventCountManager;
import hamlet.executor.tools.countManager.NoneKleeneEventCountManager;
import hamlet.query.Query;
import hamlet.query.aggregator.Aggregator;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

@Data
public abstract class Graph {

    protected WindowManager windowManager;
    protected PredicateManager predicateManager;


    protected Template template;
    protected ArrayList<Event> events;
    protected ArrayList<ArrayList<Event>> bursts;
    protected HashMap<Integer, Value> finalValues;
    protected Utils utils;

    public Graph(Template template, ArrayList<Event> events){
        this.predicateManager = template.getPredicateManager();
        this.template = template;
        this.bursts = new ArrayList<>();
        this.finalValues = new HashMap<>();
        this.windowManager = template.getWindowManager();

        loadBursts(events);
        setUtils();
        this.utils = Utils.getInstance();

    }

    public abstract void run();
    public abstract void updateFinalValues(Graphlet graphlet);


    public NoneKleeneGraphlet createNoneSharedGraphlet(ArrayList<Event> burst){
        NoneKleeneGraphlet graphlet = new NoneKleeneGraphlet(burst);
        Utils.getInstance().getGraphletManagerStaticHamlet().addGraphlet(graphlet);
        return graphlet;

    }
    public void setUtils(){

        ArrayList<Integer >queryIds = new ArrayList<>();
        for (int i =0; i< this.template.getWorkload().getQueries().size(); i++){
            queryIds.add(i);
        }

        Aggregator aggregator = this.template.getAggregator();
        Utils.newInstance(events, template, aggregator,queryIds);

        SnapshotManager snapshotManager = new SnapshotManager();
        Utils.getInstance().setSnapshotManager(snapshotManager);

        PredecessorManager predecessorManager = new PredecessorManager();
        Utils.getInstance().setPredecessorManager(predecessorManager);

        NoneKleeneEventCountManager noneKleeneEventCountManager = new NoneKleeneEventCountManager();
        Utils.getInstance().setNoneKleeneEventCountManager(noneKleeneEventCountManager);

        KleeneEventCountManager kleeneEventCountManager = new KleeneEventCountManager();
        Utils.getInstance().setKleeneEventCountManager(kleeneEventCountManager);

        GraphletManager_StaticHamlet graphletManagerStaticHamlet = new GraphletManager_StaticHamlet();
        Utils.getInstance().setGraphletManagerStaticHamlet(graphletManagerStaticHamlet);
    }

    public void loadBursts(ArrayList<Event> events){
        BurstLoader burstLoader = new BurstLoader(predicateManager);
        this.bursts = burstLoader.load(events);
        this.events = burstLoader.getEvents();
    }

    public void printGraphletSnapshot(){
        System.out.printf("\n=====graphlet snapshot=====\n");
        Snapshot snapshot= Utils.getInstance().getSnapshotManager().getLastGraphletSnapshot();

        for (int qid: snapshot.getValues().keySet()){
            System.out.printf("query "+qid+"     count: " +snapshot.getValues().get(qid).getCount()+"\n");
            System.out.printf("query "+qid+"       sum: " +snapshot.getValues().get(qid).getSum()+"\n");

        }
    }

    public void printFinalCount(){
        System.out.printf("\n\n**********Final Values Info**********\n");

        for (int qid: this.finalValues.keySet()){
            String semantic = "";

            System.out.printf("query: "+ qid);
            BigDecimal value = BigDecimal.ZERO;
            Aggregator aggForQuery = this.template.getWorkload().getQueries().get(qid).getAggregator();
            if (aggForQuery.getFunc()== Aggregator.Aggregfunctions.AVG){
                value = this.finalValues.get(qid).avg();
                semantic = "AVG";
            }
            if (aggForQuery.getFunc()== Aggregator.Aggregfunctions.SUM){
                value = this.finalValues.get(qid).getSum();
                semantic = "SUM";

            }
            if (aggForQuery.getFunc()== Aggregator.Aggregfunctions.COUNT){
                value = new BigDecimal(this.finalValues.get(qid).getCount());
                semantic = "COUNT";
            }
            System.out.printf("     "+semantic+": "+ value+"\n");
        }
        System.out.printf("\n\n**********End Final Values Info**********\n");    }

    public void printWorkload(){
        for (Query q: this.template.getWorkload().getQueries())
            System.out.printf(q.getPattern().toString()+"\n");
    }

}
