package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.Graph.Graphlet.Static.NoneKleeneGraphlet;
import hamlet.base.Snapshot;
import hamlet.Graph.tools.*;
import hamlet.Graph.tools.GraphletManager.GraphletManager_StaticHamlet;
import hamlet.Graph.tools.countManager.KleeneEventCountManager;
import hamlet.Graph.tools.countManager.NoneKleeneEventCountManager;
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
    protected final ArrayList<ArrayList<Event>> panes;
    protected HashMap<Integer, Value> finalValues;
    protected Utils utils;
    protected long latency;
    protected long memory;



    public Graph(Template template, ArrayList<Event> events, ArrayList<ArrayList<Event>> panes){
        this.predicateManager = template.getPredicateManager();
        this.template = template;
        this.panes = panes;
        this.finalValues = new HashMap<>();
        this.windowManager = template.getWindowManager();
        this.events = events;


        setUtils();
        this.utils = Utils.getInstance();

        for (int qid: Utils.getInstance().getQueryIds()){
            this.finalValues.put(qid, Value.ZERO);
        }

    }

    public abstract void run();


    public NoneKleeneGraphlet createNoneSharedGraphlet(ArrayList<Event> burst){
        NoneKleeneGraphlet graphlet = new NoneKleeneGraphlet(burst);
        Utils.getInstance().getGraphletManager().addGraphlet(graphlet);
        return graphlet;

    }
    public void setUtils(){

        ArrayList<Integer >queryIds = new ArrayList<>();
        for (int i =0; i< this.template.getWorkload().getQueries().size(); i++){
            queryIds.add(i);
        }

        Aggregator aggregator = this.template.getAggregator();

        //reset utils
        Utils.reset();
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
        Utils.getInstance().setGraphletManager(graphletManagerStaticHamlet);
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

    public void windowProcess(ArrayList<Event> burst){
        //check expired queries
        ArrayList<Integer> expiredQueries = windowManager.getExpiredQueries(burst.get(burst.size()-1).getTimeStamp());

        for (Integer qid: expiredQueries){
            System.out.printf("Query "+qid+" has expired!\n");

            //output final counts
            System.out.printf("Count: "+ this.finalValues.get(qid)+"\n");

            //reset final counts
            this.finalValues.put(qid, Value.ZERO);

            //reset all snapshots
            Utils.getInstance().getSnapshotManager().resetCountForExpiredQuery(qid);

        }

        windowManager.slideWindows(expiredQueries);

    }

}
