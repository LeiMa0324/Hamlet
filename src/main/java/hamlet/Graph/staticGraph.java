package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.base.Template;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.Graphlet.KleeneGraphlet;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Value;

import java.util.ArrayList;
import java.util.HashMap;

public class staticGraph extends Graph {



    public staticGraph(Template template, ArrayList<Event> events){
        super(template, events);
        printWorkload();

    }

    public void run(){

        this.windowManager.initAllWindows(this.events.get(0).getTimeStamp());

        for (ArrayList<Event> burst: this.bursts){
            burstProcess(burst);
        }

    }

    public void burstProcess(ArrayList<Event> burst){

        boolean isKleeneBurst = burst.get(0).getType().isKleene();
        Graphlet graphlet;
        if (isKleeneBurst){

            HashMap<Integer, Value> prefixCounts = getPrefixValuesAfterLastKleeneGraphlet(burst);
            graphlet = createKleeneGraphlet(burst, prefixCounts);

        }else {
            graphlet = createNoneSharedGraphlet(burst);
        }

        graphlet.propagate();
        updateFinalValues(graphlet);

//        windowProcess(burst);

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

    public HashMap<Integer, Value> getPrefixValuesAfterLastKleeneGraphlet(ArrayList<Event> burst){
        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = Utils.getInstance().getGraphletManagerStaticHamlet().getGraphletsInRange(
                Utils.getInstance().getGraphletManagerStaticHamlet().getLastKleeneGraphletIndex(),
                Utils.getInstance().getGraphletManagerStaticHamlet().getGraphlets().size());

        //get prefix counts for all queries
        return Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);

    }

    public Graphlet createKleeneGraphlet(ArrayList<Event> burst, HashMap<Integer, Value> prefixCounts){
        Graphlet graphlet = new KleeneGraphlet(burst);

        //add the last graphlet's total count with prefix counts to create a new snapshot
        Graphlet lastKleeneG = Utils.getInstance().getGraphletManagerStaticHamlet().getGraphlets().isEmpty()?
                null:
                Utils.getInstance().getGraphletManagerStaticHamlet().getGraphlets().get(Utils.getInstance().getGraphletManagerStaticHamlet().getLastKleeneGraphletIndex());

        //create graphlet snapshot
        Utils.getInstance().getSnapshotManager().createGraphletSnapshot(lastKleeneG, burst.get(0).getEventIndex(), prefixCounts);

        //add the new graphlet into the graphlet list
        Utils.getInstance().getGraphletManagerStaticHamlet().addGraphlet(graphlet);

        //print the graphlet snapshot
        printGraphletSnapshot();

        return graphlet;

    }



    /**
     * update the final count if the graphlet is of an end type
     * @param graphlet
     */
    public void updateFinalValues(Graphlet graphlet){


        for (Integer qid: graphlet.getEventType().getQueriesEndWith()) {

            //UPDATE GRAPH TOTAL VALUES
            Value oldcount = this.finalValues.containsKey(qid)?
                    this.finalValues.get(qid):
                    Value.ZERO;

            Value newCount = oldcount.add(graphlet.getGraphletValues().get(qid));
            this.finalValues.put(qid, newCount);

        }

        printFinalCount();
    }



}
