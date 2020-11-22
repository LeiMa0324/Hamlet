package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.base.Template;
import hamlet.Graph.Graphlet.Graphlet;
import hamlet.Graph.Graphlet.Static.KleeneGraphlet;
import hamlet.Graph.tools.GraphletManager.GraphletManager_StaticHamlet;
import hamlet.Graph.tools.Utils;
import hamlet.query.aggregator.Value;

import java.util.ArrayList;
import java.util.HashMap;

public class staticGraph extends Graph {


    public staticGraph(Template template, ArrayList<Event> events, ArrayList<ArrayList<Event>> panes){
        super(template, events, panes);
        this.utils.setGraphType(Utils.GraphType.STATIC);
        printWorkload();

    }

    public void run(){

        this.windowManager.initAllWindows(this.events.get(0).getTimeStamp());

        for (int i = 0; i< this.panes.size(); i++){
            ArrayList<Event> burst = this.panes.get(i);
            System.out.printf("Burst number: "+i+"\n");
            burstProcess(burst);
        }


        this.memory = this.events.size()*12+ this.utils.getSnapshotManager().getSnapshots().size()*this.utils.getQueryIds().size()*12;
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

        windowProcess(burst);

    }



    public HashMap<Integer, Value> getPrefixValuesAfterLastKleeneGraphlet(ArrayList<Event> burst){
        GraphletManager_StaticHamlet graphletManagerStaticHamlet = (GraphletManager_StaticHamlet)Utils.getInstance().getGraphletManager();


        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = Utils.getInstance().getGraphletManager().getGraphletsInRange(
                graphletManagerStaticHamlet.getLastKleeneGraphletIndex()==-1?0:graphletManagerStaticHamlet.getLastKleeneGraphletIndex(),
                graphletManagerStaticHamlet.getGraphlets().size());

        //get prefix counts for all queries
        return Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);

    }

    public Graphlet createKleeneGraphlet(ArrayList<Event> burst, HashMap<Integer, Value> prefixCounts){
        Graphlet graphlet = new KleeneGraphlet(burst);

        GraphletManager_StaticHamlet graphletManagerStaticHamlet = (GraphletManager_StaticHamlet)Utils.getInstance().getGraphletManager();

        //add the last graphlet's total count with prefix counts to create a new snapshot
        Graphlet lastKleeneG = (graphletManagerStaticHamlet.getGraphlets().isEmpty()||graphletManagerStaticHamlet.getLastKleeneGraphletIndex()==-1)?
                null:
                graphletManagerStaticHamlet.getGraphlets().get(graphletManagerStaticHamlet.getLastKleeneGraphletIndex());

        //create graphlet snapshot
        Utils.getInstance().getSnapshotManager().createGraphletSnapshot(lastKleeneG, burst.get(0).getEventIndex(), prefixCounts, burst);

        //add the new graphlet into the graphlet list
        Utils.getInstance().getGraphletManager().addGraphlet(graphlet);

        //print the graphlet snapshot
//        printGraphletSnapshot();

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

//        printFinalCount();
    }



}
