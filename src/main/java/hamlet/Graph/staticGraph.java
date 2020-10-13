package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.base.Template;
import hamlet.executor.Graphlet;
import hamlet.executor.Snapshot;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class staticGraph extends Graph {



    public staticGraph(Template template, ArrayList<Event> events){
        super(template, events);

    }

    public void run(){

        this.windowManager.initAllWindows(this.events.get(0).getTimeStamp());

        for (ArrayList<Event> burst: this.bursts){
            burstProcess(burst);
        }

    }

    public void burstProcess(ArrayList<Event> burst){

        boolean isKleeneBurst = burst.get(0).getType().isKleene();
        if (isKleeneBurst){
            kleeneBurstProcess(burst);
        }else {
            noneKleeneBurstProcess(burst);
        }

        //check expired queries
        ArrayList<Integer> expiredQueries = windowManager.getExpiredQueries(burst.get(burst.size()-1).getTimeStamp());
        for (Integer qid: expiredQueries){
            System.out.printf("Query "+qid+" has expired!\n");

            //output final counts
            System.out.printf("Count: "+ this.finalCounts.get(qid)+"\n");

            //reset final counts
            this.finalCounts.put(qid, BigInteger.ZERO);

            //reset all snapshots
            this.snapshotManager.resetCountForExpiredQuery(qid);

        }

        windowManager.slideWindows(expiredQueries);

    }

    public void kleeneBurstProcess(ArrayList<Event> burst){

        HashMap<Integer, BigInteger> prefixCounts = getPrefixCountsAfterLastKleeneGraphlet(burst);
        Graphlet graphlet = createKleeneGraphlet(burst, prefixCounts);
        graphlet.propagate();
        updateFinalCount(graphlet);

    }

    public void noneKleeneBurstProcess(ArrayList<Event> burst){

        Graphlet graphlet = createNoneSharedGraphlet(burst);
        graphlet.propagate();
        updateFinalCount(graphlet);

    }

    public HashMap<Integer, BigInteger> getPrefixCountsAfterLastKleeneGraphlet(ArrayList<Event> burst){
        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = this.graphletManager.getGraphletsInRange(graphletManager.getLastKleeneGraphletIndex(),
                graphletManager.getGraphlets().size());

        //get prefix counts for all queries
        return this.predecessorManager.sumPrefixEventCountsForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);

    }

    public Graphlet createKleeneGraphlet(ArrayList<Event> burst, HashMap<Integer, BigInteger> prefixCounts){
        Graphlet graphlet = new Graphlet(burst, this.noneKleeneEventCountManager, this.kleeneEventCountManager, this.queryIds);
        this.graphletManager.addGraphlet(graphlet);

        //add the last graphlet's total count with prefix counts to create a new snapshot
        Graphlet lastKleeneG = this.graphletManager.getGraphlets().get(this.graphletManager.getLastKleeneGraphletIndex());
        this.snapshotManager.createGraphletSnapshot(lastKleeneG, burst.get(0).getEventIndex(), prefixCounts);
        printSnapshot();
        return graphlet;

    }

    public Graphlet createNoneSharedGraphlet(ArrayList<Event> burst){
        Graphlet graphlet = new Graphlet(burst, this.noneKleeneEventCountManager, this.kleeneEventCountManager, this.queryIds);
        this.graphletManager.addGraphlet(graphlet);
        return graphlet;

    }

    /**
     * update the final count if the graphlet is of an end type
     * @param graphlet
     */
    public void updateFinalCount(Graphlet graphlet){


        for (Integer qid: graphlet.getEventType().getQueriesEndWith()) {

            BigInteger oldcount = this.finalCounts.containsKey(qid)?
                    this.finalCounts.get(qid):
                    BigInteger.ZERO;

            BigInteger newCount = oldcount.add(graphlet.getTotalCount().get(qid));
            this.finalCounts.put(qid, newCount);

        }

        printFinalCount();
    }

    public void printSnapshot(){
        System.out.printf("\n=====graphlet snapshot=====\n");
        Snapshot snapshot= this.snapshotManager.getLastGraphletSnapshot();

        for (int qid: snapshot.getCounts().keySet()){
            System.out.printf("query "+qid+"     count: " +snapshot.getCounts().get(qid)+"\n");
        }
    }

    public void printFinalCount(){
        System.out.printf("\n\n**********Final count Info**********\n");

        for (int qid: this.finalCounts.keySet()){
            System.out.printf("query: "+ qid);
            System.out.printf("     count: "+ this.finalCounts.get(qid)+"\n");
        }
        System.out.printf("**********End Final count Info**********\n\n");
    }

}
