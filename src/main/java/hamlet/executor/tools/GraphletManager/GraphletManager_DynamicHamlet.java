package hamlet.executor.tools.GraphletManager;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.Graphlet.KleeneGraphlet;
import hamlet.executor.Graphlet.NoneKleeneGraphlet;
import hamlet.executor.Graphlet.SplittedGraphlets;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class GraphletManager_DynamicHamlet extends GraphletManager{
    private ArrayList<Graphlet> graphlets;
    private ArrayList<Graphlet> kleeneGraphlets;

    private int lastKleeneGraphletIndex = -1;
    private ActiveFlag activeFlag;

    public GraphletManager_DynamicHamlet(){
        this.graphlets = new ArrayList<>();
        this.kleeneGraphlets = new ArrayList<>();
    }

    public void addGraphlet(Graphlet graphlet){
        this.graphlets.add(graphlet);

        if (graphlet.getEventType().isKleene()){
            this.lastKleeneGraphletIndex = this.graphlets.size()-1;
            this.kleeneGraphlets.add(graphlet);
            activeFlag = graphlet.getType()== Graphlet.GraphletType.Kleene?ActiveFlag.MERGED: ActiveFlag.SPLITS;

        }
        activeFlag = ActiveFlag.NONSHARED;
    }

    public Graphlet getLastKleeneGraphlet(){
        int gid = this.lastKleeneGraphletIndex;
        return this.activeFlag == ActiveFlag.MERGED? (KleeneGraphlet) this.graphlets.get(gid):
                (SplittedGraphlets) this.graphlets.get(gid);
    }

    //share a burst
    public void share(ArrayList<Event> burst){
        //case 1: active non-shared
        if (activeFlag == ActiveFlag.NONSHARED){
            newSharedGraphlet(burst);
        }

        //case 2: active splits
        if (activeFlag == ActiveFlag.SPLITS){
            newSharedGraphlet(burst);
        }

        //case 3: active merged
        if (activeFlag == ActiveFlag.MERGED){
            extendSharedGraphlet(burst);
        }

    }

    public void split(ArrayList<Event> burst){
        //case 1: active non-shared
        if (activeFlag == ActiveFlag.NONSHARED){
            newSplitGraphlets(burst);
        }

        //case 2: active splits
        if (activeFlag == ActiveFlag.SPLITS){
            extendSplitGraphlet(burst);
        }

        //case 3: active merged
        if (activeFlag == ActiveFlag.MERGED){
            newSplitGraphlets(burst);
        }


    }

    //todo
    public void extendSharedGraphlet(ArrayList<Event> burst){

    }

    public void extendSplitGraphlet(ArrayList<Event> burst){

        SplittedGraphlets lastSplitG = (SplittedGraphlets)this.graphlets.get(this.lastKleeneGraphletIndex);

        lastSplitG.extend(burst);
        lastSplitG.calculateValues();
        this.graphlets.remove(graphlets.size()-1);
        this.kleeneGraphlets.remove(kleeneGraphlets.size()-1);

        this.graphlets.add(lastSplitG);
        this.kleeneGraphlets.add(lastSplitG);

    }

    public void newSharedGraphlet(ArrayList<Event> burst){

        KleeneGraphlet graphlet = new KleeneGraphlet(burst);

        //add the last kleene graphlet's total count with prefix counts to create a new snapshot
        //could be splits or merged
        Graphlet lastKleeneG = this.graphlets.isEmpty()?
                null:
                this.graphlets.get(this.lastKleeneGraphletIndex);

        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = getGraphletsInRange(
                this.lastKleeneGraphletIndex,
                this.graphlets.size());

        //get prefix counts from candidate graphlets for all queries
        HashMap<Integer, Value> prefixValues = Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);


        //create graphlet snapshot
        Utils.getInstance().getSnapshotManager().createGraphletSnapshot(lastKleeneG, burst.get(0).getEventIndex(), prefixValues);

        //add the new graphlet into the graphlet list
        addGraphlet(graphlet);


    }

    public void newSplitGraphlets(ArrayList<Event> burst){

        SplittedGraphlets splittedGraphlets = new SplittedGraphlets(burst);

        //add the last kleene graphlet's total count with prefix counts to get values for queries
        //could be splits or merged but doesn't matter, only value is needed
        Graphlet lastKleeneG = this.graphlets.isEmpty()?
                null:
                this.graphlets.get(this.lastKleeneGraphletIndex);

        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = getGraphletsInRange(
                this.lastKleeneGraphletIndex,
                this.graphlets.size());

        //get prefix counts from candidate graphlets for all queries
        HashMap<Integer, Value> prefixValues = Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);


        //values = prefix count+ last kleene.value
        HashMap<Integer, Value> values = new HashMap<>();

        for (Integer qid : prefixValues.keySet()) {
            Value value = lastKleeneG==null?prefixValues.get(qid):
                    lastKleeneG.getGraphletValues().get(qid).add(prefixValues.get(qid));

            values.put(qid, value);
        }

        //pass the values to the splitted one
        splittedGraphlets.setInputValues(values);

        //calculate the graphlet values
        splittedGraphlets.calculateValues();

        //add the new splitted graphlet into the graphlet list
        addGraphlet(splittedGraphlets);

    }

    public void newNoneSharedGraphlet(ArrayList<Event> burst){
        NoneKleeneGraphlet graphlet = new NoneKleeneGraphlet(burst);
        //todo: 重写update逻辑
        graphlet.propagate();
        addGraphlet(graphlet);

    }


    /**
     * get the params of the cost model
     * @return
     */
    public HashMap<String, Integer> getParams(ArrayList<Event> burst){


        int lastKleeneSize = (this.graphlets.isEmpty()|| this.lastKleeneGraphletIndex==-1)?0:this.graphlets.get(lastKleeneGraphletIndex).getEvents().size();

        HashMap<String, Integer> params = new HashMap<>();

        //sc: number of snapshots created in a shared graphlet
        int sc;
        //if one event has different predecessors, then an event-level snapshot is created
        sc = Utils.getInstance().getPredecessorManager().hasSamePredecessorsForValidQueries(burst.get(0))?
                burst.size():0;
        params.put("sc", sc);

        //sp number of snapshots propagated to a shared graphlet
        int sp;
        sp = activeFlag== ActiveFlag.MERGED?
                0         //todo: merged graphlet 内的snapshots+这个burst的snapshots+1
                :sc+1;

        params.put("sp", sp);

        //k number of queries
        int k;
        k = Utils.getInstance().getQueryIds().size();
        params.put("k", k);

        //p number of predecessor graphlets
        int p = 1;
        params.put("p",p);

        //sharedg size of shared graphlet
        int sharedg;

        sharedg = activeFlag== ActiveFlag.MERGED?
                lastKleeneSize+burst.size():
                burst.size();
        params.put("sharedg", sharedg);

        //splitg the size of the splitted graphlet
        int splitg = this.activeFlag== ActiveFlag.MERGED?
                burst.size():
                burst.size()+lastKleeneSize;
        params.put("splitg",splitg);

        //b number of events in a burst
        int b = burst.size();
        params.put("b",b);

        //n number of existing events
        int n = burst.get(0).getEventIndex();
        params.put("n",n);

        return params;

    }


    public enum ActiveFlag{
        SPLITS,
        MERGED,
        NONSHARED
    }
}
