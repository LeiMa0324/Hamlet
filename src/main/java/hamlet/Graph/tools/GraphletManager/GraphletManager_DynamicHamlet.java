package hamlet.Graph.tools.GraphletManager;

import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.Graph.Graphlet.*;
import hamlet.Graph.Graphlet.Dynamic.MergedGraphlet;
import hamlet.Graph.Graphlet.Dynamic.SplittedGraphlets;
import hamlet.Graph.Graphlet.Static.NoneKleeneGraphlet;
import hamlet.Graph.tools.Utils;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class GraphletManager_DynamicHamlet extends GraphletManager{

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
        if (this.lastKleeneGraphletIndex==-1){
            return null;
        }else {
            int gid = this.lastKleeneGraphletIndex;

            return this.graphlets.get(gid) ;
        }
    }

    //share a burst
    public HashMap<Integer, Value> share(ArrayList<Event> burst){

        HashMap<Integer, Value> burstValues = new HashMap<>();

        //case 1: active non-shared
        if (activeFlag == ActiveFlag.NONSHARED||activeFlag ==null){
            burstValues = newMergedGraphlet(burst);
        }

        //case 2: active splits
        if (activeFlag == ActiveFlag.SPLITS){
            burstValues = newMergedGraphlet(burst);

        }

        //case 3: active merged
        if (activeFlag == ActiveFlag.MERGED){
            burstValues = extendMergedGraphlet(burst);
        }

        return burstValues;

    }

    public HashMap<Integer, Value> split(ArrayList<Event> burst){

        HashMap<Integer, Value> burstValues = new HashMap<>();

        //case 1: active non-shared
        if (activeFlag == ActiveFlag.NONSHARED||activeFlag==null){
            burstValues = newSplitGraphlets(burst);
        }

        //case 2: active splits
        if (activeFlag == ActiveFlag.SPLITS){
            burstValues = extendSplitGraphlet(burst);
        }

        //case 3: active merged
        if (activeFlag == ActiveFlag.MERGED){
            burstValues = newSplitGraphlets(burst);
        }

        return burstValues;

    }

    public HashMap<Integer, Value> extendMergedGraphlet(ArrayList<Event> burst){

        MergedGraphlet lastKleeneG = this.lastKleeneGraphletIndex==-1?null: (MergedGraphlet)this.graphlets.get(this.lastKleeneGraphletIndex);
        HashMap<Integer, Value> oldValues = lastKleeneG.getGraphletValues();

        //extend the merged graphlet
        lastKleeneG.extend(burst);

        HashMap<Integer, Value> newValues = lastKleeneG.getGraphletValues();

        //update graphlet list
        this.graphlets.remove(graphlets.size()-1);
        this.kleeneGraphlets.remove(kleeneGraphlets.size()-1);

        this.graphlets.add(lastKleeneG);
        this.kleeneGraphlets.add(lastKleeneG);

        this.lastKleeneGraphletIndex = graphlets.size()-1;
        this.activeFlag = ActiveFlag.MERGED;

        //the increment part
        HashMap<Integer, Value> added = new HashMap<>();
        for (int qid: oldValues.keySet()){
            added.put(qid, newValues.get(qid).substract(oldValues.get(qid)));
        }

        lastKleeneG.printGraphletInfo(burst);

        return added;


    }

    public HashMap<Integer, Value> extendSplitGraphlet(ArrayList<Event> burst){

        SplittedGraphlets lastSplitG =(SplittedGraphlets)this.graphlets.get(this.lastKleeneGraphletIndex);
        HashMap<Integer, Value> oldValues = lastSplitG.getGraphletValues();

        //extend the split graphlet
        lastSplitG.extend(burst);
        lastSplitG.calculateValues();
        HashMap<Integer, Value> newValues = lastSplitG.getGraphletValues();

        this.graphlets.remove(graphlets.size()-1);
        this.kleeneGraphlets.remove(kleeneGraphlets.size()-1);

        this.graphlets.add(lastSplitG);
        this.kleeneGraphlets.add(lastSplitG);

        this.lastKleeneGraphletIndex = graphlets.size()-1;
        this.activeFlag = ActiveFlag.SPLITS;

        //the increment part
        HashMap<Integer, Value> added = new HashMap<>();
        for (int qid: oldValues.keySet()){
            added.put(qid, newValues.get(qid).substract(oldValues.get(qid)));
        }




        lastSplitG.printGraphletInfo(burst);

        return added;

    }

    public HashMap<Integer, Value> newMergedGraphlet(ArrayList<Event> burst){


        MergedGraphlet graphlet = new MergedGraphlet(burst);

        //add the last kleene graphlet's total count with prefix counts to create a new snapshot
        //could be splits or merged
        Graphlet lastKleeneG = (this.graphlets.isEmpty()||this.lastKleeneGraphletIndex==-1)?
                null:
                this.graphlets.get(this.lastKleeneGraphletIndex);

        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = getGraphletsInRange(
                this.lastKleeneGraphletIndex==-1?0:this.lastKleeneGraphletIndex,
                this.graphlets.size());

        //get prefix counts from candidate graphlets for all queries
        HashMap<Integer, Value> prefixValues = Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);


        //create graphlet snapshot
        Utils.getInstance().getSnapshotManager().createGraphletSnapshot(lastKleeneG, burst.get(0).getEventIndex(), prefixValues, burst);

        //propagate
        graphlet.propagate();

        //add the new graphlet into the graphlet list
        addGraphlet(graphlet);

        graphlet.printGraphletInfo(burst);

        return graphlet.getGraphletValues();


    }

    public HashMap<Integer, Value> newSplitGraphlets(ArrayList<Event> burst){

        SplittedGraphlets splittedGraphlets = new SplittedGraphlets(burst);

        //add the last kleene graphlet's total count with prefix counts to get values for queries
        //could be splits or merged but doesn't matter, only value is needed
        Graphlet lastKleeneG = (this.graphlets.isEmpty()||this.lastKleeneGraphletIndex==-1)?
                null:
                this.graphlets.get(this.lastKleeneGraphletIndex);

        //get candidate graphlets
        HashMap<EventType, ArrayList<Graphlet>> candidateGraphlets = getGraphletsInRange(
                this.lastKleeneGraphletIndex==-1?0: this.lastKleeneGraphletIndex,
                this.graphlets.size());

        //get prefix counts from candidate graphlets for all queries
        HashMap<Integer, Value> prefixValues = Utils.getInstance().getPredecessorManager().sumPrefixEventValuesForKleeneEventTypeForAllQueries(burst.get(0).getType(), candidateGraphlets);


        //values = prefix count+ last kleene.value
        HashMap<Integer, Value> values = new HashMap<>();

        for (Integer qid : Utils.getInstance().getQueryIds()) {
            Value value = lastKleeneG==null?prefixValues.get(qid):
                    lastKleeneG.getGraphletValues().get(qid).add(prefixValues.get(qid));

            values.put(qid, value);
        }

        //add one for start queries
        ArrayList<Integer> validStartQueries = burst.get(0).getType().getQueriesStartWith();
        validStartQueries.retainAll(burst.get(0).getValidQueries());

        for (int qid: validStartQueries){
            Value oldValue = values.get(qid);
            //increment one for start queries
            Value newValue = oldValue.add(Value.ONE);
            values.put(qid, newValue);
        }

        //pass the values to the splitted one
        splittedGraphlets.setInputValues(values);

        //calculate the graphlet values
        splittedGraphlets.calculateValues();

        //add the new splitted graphlet into the graphlet list
        addGraphlet(splittedGraphlets);

        splittedGraphlets.printGraphletInfo(burst);

        return splittedGraphlets.getGraphletValues();

    }

    public HashMap<Integer, Value> newNoneSharedGraphlet(ArrayList<Event> burst){

        NoneKleeneGraphlet graphlet = new NoneKleeneGraphlet(burst);
        graphlet.propagate();
        addGraphlet(graphlet);
        return graphlet.getGraphletValues();

    }


    /**
     * get the params of the cost model
     * @return
     */
    public HashMap<String, Integer> getParams(ArrayList<Event> burst){


        int lastKleeneSize = (this.graphlets.isEmpty()|| this.lastKleeneGraphletIndex==-1)?0:this.graphlets.get(lastKleeneGraphletIndex).getEvents().size();

        HashMap<String, Integer> params = new HashMap<>();

        //sc: number of snapshots created in a burst
        int sc;
        //if one event has different predecessors, then an event-level snapshot is created
        sc = 0;
        for (Event event: burst){
            if (event.isHasSnapshot()){
                sc+=1;
            }
        }
        params.put("sc", sc);

        //sp number of snapshots propagated to a shared graphlet
        int sp;
        sp = activeFlag== ActiveFlag.MERGED?
                ((MergedGraphlet)this.graphlets.get(lastKleeneGraphletIndex)).getSnapshotNum()
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
