package hamlet.graph;

import hamlet.event.Event;
import hamlet.graphlet.SharedGraphlet;
import hamlet.template.Template;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * the static hamlet graph with static sharing decision under the condition of predicates
 * compared with dynamic graph
 */
public class StaticGraph extends Graph{

     Integer batchSize;
     ArrayList<ArrayList<Event>> batches;
     Integer finalcountCounter = 0;
     Integer snapshotCounter = 0;

    //snapshots for each queries
     HashMap<Integer, BigInteger> predSnapshots;
    Integer denseBatchnum = 0;


    public StaticGraph(Template template, String streamFile, int epw, Integer batchSize, int snapshotnum,
                       double denseBatchPercent, boolean openMsg) {

        super(template, streamFile, epw, openMsg);
        this.batchSize = batchSize;
        this.batches = new ArrayList<>();

        this.predSnapshots = new HashMap<>();

        // initialize all snapshots
        for (Integer qid =1;qid<template.getQueries().size();qid++){
            predSnapshots.put(qid, new BigInteger("1"));

        }

        for(int i=0; i<template.getQueries().size()-snapshotnum;i++) {

            Set<Integer> keys = predSnapshots.keySet();
            Iterator iter = keys.iterator();
            Integer key = (Integer) iter.next();
            predSnapshots.remove(key);
        }

        System.out.println(predSnapshots);

        loadBatches();      //reload the events into batches
        denseBatchnum =new BigDecimal(denseBatchPercent+"")
                .multiply(new BigDecimal(batches.size()+"")).intValue(); //the number of dense batches

    }

    /**
     * always share with snapshotNum of snapshots in a batch
     */
    public void staticRun() {

        //read data in batch
        for (ArrayList<Event> batch: batches) {
            sharedBatch(batch, batches.indexOf(batch));

        }

        finishingGraphlet();
        System.out.println("Static final count updates:" + finalcountCounter);
        System.out.println("Static snapshot updates:" + snapshotCounter);
        System.out.println("Static snapshot dense batch number:" + denseBatchnum);
        System.out.println("Static snapshot sparse batch number:" + (batches.size()- denseBatchnum));



    }

     void sharedBatch(ArrayList<Event> batch, int index){

         SharedGraphlet shareG = null;
         boolean isDenseBatch = index<denseBatchnum;

         if (Graphlets.isEmpty()){
             updateSnapshot(batch.get(0));       //新建snapshot
             newSharedGraphlet(batch.get(0));   //new shared graphlet
             updatePredSnapshots();
             batch.remove(0);
        }

         shareG = (SharedGraphlet)Graphlets.get(activeFlag);  //将events加入Graphlet


         ExpandGraphletbyBatch(batch, shareG, isDenseBatch);   //expand graphlet

        Graphlets.put(activeFlag, shareG);  //add G back to Graphlets
        this.memory += batch.size()*12;

    }

    public void updatePredSnapshots(){

        SharedGraphlet shareG = (SharedGraphlet)Graphlets.get(activeFlag);  //将events加入Graphlet

        for (Integer q: predSnapshots.keySet()){
            predSnapshots.put(q, shareG.getCoeff().multiply(this.SnapShot.getCounts().get(q)));
        }
    }

    @Override
    void newSharedGraphlet(Event e){
        super.newSharedGraphlet(e);
    }


    void ExpandGraphletbyBatch(ArrayList<Event> batch, SharedGraphlet g, boolean isDenseBatch) {   //expand the current graphlet

        // dense batch，每一个event都update一次snapshots
        // 非dense batch，一个batch update 三次snapshots
        int eventsperSnapshot = isDenseBatch?1:batchSize/3;
        int eventCounterInBatch = 0;

        assert eventsperSnapshot!= 0;

        for (Event e: batch) {

            if (eventCounterInBatch == eventsperSnapshot) {
                snapshotCounter++;
                //update predicate snapshots
                updatePredSnapshots();

                g.addEvent(e);
                Graphlets.put(activeFlag, g);

                eventCounterInBatch = 0;    //reset events counter

            } else {
                g.addEvent(e);
                Graphlets.put(activeFlag, g);
                eventCounterInBatch++;

            }
        }

    }

    @Override
    public void updateSnapshot(Event e){

        snapshotCounter++;

        for (Integer qid: e.eventType.getQids()){
            //the first snapshot
            if (Graphlets.isEmpty()){
                this.SnapShot.getCounts().put(qid, new BigInteger("1"));

            }else {
                SharedGraphlet g = (SharedGraphlet) Graphlets.get(activeFlag);
                this.SnapShot.updatewithPredicate(g.getCoeff(), qid);

            }

        }

//        System.out.println("snapshots: ");
    }

    @Override
    public boolean finishingGraphlet(){
        if (Graphlets.isEmpty()){
            return false;
        }

        updateFinalCounts();  //update final count
        return true;
    }


    void updateFinalCounts(){

        SharedGraphlet activeG = (SharedGraphlet) Graphlets.get(activeFlag);

        for (Integer q: Graphlets.get(activeFlag).eventType.getQids()){
            BigInteger previousCount = this.finalCount.get(q);
            this.finalCount.put(q,previousCount.add(this.SnapShot.getCounts().get(q).multiply(activeG.getCoeff())));
        }

        finalcountCounter++;
//        System.out.println("final count"+finalCount);


    }

    /**
     * reload events into batches
     */
     void loadBatches(){

        int batchnum = ((events.size()%batchSize>0)?1:0)+(events.size()/batchSize);

        for (int i =0;i<batchnum; i++){
            ArrayList<Event> batch = new ArrayList<>();
            for (int j=0;j<batchSize;j++){
                if (i*batchSize+j==events.size()){
                    break;
                }
                batch.add(events.get(i*batchSize+j));
            }
            batches.add(batch);

        }

    }

}

