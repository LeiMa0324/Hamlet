package hamlet.aoldgraph;

import hamlet.aoldevent.Event;
import hamlet.aoldgraphlet.SharedGraphlet;
import hamlet.aoldtemplate.Template;

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

    // the burst size
     Integer burstSize;

     // the array of all bursts
     ArrayList<ArrayList<Event>> bursts;


     Integer finalcountCounter = 0;

     // the counter for snapshots
     Integer snapshotCounter = 0;

    //snapshots for each queries
     HashMap<Integer, BigInteger> predSnapshots;

     //the number of dense bursts
    Integer denseBurstnum = 0;

    /**
     * the constructor of a static sharing hamlet graph
     * @param template the hamlet template
     * @param streamFile the stream file
     * @param epw events per Window
     * @param burstSize the size of a burst
     * @param snapshotnum the total number of snapshots due to predicates
     * @param denseBurstPercent the percent of dense bursts in all the bursts.
     *                          a dense burst: place a snapshot for each event
     *                          a sparse burst: place a snapshot for every three events
     * @param openMsg
     */
    public StaticGraph(Template template, String streamFile, int epw, Integer burstSize, int snapshotnum,
                       double denseBurstPercent, boolean openMsg) {

        super(template, streamFile, epw, openMsg);
        this.burstSize = burstSize;
        this.bursts = new ArrayList<>();
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

        //reload the events into bursts
        loadBatches();

        //the number of dense bursts
        denseBurstnum =new BigDecimal(denseBurstPercent+"")
                .multiply(new BigDecimal(bursts.size()+"")).intValue();

    }

    /**
     * This method runs the hamlet with static decision for each burst
     */
    public void staticRun() {

        //read data in burst
        for (ArrayList<Event> burst: bursts) {

            //run hamlet for each burst
            sharedBurst(burst, bursts.indexOf(burst));

        }

        // finish the graphlet and update the final counts if necessary
        finishingGraphlet();

        System.out.println("Static final count updates:" + finalcountCounter);
        System.out.println("Static snapshot updates:" + snapshotCounter);
        System.out.println("Static snapshot dense batch number:" + denseBurstnum);
        System.out.println("Static snapshot sparse batch number:" + (bursts.size()- denseBurstnum));

    }

    /**
     * this method runs the hamlet with static sharing decision for a burst
     * @param burst a burst of events
     * @param index the index of the burst
     */
     void sharedBurst(ArrayList<Event> burst, int index){

         SharedGraphlet shareG = null;

         //first denseBurstnum of bursts are dense burst
         boolean isDenseBatch = index < denseBurstnum;

         if (Graphlets.isEmpty()){

             //update the graphlet snapshot
             updateSnapshot(burst.get(0));

             //create a new shared graphlet
             newSharedGraphlet(burst.get(0));

             //update the event level snapshots
             updatePredSnapshots();

             //remove the event from the burst
             burst.remove(0);
        }

         //get the active graphlet
         shareG = (SharedGraphlet)Graphlets.get(activeFlag);

         //expand graphlet
         ExpandGraphletbyBurst(burst, shareG, isDenseBatch);

         //add G back to Graphlets
        Graphlets.put(activeFlag, shareG);
        this.memory += burst.size()*12;

    }

    /**
     * update the event level snapshots due to predicates
     */

    public void updatePredSnapshots(){

        //get the active graphlet
        SharedGraphlet shareG = (SharedGraphlet)Graphlets.get(activeFlag);

        //update the event-level predicates for each query
        for (Integer q: predSnapshots.keySet()){
            predSnapshots.put(q, shareG.getCoeff().multiply(this.SnapShot.getCounts().get(q)));
        }
    }


    /**
     * expand an active graphlet by a burst of events
     * @param burst a burst of events
     * @param g the active graphlet
     * @param isDenseBatch is this burst a dense one
     */
    void ExpandGraphletbyBurst(ArrayList<Event> burst, SharedGraphlet g, boolean isDenseBatch) {

        // dense burst，update a snapshot for each event
        // sparse burst，update a snapshot for every three events
        int eventsperSnapshot = isDenseBatch?1: burstSize /3;

        //counter for events in a burst
        int eventCounterInBatch = 0;

        assert eventsperSnapshot!= 0;

        //for each event in the burst
        for (Event e: burst) {

            if (eventCounterInBatch == eventsperSnapshot) {

                //increment snapshot counter
                snapshotCounter++;

                //update event level snapshots
                updatePredSnapshots();

                //add event into the graphlet
                g.addEvent(e);

                //add graphlet into the graphlet list
                Graphlets.put(activeFlag, g);

                //reset events counter
                eventCounterInBatch = 0;

            } else {
                g.addEvent(e);
                Graphlets.put(activeFlag, g);
                eventCounterInBatch++;

            }
        }

    }

    @Override
    /**
     * update the graphlet level snapshot
     */
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
    /**
     * finish the graphlet and update the final count
     */
    public boolean finishingGraphlet(){
        if (Graphlets.isEmpty()){
            return false;
        }

        updateFinalCounts();  //update final count
        return true;
    }

    /**
     * update the final count when the graphlet is of a end event type
     */
    void updateFinalCounts(){

        SharedGraphlet activeG = (SharedGraphlet) Graphlets.get(activeFlag);

        //update the final count for each query
        for (Integer q: Graphlets.get(activeFlag).eventType.getQids()){
            BigInteger previousCount = this.finalCount.get(q);
            this.finalCount.put(q,previousCount.add(this.SnapShot.getCounts().get(q).multiply(activeG.getCoeff())));
        }

        finalcountCounter++;
//        System.out.println("final count"+finalCount);

    }

    /**
     * load events into bursts
     */
     void loadBatches(){

        int batchnum = ((events.size()% burstSize >0)?1:0)+(events.size()/ burstSize);

        for (int i =0;i<batchnum; i++){
            ArrayList<Event> batch = new ArrayList<>();
            for (int j = 0; j< burstSize; j++){
                if (i* burstSize +j==events.size()){
                    break;
                }
                batch.add(events.get(i* burstSize +j));
            }
            bursts.add(batch);

        }

    }

}

