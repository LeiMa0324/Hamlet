package hamlet.graph;

import hamlet.event.Event;
import hamlet.graphlet.SharedGraphlet;
import hamlet.graphlet.SplittedGraphlet;
import hamlet.template.Template;

import java.math.BigInteger;
import java.util.ArrayList;

/**
 * the dynamic hamlet graph with dynamic sharing decision under the condition of predicates
 * benefit model computes the benefit to share for each burst then give the decision to share or not
 */
public class DynamicGraph extends StaticGraph{


    // the active splitted graphlets
    private ArrayList<SplittedGraphlet> activeSplittedGs;

    //if the first snapshot is created
    private boolean firstSnapshot = true;

    //the number of non-share decisions
    public Integer splitNum;

    //the number of share decisions
    public Integer mergeNum;

    /**
     * constructor of the dynamic graph
     * @param template the hamlet template
     * @param streamFile the stream file
     * @param epw events per window
     * @param burstSize the burst size
     * @param snapshotnum the total number of snapshots due to predicates
     * @param denseBatchPercent the percent of dense bursts in all the bursts.
     *      *                          a dense burst: place a snapshot for each event
     *      *                          a sparse burst: place a snapshot for every three events
     * @param openMsg
     */

    public DynamicGraph(Template template, String streamFile, int epw, Integer burstSize,int snapshotnum,
                        double denseBatchPercent, boolean openMsg) {


        super(template, streamFile, epw, burstSize, snapshotnum, denseBatchPercent, openMsg);

        this.activeSplittedGs = new ArrayList<>();
        this.splitNum = 0;
        this.mergeNum = 0;

    }


    /**
     * This method runs the hamlet with dynamic sharing decision for each burst
     */
    public void dynamicRun() {

        String shared = template.getSharedEvents().get(0);

        //read data in burst
        for (ArrayList<Event> burst: bursts) {

            int actualBatchsize = burstSize <events.size()? burstSize :events.size();

            // mc number of snapshots in a burst
            //dense burst: burst size
            // sparse burst: 3
            int mc = bursts.indexOf(burst)< denseBurstnum ?burst.size():3;

            //k number of queries
            int k = template.getQueries().size();

            //g number of events per graphlet

            //the size of the shared graphlet
            int sharedg = (activeFlag.equals(shared))?Graphlets.get(activeFlag).getEventList().size()+burst.size():burst.size();

            // the size of the non shared graphlet
            int nonsharedg = (activeSplittedGs.isEmpty())?burst.size():(activeSplittedGs.get(0).getEventList().size()+burst.size());

            // p number of preds for each event type in each query
            int p = 1;

            //b number of events in a burst
            int b = burst.size();

            // n number of events
            int n = events.size();

            //mp  number of snapshots in a shared graphlet
            int mp = mc+1;

            int batchindex = bursts.indexOf(burst);

            //the sharing decision
            boolean toShare = isBeneficialToShare(mc,k,p,sharedg,nonsharedg,b,n,mp, batchindex);


            if (toShare){
                //share the burst
                sharedBurst(burst, batchindex);
                mergeNum++;
            }else {
                // split the burst
                splittedBurst(burst);
                splitNum++;
            }

        }
        this.updateFinalCounts();
        System.out.println("Dynamic final count updates:" + finalcountCounter);
        System.out.println("Dynamic snapshot updates:" + snapshotCounter);


    }

    @Override
    /**
     * this method runs the dynamic hamlet with sharing decision for a burst
     * @param burst a burst of events
     * @param index the index of the burst
     */
    void sharedBurst(ArrayList<Event> burst, int index){

        SharedGraphlet shareG = null;

        //if the active graphlet is a shared one
        String lastshared = (activeFlag.equals(template.getSharedEvents().get(0)))?"1":activeFlag;

        switch (lastshared){

            //merge the active graphlet with this burst if it's a shared one
            case "1":

                shareG = (SharedGraphlet)Graphlets.get(activeFlag);

                //add the burst into the graphlet
                ExpandGraphletbyBurst(burst, shareG, index< denseBurstnum);

                //add the active graphlet back to Graphlets
                Graphlets.put(activeFlag, shareG);

                break;

            // create a new shared graphlet if the active graphlet is not a shared one
            default:

                // finish the active graphlet and update the final count
                updateFinalCounts();

                //create a new graphlet level snapshot
                updateSnapshot(burst.get(0), false);

                //create shared Graphlet
                newSharedGraphlet(burst.get(0));

                //create the pred snapshots for the first event
                updatePredSnapshots();

                //empty active splits
                activeSplittedGs.clear();
                shareG = (SharedGraphlet)Graphlets.get(activeFlag);

                //remove the event from the burst
                burst.remove(0);

                //expand the shared graphlet with the burst
                ExpandGraphletbyBurst(burst, shareG, index< denseBurstnum);

                //add G back to Graphlets
                Graphlets.put(activeFlag, shareG);

                break;

        }

        this.memory += burst.size()*12;

    }

    /**
     * this method runs the dynamic hamlet with not sharing decision for a burst
     * the burst is splitted into graphlets for each query
     * @param burst a burst of events
     */
    private void splittedBurst(ArrayList<Event> burst){

        // if the active graphlet is a shared one
        String lastshared = (activeFlag.equals(template.getSharedEvents().get(0)))?"1":activeFlag;

        switch (lastshared){

            //active graphlet is shared
            case "1":

                //finish the active graphlet, update the final count
                updateFinalCounts();

                //create a new graphlet level snapshot
                updateSnapshot(burst.get(0),true);

                //create new splitted graphlets for each query
                newSplittedGraphlets(burst.get(0));

                //remove the first event from the burst
                burst.remove(0);

                break;

                //last graphlet is split, do nothing
            case "-1":
                break;

            //first burst
            default:
                //create a new graphlet level snapshot
                updateSnapshot(burst.get(0),false);

                //create new splitted graphlets for each query
                newSplittedGraphlets(burst.get(0));

                //remove the first event from the burst
                burst.remove(0);


        }

        // expand splitted Graphlets with the rest of the burst

        ExpandSplittedGraphlets(burst);

        this.memory += burst.size()*12*template.getQueries().size();

    }

    @Override
    /**
     * update the final count when a graphlet is finished
     */
    void updateFinalCounts(){

        finalcountCounter++;

        switch (activeFlag){

            //the active graphlet is shared
            case "1":

                //use the super class's final count update method
                super.updateFinalCounts();

                break;

            //if the active graphlets are splitted or empty
            default:

                //empty active graphlets
                if (activeSplittedGs.isEmpty()){
                    for (Integer q=1; q<= template.getQueries().size();q++){
                        this.finalCount.put(q, new BigInteger("0"));
                    }
                }else {
                    //update final counts for active splitted graphlets

                    for (SplittedGraphlet g : activeSplittedGs) {
                        BigInteger previousCount = this.finalCount.get(g.getQid());

                        // pass the inter count of active G to final count
                        this.finalCount.put(g.getQid(), previousCount.add(this.SnapShot.getCounts().get(g.getQid()).multiply(g.getCoeff())));
                    }
                }
                break;
        }
    }


    /**
     * create new splitted graphlets for all the queries
     * @param e the coming event
     */
    private void newSplittedGraphlets(Event e){
        for (Integer qid: e.eventType.getQids()){
            SplittedGraphlet splittedG = new SplittedGraphlet(e, qid);
            activeSplittedGs.add(splittedG);

        }

        //set the active flag =-1 if the active graphlets are splitted
        setActiveFlag("-1");
    }

    /**
     * expand the splitted graphlets with a burst
     * @param burst the given burst
     */
    private void ExpandSplittedGraphlets(ArrayList<Event> burst){

        //add the burst into each splitted graphlet
        for (SplittedGraphlet g: activeSplittedGs){
            g.addEvents(burst);
        }

    }

    /**
     /**
     * update the graphlet level snapshot
     * @param e the coming event
     * @param isLastGraphletShare if the last graphlet is shared
     */
    public void updateSnapshot(Event e, boolean isLastGraphletShare){

        //从上一个graphlet中获得coeff并update snapshot
        snapshotCounter++;

        //for the first snapshot, every value is 1
        if (firstSnapshot) {
            for (Integer qid : e.eventType.getQids()) {
                this.SnapShot.getCounts().put(qid, new BigInteger("1"));

            }
            firstSnapshot = false;

        }else {
            //if the last graphlet is shared, use the super class's update snapshot method
            if (isLastGraphletShare) {
               super.updateSnapshot(e);

            }else {
                    //if the last graphlets are splitted, update the value of the snapshot from each splitted graphlet
                    for (SplittedGraphlet g: activeSplittedGs){
                        this.SnapShot.updatewithPredicate(activeSplittedGs.get(0).getCoeff(), g.getQid());
                    }
                }

        }

//        System.out.println("snapshots: "+SnapShot);
    }


    /**
     * this method calculate the benefit to share for each burst
     * @param mc number of snapshots in a burst
     * @param k number of queries
     * @param p number of predicates for each event in each query
     * @param sharedg the size of the shared graphlet
     * @param nonsharedg the size of the non-shared graphlet
     * @param b number of events in a burst
     * @param n number of events
     * @param mp number of snapshots in a shared graphlet
     * @param burstindex the index of the burst
     * @return to share or not
     */
    public boolean isBeneficialToShare(int mc, int k, int p, int sharedg, int nonsharedg, int b, int n, int mp, int burstindex){


        Double sharedCost = mc*k*sharedg*p+b*(Math.log(sharedg)/Math.log(2)+n*mp);
        Double nonsharedCost = k*b*(Math.log(nonsharedg)/Math.log(2)+n);
        Double benefit = nonsharedCost - sharedCost;

        System.out.println((benefit>0)?"=============Burst "+burstindex+", choose to share============\n":"=============Burst "+burstindex+", choose to split============\n");
//        System.out.println("mc(number of snapshots in a Burst): " + mc+"\n"+
//        "k(number of querie): "+k+"\n"+
//        "shared g(number of events per graphlet):"+sharedg +"\n"+
//        "non shared g(number of events per graphlet):"+nonsharedg +"\n"+
//        "p(number of preds for each event type in each query): "+p+"\n"+
//        "b(Burst size): "+b+"\n"+
//        "mp(number of snapshots propagated): "+mp);
//        System.out.println("shared cost :" +sharedCost);
//        System.out.println("non shared cost :" +nonsharedCost);

        return benefit>0;

    }


}

