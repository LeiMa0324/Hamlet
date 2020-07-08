package hamlet.graph;

import hamlet.event.Event;
import hamlet.graphlet.SharedGraphlet;
import hamlet.graphlet.SplittedGraphlet;
import hamlet.template.Template;

import java.math.BigInteger;
import java.util.ArrayList;


public class DynamicGraph extends StaticGraph{

    private ArrayList<SplittedGraphlet> activeSplittedGs;  // the active splitted graphlets
    private boolean firstSnapshot = true;
    public Integer splitNum;
    public Integer mergeNum;

    public DynamicGraph(Template template, String streamFile, int epw, Integer batchSize,int snapshotnum,
                        double denseBatchPercent, boolean openMsg) {


        super(template, streamFile, epw, batchSize, snapshotnum, denseBatchPercent, openMsg);

        this.activeSplittedGs = new ArrayList<>();
        this.splitNum = 0;
        this.mergeNum = 0;

    }


    /**
     * dynamic decision to split or merge
     */
    public void dynamicRun() {

        String shared = template.getSharedEvents().get(0);

        //read data in batch
        for (ArrayList<Event> batch: batches) {



            int actualBatchsize = batchSize<events.size()?batchSize:events.size();

            // mc number of snapshots in a batch
            //dense batch: batch size, sparse batch: 3
            int mc = batches.indexOf(batch)<denseBatchnum?batch.size():3;
            //k number of queries
            int k = template.getQueries().size();
            //g number of events per graphlet

            //the size of the shared graphlet
            int sharedg = (activeFlag.equals(shared))?Graphlets.get(activeFlag).getEventList().size()+batch.size():batch.size();

            // the size of the non shared graphlet
            int nonsharedg = (activeSplittedGs.isEmpty())?batch.size():(activeSplittedGs.get(0).getEventList().size()+batch.size());
            // p number of preds for each event type in each query
            int p = 1;
            //b number of events in a batch
            int b = batch.size();
            // n number of events
            int n = events.size();

            //mp  number of snapshots in a shared graphlet
            int mp = mc+1;

            int batchindex = batches.indexOf(batch);
            boolean toShare = isBeneficialToShare(mc,k,p,sharedg,nonsharedg,b,n,mp, batchindex);


            if (toShare){
                sharedBatch(batch, batchindex);
                mergeNum++;
            }else {
                splittedBatch(batch);
                splitNum++;
            }

        }
        this.updateFinalCounts();
        System.out.println("Dynamic final count updates:" + finalcountCounter);
        System.out.println("Dynamic snapshot updates:" + snapshotCounter);


    }

    @Override
    void sharedBatch(ArrayList<Event> batch, int index){

        SharedGraphlet shareG = null;
        String lastshared = (activeFlag.equals(template.getSharedEvents().get(0)))?"1":activeFlag;

        switch (lastshared){
            case "1":   //上一个graphlet为shared, merge shared graphlet
                shareG = (SharedGraphlet)Graphlets.get(activeFlag);  //将events加入Graphlet
                ExpandGraphletbyBatch(batch, shareG, index<denseBatchnum);
                Graphlets.put(activeFlag, shareG);  //add G back to Graphlets

                break;

            default:  //上一个graphlet为split或者为空

                updateFinalCounts();

                updateSnapshot(batch.get(0), false);       //新建snapshot
                newSharedGraphlet(batch.get(0));    //create shared Graphlet
                updatePredSnapshots();          //create the pred snapshots for the first event

                activeSplittedGs.clear();       //empty active splits
                shareG = (SharedGraphlet)Graphlets.get(activeFlag);
                batch.remove(0);    //删除第一个元素

                ExpandGraphletbyBatch(batch, shareG, index<denseBatchnum);  //expand the shared graphlet

                Graphlets.put(activeFlag, shareG);  //add G back to Graphlets

                break;

        }

        this.memory += batch.size()*12;

    }


    private void splittedBatch(ArrayList<Event> batch){


        String lastshared = (activeFlag.equals(template.getSharedEvents().get(0)))?"1":activeFlag;

        switch (lastshared){
            case "1":   //last graphlet is shared

                updateFinalCounts();
                updateSnapshot(batch.get(0),true);       //update snapshot
                newSplittedGraphlets(batch.get(0)); //new splitted graphlets
                batch.remove(0);

                break;

            case "-1":  //last graphlet is split
                break;

            default:   //first snapshot
                updateSnapshot(batch.get(0),false);       //update snapshot
                newSplittedGraphlets(batch.get(0)); //new splitted graphlets
                batch.remove(0);


        }

        // expand splitted Graphlets

        ExpandSplittedGraphlets(batch);

        this.memory += batch.size()*12*template.getQueries().size();

    }

    @Override
    void updateFinalCounts(){

        finalcountCounter++;
        switch (activeFlag){
            case "1": //shared graphlet

                //update final count
                super.updateFinalCounts();

                break;

            default:      //splitted graphlets or empty

                //update final counts for last splitted graphlet
                if (activeSplittedGs.isEmpty()){
                    for (Integer q=1; q<= template.getQueries().size();q++){
                        this.finalCount.put(q, new BigInteger("0"));
                    }
                }else {

                    for (SplittedGraphlet g : activeSplittedGs) {
                        BigInteger previousCount = this.finalCount.get(g.getQid());
                        this.finalCount.put(g.getQid(), previousCount.add(this.SnapShot.getCounts().get(g.getQid()).multiply(g.getCoeff()))); // pass the inter count of active G to final count
                    }
                }
                break;
        }
    }



    private void newSplittedGraphlets(Event e){
        for (Integer qid: e.eventType.getQids()){
            SplittedGraphlet splittedG = new SplittedGraphlet(e, qid);
            activeSplittedGs.add(splittedG);

        }

        setActiveFlag("-1");    //set the active flag
    }

    private void ExpandSplittedGraphlets(ArrayList<Event> batch){

        for (SplittedGraphlet g: activeSplittedGs){
            g.addEvents(batch);
        }

    }

    /**
     * update the snapshot before the shared graphlet
     * @param e
     * @param isLastGraphletShare
     */
    public void updateSnapshot(Event e, boolean isLastGraphletShare){
        //从上一个graphlet中获得coeff并update snapshot
        snapshotCounter++;

        if (firstSnapshot) {
            for (Integer qid : e.eventType.getQids()) {
                this.SnapShot.getCounts().put(qid, new BigInteger("1"));

            }
            firstSnapshot = false;
        }else {

            if (isLastGraphletShare) {
               super.updateSnapshot(e);

            }else {

                    for (SplittedGraphlet g: activeSplittedGs){
                        this.SnapShot.updatewithPredicate(activeSplittedGs.get(0).getCoeff(), g.getQid());
                    }
                }

        }

//        System.out.println("snapshots: "+SnapShot);
    }



    public boolean isBeneficialToShare(int mc, int k, int p, int sharedg, int nonsharedg, int b, int n, int mp, int batchindex){


        Double sharedCost = mc*k*sharedg*p+b*(Math.log(sharedg)/Math.log(2)+n*mp);
        Double nonsharedCost = k*b*(Math.log(nonsharedg)/Math.log(2)+n);
        Double benefit = nonsharedCost - sharedCost;

        System.out.println((benefit>0)?"=============batch "+batchindex+", choose to share============\n":"=============batch "+batchindex+", choose to split============\n");
//        System.out.println("mc(number of snapshots in a batch): " + mc+"\n"+
//        "k(number of querie): "+k+"\n"+
//        "shared g(number of events per graphlet):"+sharedg +"\n"+
//        "non shared g(number of events per graphlet):"+nonsharedg +"\n"+
//        "p(number of preds for each event type in each query): "+p+"\n"+
//        "b(batch size): "+b+"\n"+
//        "mp(number of snapshots propagated): "+mp);
//        System.out.println("shared cost :" +sharedCost);
//        System.out.println("non shared cost :" +nonsharedCost);

        return benefit>0;

    }


}

