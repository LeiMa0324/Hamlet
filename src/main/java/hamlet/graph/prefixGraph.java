package hamlet.graph;

import hamlet.event.Event;
import hamlet.event.StreamLoader;
import hamlet.graphlet.Graphlet;
import hamlet.graphlet.NonSharedGraphlet;
import hamlet.graphlet.SharedGraphlet;
import hamlet.template.EventType;
import hamlet.template.Template;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

public class prefixGraph extends Graph {

    private HashMap<String, ArrayList<Graphlet>> Graphlets;   //a list of Graphlets
    private Graphlet activeGraphlet;
    private Snapshot localSnapshot;
    private HashMap<Integer, Boolean> localSnapshotComplete;

    public prefixGraph(Template template, String streamFile, int epw, boolean openMsg) {
        super( template,  streamFile,  epw,  openMsg);
        this.Graphlets = new HashMap<>();
        this.localSnapshot = new Snapshot();
        this.localSnapshotComplete = new HashMap<>();
    }

    @Override
    public void run(){

        for (Event e: events){
            boolean expandGraphlet = false;


            if ((!(activeGraphlet==null)) && activeGraphlet.eventType.string.equals(e.string)){
                expandGraphlet = true;
            }


            if (expandGraphlet){
                super.ExpandGraphlet(e, activeGraphlet);
            }

            if (!expandGraphlet){           //new a graphlet

                updateActiveGraphletCount();   //finish the last graphlt

                if (e.eventType.isShared){
                    updateSnapshot(e);       //update snapshot
                    newSharedGraphlet(e);

                }else {
                    newNonSharedGraphlet(e);
                }

            }

        }

//        System.out.println("final count"+ finalCount);
        updateActiveGraphletCount();

    }

    @Override
    public void newSharedGraphlet(Event e){
        super.newSharedGraphlet(e);
        activeGraphlet = lastSharedG;
        addActiveGraphlet(e.string);
    }

    public void newNonSharedGraphlet(Event e){

        activeGraphlet= new NonSharedGraphlet(e);
        addActiveGraphlet(e.string);


    }

    public void addActiveGraphlet(String et){

        ArrayList<Graphlet> graphletList = Graphlets.get(et);
        if (graphletList==null){
            graphletList = new ArrayList<>();
        }
        graphletList.add(activeGraphlet);
        Graphlets.put(et, graphletList);
    }


    public boolean updateActiveGraphletCount() {

        if (activeGraphlet==null){
            return false;
        }

        //update inter count for shared graphlet
        if (activeGraphlet.isShared){
            ((SharedGraphlet)activeGraphlet).updateCounts(this.SnapShot);
            for (Integer qid: activeGraphlet.eventType.getQids()) {
                ((SharedGraphlet) activeGraphlet).interCounts.put(qid, this.SnapShot.getCounts().get(qid).multiply(
                        ((SharedGraphlet) activeGraphlet).getCoeff()
                ));
            }
        }

        //update inter count for non shared graphlet
        if (!(activeGraphlet.isShared)){

            for (Integer qid: activeGraphlet.eventType.getQids()){

                /**
                 * START EVENT
                 */
                if (activeGraphlet.eventType.getTypes().get(qid).contains("START")){
                    activeGraphlet.interCounts.put(qid, new BigInteger(activeGraphlet.getEventList().size()+""));

                    localSnapshot.getCounts().put(qid, activeGraphlet.interCounts.get(qid));    //initialize local snapshot
                }
                /**
                 * NOT START EVENT
                 */
                else {
                    ArrayList<Graphlet> predGs = findPredecessors(qid, activeGraphlet.eventType);
                    if (predGs==null){
                        return false;
                    }
                    Graphlet latestPred = predGs.get(predGs.size()-1);  //the latest predcessor

                    //local inter count: pred count * the number of events in the graphlet
                    activeGraphlet.interCounts.put(qid, latestPred.interCounts.get(qid).multiply(
                           new BigInteger( activeGraphlet.getEventList().size()+"")
                    ));

                    localSnapshot.getCounts().put(qid, activeGraphlet.interCounts.get(qid));    //update local snapshot

                }

            }
        }

        // if the graphlet is END EVENT, re - update inter count
        if (! activeGraphlet.eventType.getEndQueries().isEmpty()){

            for (Integer qid: activeGraphlet.eventType.getEndQueries()){
                ArrayList<Graphlet> predGs = findPredecessors(qid, activeGraphlet.eventType);

                if (predGs!=null) {
                    BigInteger sum = BigInteger.ZERO;

                    for (Graphlet p : predGs) {
                        sum = sum.add(p.interCounts.get(qid));
                    }

                    // intercount is sum(all pred)
                    activeGraphlet.interCounts.put(qid, sum.multiply(new BigInteger("" + activeGraphlet.getEventList().size() + "")));

                    BigInteger count = finalCount.get(qid);
                    finalCount.put(qid, count.add(activeGraphlet.interCounts.get(qid))); //increment final count
//                System.out.println(finalCount);


                    localSnapshot.getCounts().put(qid, BigInteger.ZERO);  //reset local snapshot
                }
                else {
                    BigInteger count = finalCount.get(qid);
                    finalCount.put(qid, count.add(activeGraphlet.interCounts.get(qid))); //increment final count
                }
            }

        }
        return true;

    }

    @Override
    public void updateSnapshot(Event e){

        for (Integer qid: e.eventType.getQids()){

            //first snapshot
            if (!SnapShot.getCounts().keySet().contains(qid)){
                // 是start event时为1，否则为0
                if (e.eventType.getTypes().get(qid).contains("START")) {
                    this.SnapShot.getCounts().put(qid,BigInteger.ONE);
                }else {
                    this.SnapShot.getCounts().put(qid,BigInteger.ZERO);
                }
            }else {     //not first snapshot

                ArrayList<Graphlet> prefGs = findPredecessors(qid, e.eventType);

                if (prefGs == null) {
                    BigInteger shot = this.SnapShot.getCounts().get(qid);
                    this.SnapShot.getCounts().put(qid, shot.multiply(lastSharedG.getCoeff().add(BigInteger.ONE)).add(BigInteger.ONE));
                } else {
                    NonSharedGraphlet pred = (NonSharedGraphlet) prefGs.get(prefGs.size() - 1);
                    this.SnapShot.update(lastSharedG.getCoeff(), pred, qid);
                }
            }

        }
//        System.out.println("snapshot update:"+SnapShot);


    }

    public ArrayList<Graphlet> findPredecessors(Integer qid, EventType et){

        EventType predEt = et.getPred(qid);  //找到predecessor的类型

        return predEt==null?null:Graphlets.get(predEt.string);

    }

    }
