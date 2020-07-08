package hamlet.graph;

import hamlet.event.Event;
import hamlet.graphlet.Graphlet;
import hamlet.graphlet.NonSharedGraphlet;
import hamlet.graphlet.SharedGraphlet;
import hamlet.template.EventType;
import hamlet.template.Template;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * this class contains the hamlet Graph without the predicates
 */
public class hamletGraph extends Graph {

    //a list of Graphlets
    private HashMap<String, ArrayList<Graphlet>> Graphlets;

    //the active Graphlet
    private Graphlet activeGraphlet;

    public hamletGraph(Template template, String streamFile, int epw, boolean openMsg) {
        super( template,  streamFile,  epw,  openMsg);
        this.Graphlets = new HashMap<>();
    }

    /**
     * run the graph
     */
    public void run(){

        // for each event
        for (Event e: events){


            boolean expandGraphlet = false;

            /**
             * if the active graphlet is of the same event type of this event
             * expand the active graphlet
             */
            if ((!(activeGraphlet==null)) && activeGraphlet.eventType.string.equals(e.string)){
                expandGraphlet = true;
            }


            if (expandGraphlet){
                super.ExpandGraphlet(e, activeGraphlet);
            }

            /**
             * else, create a new graphlet
             */

            if (!expandGraphlet){

                //finish the last graphlt
                updateActiveGraphletCount();

                if (e.eventType.isShared){

                    //update snapshot
                    updateSnapshot(e);

                    //create a shared graphlet
                    newSharedGraphlet(e);

                }else {

                    //create a nonshared graphlet
                    newNonSharedGraphlet(e);
                }

            }

        }

        //update the last graphlet
        updateActiveGraphletCount();

//        System.out.println("prefix graph final count"+ finalCount);


    }

    @Override

    /**
     * create a new shared graphlet
     * @param e the coming event
     */
    public void newSharedGraphlet(Event e){

        //call function from super class
        super.newSharedGraphlet(e);

        //set up the active graphlet
        activeGraphlet = lastSharedG;

        //add the active graphlet into the list of Graphlets
        addActiveGraphlet(e.string);
    }

    /**
     * create a new non-shared graphlet
     * @param e the coming event
     */
    public void newNonSharedGraphlet(Event e){

        //create a new non-shared graphlet based on the coming event
        activeGraphlet= new NonSharedGraphlet(e);

        //add the active graphlet into the list of Graphlets
        addActiveGraphlet(e.string);


    }
    /**
     * add the active graphlet into the list of graphlets
     * @param eventString the event string of the active graphlet
     */
    public void addActiveGraphlet(String eventString){

        ArrayList<Graphlet> graphletList = Graphlets.get(eventString);
        if (graphletList==null){
            graphletList = new ArrayList<>();
        }
        graphletList.add(activeGraphlet);
        Graphlets.put(eventString, graphletList);
    }

    /**
     * update the intermediate count for a graphlet
     * @return
     */
    public boolean updateActiveGraphletCount() {

        if (activeGraphlet==null){
            return false;
        }

        /**
         *update inter count for shared graphlet
         */
        if (activeGraphlet.isShared){
            updateSharedGraphletCount();
        }

        /**
         *update inter count for non shared graphlet
         */
        if (!(activeGraphlet.isShared)){
            updateNonSharedGraphletCount();
        }
//
//        System.out.println("graphlet "+ activeGraphlet.eventType.string);
//        System.out.println("inter count"+activeGraphlet.interCounts);

        /**
         * if this graphlet is of an END EVENT
         * increment final count with the graphlet's intercount
         */
        if (! activeGraphlet.eventType.getEndQueries().isEmpty()){

            // for each query that ends with this event type
            for (Integer qid: activeGraphlet.eventType.getEndQueries()){

                    BigInteger count = finalCount.get(qid);

                //increment final count
                    finalCount.put(qid, count.add(activeGraphlet.interCounts.get(qid)));

            }

        }

        return true;

    }

    /**
     * update the inter count of a shared graphlet
     * inter count = sum of all events' inter count
     */
    public void updateSharedGraphletCount(){

        ((SharedGraphlet)activeGraphlet).updateCounts(this.SnapShot);

        for (Integer qid: activeGraphlet.eventType.getQids()) {
            ((SharedGraphlet) activeGraphlet).interCounts.put(qid, this.SnapShot.getCounts().get(qid).multiply(
                    ((SharedGraphlet) activeGraphlet).getCoeff()
            ));
        }

    }

    /**
     * update the inter count of a non-shared graphlet
     * inter count = sum of all events' inter count
     */
    public void updateNonSharedGraphletCount(){


        for (Integer qid: activeGraphlet.eventType.getQids()){

            /**
             * START EVENT
             * graphlet intercount = event number
             */
            if (activeGraphlet.eventType.getTypes().get(qid).contains("START")){
                activeGraphlet.interCounts.put(qid, new BigInteger(activeGraphlet.getEventList().size()+""));
            }

            /**
             * NOT START EVENT
             * graphlet intercount = first event's intercount * event number
             */

            else {

                //first event's intercount = sum(pred graphlets)
                BigInteger predSum = sumPredecessorsInterCount(qid, activeGraphlet.eventType);

                /**
                 * all events in the graphlet has the same inter count
                 */
                activeGraphlet.interCounts.put(qid, predSum.multiply(
                        new BigInteger( activeGraphlet.getEventList().size()+"")
                ));

            }
        }
    }

    @Override
    public void updateSnapshot(Event e){

        for (Integer qid: e.eventType.getQids()){

            /**
             * if it's the first snapshot for the start event
             */
            if ((!SnapShot.getCounts().containsKey(qid))&&e.eventType.getTypes().get(qid).contains("START")) {

                this.SnapShot.getCounts().put(qid,BigInteger.ONE);


            }else {
                /**
                 * not start event for the query
                 * the new snapshot = current event type's predecessor grahlet's sum
                 * including itself
                 */
                BigInteger predsum = sumPredecessorsInterCount(qid, e.eventType);
                this.SnapShot.getCounts().put(qid, predsum);

            }
        }
    }

    /**
     * get the sum of the intercount of an event type's predecessor graphlets for a query
     * @param qid the query id
     * @param et the given event type
     * @return the sum of predecessor graphlets' intercoumt
     */
    public BigInteger sumPredecessorsInterCount(Integer qid, EventType et){

        BigInteger sum = BigInteger.ZERO;

        /**
         * if no graphlets, return 0
         */
        if (Graphlets==null){
            return sum;
        }


        ArrayList<EventType> predEts = et.getEdges().get(qid);

        /**
         * if no predecessor event types, return 0
         */
        if (predEts.isEmpty()){
            return sum;
        }

        /**
         * for each pred event type, find the pred Graphlets
         */
        for (EventType predEt: predEts){

            ArrayList<Graphlet> predGs = Graphlets.get(predEt.string);

            /**
             * if no predecessor graphlet, return 0
             */
            if (predGs==null){
                continue;
            }

            /**
             * sum of all pred Graphlets' intercounts
             */
            for (Graphlet p: predGs){
                sum = sum.add(p.interCounts.get(qid));
            }

        }

        return sum;
    }
}





