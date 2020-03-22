package hamlet.graph;

import hamlet.event.Event;
import hamlet.graphlet.Graphlet;
import hamlet.graphlet.NonSharedGraphlet;
import hamlet.graphlet.SharedGraphlet;
import hamlet.template.EventType;
import hamlet.template.Template;
import hamlet.utils.Observable;
import hamlet.utils.Observer;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 *
 * 3.19 Updates:
            counter of number of valid events
            final count is updated when a END event arrives
            logger slowed down the running time by a lot, printing is better but also slows down（a lot of toString() are called）
            query and stream template is finished.
 *
 * Notes:
 *      memory = #of relevant events *12+ 12* number of queries(a snapshot's size, but actually the count is in BigInteger which is much larger than 12)
 *
 */
@Data
public class Graph implements Observable{


    private ArrayList<Observer> observers = new ArrayList();    //observers
    private Snapshot SnapShot;
    private final Template template;
    private HashMap<String, Graphlet> Graphlets;   //a list of Graphlets
    private SharedGraphlet lastSharedG;
    private String activeFlag; //the current active Graphlet
    private ArrayList<Event> events;
    private HashMap<Integer, BigInteger> finalCount;
    private Integer eventCounter;
    private long memory;
    private boolean openMsg;

    /**
     * construct Hamlet.Graph by hamletTemplate
     * @param template the Template
     */
    public Graph(Template template, String streamFile, int epw, boolean openMsg) {
        this.template = template;
        this.Graphlets = new HashMap<String, Graphlet>();
        this.SnapShot = new Snapshot();
        this.activeFlag = "";
        this.events = new ArrayList<Event>();
        this.finalCount = new HashMap<Integer, BigInteger>();
        this.eventCounter = 0;
        this.openMsg = openMsg;
        loadStream(streamFile, epw);

    }

    /**
     * run when reading events from the stream
     */
    public void run() {
        for (Event e : events) {

            /**
             * Graphlet maintainance
            */
            this.eventCounter ++;

            if (Graphlets.get(e.string) == null || !Graphlets.get(e.string).isActive)   //if this Graphlet doesn't exist or is inactive
            {
                finishingLastNotify();
                if (e.eventType.isShared) {  //create a shared G
                    updateSnapshot(e);       //update snapshot
                    SharedGraphlet sharedG = new SharedGraphlet(e);
                    lastSharedG = sharedG;  //maintain the last shared G

                    Graphlets.put(e.string, sharedG);
                    register(sharedG);
                    setActiveFlag(e.string);    //set the active flag

                } else {      //create a non shared G
                    NonSharedGraphlet nonsharedG = new NonSharedGraphlet(e);

                    //找到predG
                    HashMap<Integer, BigInteger> predcounts = new HashMap<>();
                    for (Integer qid: nonsharedG.eventType.getQids()){
                        EventType pred = e.eventType.getPred(qid); //get the pred for one query
                        if (pred!=null){
                            NonSharedGraphlet predG =(NonSharedGraphlet) Graphlets.get(pred.string);   //get the Predecessor Graphlet
                            BigInteger predcount = (predG ==null)?new BigInteger("0"):predG.getCounts().get(qid);
                            predcounts.put(qid, predcount);
                        }else {
                            predcounts.put(qid, new BigInteger("0"));
                        }

                    }
                    nonsharedG.setPredCounts(predcounts);
                    Graphlets.put(e.string, nonsharedG);
                    register(nonsharedG);
                    setActiveFlag(e.string);
                }
            }
            else {     //graphlet exists and is active
                ExpandGraphlet(e, Graphlets.get(activeFlag));   //expand the active graphlet
            }
            /**
             * Maintain e.count
             * update final count for "END" event
             */
            updateFinalCount(e );

        }
        memoryCalculate();
        if (openMsg){
            System.out.println("hamlet final count: "+finalCount);
        }
    }

    public void loadStream(String streamFile, int epw){
        try {    //load the stream into a list of events
            Scanner scanner = new Scanner(new File(streamFile));
            int numofEvents = 0;
            while (scanner.hasNext()&&numofEvents<epw) {
                String line = scanner.nextLine();
                String[] record = line.split(",");
                //if e is in the template, ignore all dummy events
                if (!template.eventTypeExists(record[1])) {
                    continue;
                }
                Event e = new Event(line, template.getEventTypebyString(record[1]));
                this.events.add(e);
                numofEvents++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * when a shared G is created, update the current snapshot
     * for each query, find the pred G of this shared G, update snapshot according to query id
     * @param e
     */

    public void updateSnapshot(Event e){

        for (Integer qid: e.eventType.getQids()){

            EventType pred = e.eventType.getPred(qid); //get the pred for one query
            NonSharedGraphlet predG =(NonSharedGraphlet) Graphlets.get(pred.string);   //get the Predecessor Graphlet

            if (predG!=null&&!predG.getIsCalculated().get(qid)){
                if (lastSharedG==null){        // no snapshot before
                    this.SnapShot.update(predG, qid);
                }else{
                    this.SnapShot.update(lastSharedG.getCoeff(),predG,qid);   //update snapshot
                }
                predG.getIsCalculated().put(qid, true);
            }
        }

//        System.out.println("snapshots: "+SnapShot);
    }

    /**
     * expand the active graphlet
     * @param e the coming event
     */
    public void ExpandGraphlet(Event e, Graphlet g) {   //expand the current graphlet
        g.addEvent(e);

    }


    /**
     * update the final count when an end event arrives
     */

    public boolean updateFinalCount(Event e) {
        if (e.getEndQueries().isEmpty()){
            return false;
        }

        for (Integer q: e.getEndQueries()){     //for all queries that end with E
            if (e.eventType.isShared){      // if e is a shared event(with kleene)
                e.updateCount(q, SnapShot.getCounts().get(q).multiply(e.getCoeff()));  //e.count = snapshot*e.coeff
                if (!this.finalCount.keySet().contains(q)){     //if final count is empty for this query
                    this.finalCount.put(q, e.getCount().get(q));    //final count = e.count
                }else {
                    finalCount.put(q, finalCount.get(q).add(e.getCount().get(q)));       //increment final count
                }
            }else {
                //TODO: Assume that end event is the immediate successor of shared events
                e.updateCount(q, SnapShot.getCounts().get(q).multiply(lastSharedG.getCoeff()));
                finalCount.put(q, finalCount.get(q).add(e.getCount().get(q)));      //update the final count
            }
        }

        return true;
    }


    /**
     * # of relevant event*12 + snapshot* constant
     * no
     */
    private void memoryCalculate(){
        memory = this.eventCounter*12 + 12*template.getQueries().size();
    }

    /**
     * set the active flag, activeNotify every graphlet
     * @param active the active event string
     */
    public void setActiveFlag(String active){
        this.activeFlag = active;
        notifyObservers();
    }

    public void finishingLastNotify(){
        observers.forEach(observer -> observer.finishNotify(this.activeFlag));

    }

    /**
     * register for observers
     * @param o Object to register
     */
    @Override
    public void register(Observer o){
        observers.add(o);
    }

    /**
     * activeNotify all graphlets when change the activeFlag
     */
    @Override
    public void notifyObservers(){
        observers.forEach(observer -> observer.activeNotify(this.activeFlag));
    }

}

