package Hamlet.Graph;

import Hamlet.Event.Event;
import Hamlet.Graphlet.Graphlet;
import Hamlet.Graphlet.NonSharedGraphlet;
import Hamlet.Graphlet.SharedGraphlet;
import Hamlet.Template.EventType;
import Hamlet.Template.Template;
import Hamlet.Utils.Observable;
import Hamlet.Utils.Observer;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hamlet.Graph maitains:
 *      the hamletTemplate
 *      the current snapshot
 *      a hashmap of graphlets
 *      a state flag activeFlag indicating the current graphlet
 *      the last shared Graphlet for snapshot propagation
 *      a list of events in the stream file
 *      the final count
 *
 * When a coming event is
 *      consistent with the current active graphlet(shared or not):
 *           Expand Graphlet .
 *      not consistent with the current active graphlet(shared or not):
 *          new a graphlet, if the graphlet is shared:
 *                                  1. find it's predecessor, update the snapshot
 *                          if the graphlet is non-shared:
 *                                  case1: from uninitiated state or another non-shared G to the non-shared G:
 *                                      1. new a non-shared G
 *                                  case2: from a shared G to this non-shared G
 *                                      1. new a non-shared G
 *                                      2. update the final count
 *
 * 3.19 Updates:
            counter of number of valid events
            final count is updated when a END event arrives
            logger slowed down the running time by a lot, printing is better but also slows down.
            query and stream template is finished.
 *
 * Questions:
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

        // TODO: 2020/2/28 events with same timestamp should have no predecessor relationship

        this.template = template;
        this.Graphlets = new HashMap<String, Graphlet>();
        this.SnapShot = new Snapshot();
        this.activeFlag = "";
        this.events = new ArrayList<Event>();
        this.finalCount = new HashMap<Integer, BigInteger>();
        this.eventCounter = 0;
        this.openMsg = openMsg;
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
     * run when reading events from the stream
     */
    public void run() {
        for (Event e : events) {

            /**
             * Graphlet maintainance
            */
            this.eventCounter ++;
            StringBuilder msg = new StringBuilder(String.format("\n============================ Coming event: %s ============================\n\n", e.string));

            if (Graphlets.get(e.string) == null || !Graphlets.get(e.string).isActive)   //if this Graphlet doesn't exist or is inactive
            {
                if (e.eventType.isShared) {  //create a shared G
                    updateSnapshot(e);       //update snapshot
                    SharedGraphlet sharedG = new SharedGraphlet(e);
                    lastSharedG = sharedG;  //maintain the last shared G

                    Graphlets.put(e.string, sharedG);
                    register(sharedG);

                    msg.append("\n..Creating new shared Graphlet...\n\n"+sharedG.toString());
                    msg.append("\n\n***************** SNAP SHOT UPDATE *****************\n\n");
                    msg.append(this.SnapShot);

                    setActiveFlag(e.string);    //set the active flag

                } else {      //create a non shared G
                    NonSharedGraphlet nonsharedG = new NonSharedGraphlet(e);
                    Graphlets.put(e.string, nonsharedG);
                    register(nonsharedG);
                    msg.append("...Creating new non-shared Graphlet...\n\n"+nonsharedG.toString());

                    setActiveFlag(e.string);
                }


            }
            else {     //graphlet exists and is active
                ExpandGraphlet(e, Graphlets.get(activeFlag),  msg);   //expand the active graphlet
            }
            /**
             * Maintain e.count
             * update final count for "END" event
             */
            updateFinalCount(e, msg);

            if (openMsg){
                System.out.println(msg);

            }
        }
        memoryCalculate();
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
            if (!predG.isCalculated){
                if (lastSharedG==null){        // no snapshot before
                    this.SnapShot.update(predG, qid);
                }else{
                    this.SnapShot.update(lastSharedG.getCoeff(),predG,qid);   //update snapshot
                }
                predG.isCalculated = true;
            }
        }

    }

    /**
     * expand the active graphlet
     * @param e the coming event
     */
    public void ExpandGraphlet(Event e, Graphlet g, StringBuilder msg) {   //expand the current graphlet
        g.addEvent(e);
        msg.append("..Expanding current active Graphlet...\n\n"+g.toString());

    }


    /**
     * update the final count when an end event arrives
     */

    public boolean updateFinalCount(Event e, StringBuilder msg) {
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
        msg.append("\n\n***************** UPDATE FINAL COUNT *****************\n\n"+e.string+" is an END event, final count will be updated\n");

        return true;
    }


    /**
     * # of relevant event*12 + snapshot* constant
     * no
     */
    //TODO: calculate snapshot's memory
    private void memoryCalculate(){
        memory = this.eventCounter*12 + 12*template.getQueries().size();
    }

    /**
     * set the active flag, notify every graphlet
     * @param active the active event string
     */
    public void setActiveFlag(String active){
        this.activeFlag = active;
        notifyObservers();
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
     * notify all graphlets when change the activeFlag
     */
    @Override
    public void notifyObservers(){
        observers.forEach(observer -> observer.notify(this.activeFlag));
    }

}

