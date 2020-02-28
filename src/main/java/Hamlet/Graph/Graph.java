package Hamlet.Graph;

import Hamlet.Event.Event;
import Hamlet.Graphlet.Graphlet;
import Hamlet.Graphlet.NonSharedGraphlet;
import Hamlet.Graphlet.SharedGraphlet;
import Hamlet.Template.Template;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Hamlet.Graph maitains the curentsnapshot, the template, two graphlets(shared and non-shared)
 * and a state flag indicating the current graphlet.
 * When a coming event is consitent with the current graphlet(shared or not), call ExpandGraphlet .
 *  if it's not: call SwitchGraphlet
 * Only create a snapshot when a shared graphlet is newly created.
 */
@Data
public class Graph {
    private Snapshot currentSnapShot;
    private final Template template;
    private HashMap<String, Graphlet> Graphlets;   //a list of non-shared G
    private String currentGraphletFlag; //which graphlet it is on
    private ArrayList<Event> events;
    private HashMap<Integer, BigInteger> finalCount;


    /**
     * construct Hamlet.Graph by template
     *
     * @param template the Template
     */
    public Graph(Template template, String streamFile, int epw) {
        // TODO: 2020/2/28 同一个时间戳的事件没有predecessor关系
        this.template = template;
        this.nonsharedGs = new HashMap<String, NonSharedGraphlet>();
        this.currentSnapShot = new Snapshot();
        this.currentGraphletFlag = "";
        this.events = new ArrayList<Event>();
        this.finalCount = new HashMap<Integer, BigInteger>();
        try {    //load the stream into a list of events
            Scanner scanner = new Scanner(new File(streamFile));
            int numofEvents = 0;
            while (scanner.hasNext()&&numofEvents<epw) {
                String line = scanner.nextLine();
                String[] record = line.split(",");
                Event e = new Event(line, template.getEventTypebyString(record[1]));
                this.events.add(e);
                numofEvents++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * run events
     */
    //Todo flag指示某个graphlet
    public void run() {
        for (Event e : events) {
//            e.setEventType(template.getEventTypebyString(e.getEventString()));
            switch (currentGraphletFlag) {
                case "":   //no graphlets
                    if (e.eventType.isShared){
                        newSharedGraphlet(e);
                    }else {
                        newNonsharedGraphlet(e);
                    }
                    break;
                default:
                    if (IsExpandingG(e)) {      //SWTICH G
                        ExpandGraphlet(e);
                    } else {
                        SwitchGraphlet(e);
                    }
                    break;
            }
        }
        System.out.println("final snapshot is " + currentSnapShot);
        calculateFinalCount();
        System.out.println("final count is" + finalCount);

    }

    /**
     * check if the coming event is consistent with the current graphlet
     *
     * @param e the coming event
     * @return true if e is expandable
     */
    public boolean IsExpandingG(Event e) {
        return e.eventType.isShared && currentGraphletFlag == 1 || !e.getEventType().isShared && currentGraphletFlag == 0;
    }

    /**
     * find the matched Graphlet for e and expand it.
     *
     * @param e the coming event
     */
    public void ExpandGraphlet(Event e) {   //expand the current graphlet
        switch (currentGraphletFlag) {
            case 0:
                if (getGraphlet(e)==null){
                    newNonsharedGraphlet(e);
                }else {
                    NonSharedGraphlet matchG = (NonSharedGraphlet) getGraphlet(e);
                    matchG.addEvent(e);
                    this.nonsharedGs.put(e.string,matchG);
                }
                break;
            case 1:
                if (getGraphlet(e)==null){
                    newSharedGraphlet(e);
                }else {
                    this.sharedG.addEvent(e);
                }
                break;
        }

    }

    /**

     *
     * @param e the coming event
     */
    public void SwitchGraphlet(Event e) {
        currentGraphletFlag = 1 - currentGraphletFlag;
        if (currentGraphletFlag == 1) {    //from non shared to share, update the snaposhot
            if (this.currentSnapShot.getSnapshotHashMap()!=null){
                this.currentSnapShot = new Snapshot(this.currentSnapShot, this.sharedG.getCoeff(), this.nonsharedGs); //update the snapshot
                System.out.println("snapshot is "+this.currentSnapShot);
            }else {
                // Todo hhhhh
                this.currentSnapShot = new Snapshot(this.nonsharedGs);
                System.out.println("snapshot is "+this.currentSnapShot);

            }
            newSharedGraphlet(e);
        } else {      //from shared to non shared, every non-shared info is stored in the snapshot
            this.sharedG.CalculateCoefficient();        //calculate the coeff of shared
            newNonsharedGraphlet(e);     //new an non-shared G add it into the hashmap
            updateFinalCount();         //update the final count
        }
    }

    /**
     * only update when finish a shared Pattern
     * final count += final count + sum(coeff)*snapshot
     */
    public void updateFinalCount() {
        for (int q : this.currentSnapShot.getSnapshotHashMap().keySet()) {
            if (this.finalCount.get(q)==null){
                this.finalCount.put(q, this.currentSnapShot.getSnapshotHashMap().get(q).multiply(sharedG.getCoeff()));

            }
            else {
                this.finalCount.put(q, this.finalCount.get(q).add(this.currentSnapShot.getSnapshotHashMap().get(q).multiply(sharedG.getCoeff())));
            }
            //calculate final count
        }
    }
        /**
         * if ths shared is the last graphlet, final count += snapshot*(coeff+1)
         * if the unshared is the last graphlet, final count stays unchanged
         */
    public void calculateFinalCount () {
        this.sharedG.CalculateCoefficient();        //calculate the last shared graphlet's coefficeint
        if (currentGraphletFlag == 1) {          // if end graphlet is shared, add the result into the final count
            updateFinalCount();
        }
    }

    /**
     * find the correct graphlet for an event
     * @param e
     */
    public Graphlet getGraphlet(Event e){
        if (e.eventType.isShared){      //search shared Gs
            return sharedG;
        }else {
            return nonsharedGs.get(e.string);
        }
    }

    public void newSharedGraphlet(Event e){
        this.sharedG = new SharedGraphlet(e.eventType);
        sharedG.addEvent(e);
        currentGraphletFlag = 1;
    }
    /**
     * create a new graphlet based on an event, and add it into corresponding hashmap
     * @param e an incoming event
     */
    public void newNonsharedGraphlet(Event e){

        NonSharedGraphlet nonsharedG = new NonSharedGraphlet(e.eventType);
        nonsharedG.addEvent(e);
        this.nonsharedGs.remove(e.string);
        this.nonsharedGs.put(e.string, nonsharedG);
        currentGraphletFlag = 0;

    }

}

