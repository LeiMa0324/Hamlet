package Hamlet.Graph;

import Hamlet.Event.Event;
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
    private Template template;
    private SharedGraphlet sharedG;
    private NonSharedGraphlet nonsharedG;
    private int currentGraphletFlag; //-1: non-initialized, 0: non-sharedG, 1: shared G
    private ArrayList<Event> events;
    private HashMap<Integer, BigInteger> finalCount;

    /**
     * construct Hamlet.Graph by template
     *
     * @param tmp the Template
     */
    public Graph(Template tmp, String streamFile) {
        this.template = tmp;
        this.sharedG = new SharedGraphlet();
        this.nonsharedG = new NonSharedGraphlet();
        this.currentSnapShot = new Snapshot();
        this.currentGraphletFlag = -1;
        this.events = new ArrayList<Event>();
        this.finalCount = new HashMap<Integer, BigInteger>();
        try {    //load the stream into a list of events
            Scanner scanner = new Scanner(new File(streamFile));
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                Event e = new Event(line);
                this.events.add(e);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * run events
     */
    public void run() {
        for (Event e : events) {
            e.setEventType(template.getStrToEventTypeHashMap().get(e.getEventString()));
            switch (currentGraphletFlag) {
                case -1:
                    ExpandGraphlet(e);
                    break;
                default:
                    if (IsExpandingCurrentGraph(e)) {
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
    public boolean IsExpandingCurrentGraph(Event e) {
        return e.getEventType().getTemplateNode().isShared && currentGraphletFlag == 1 || !e.getEventType().getTemplateNode().isShared() && currentGraphletFlag == 0;
    }

    /**
     * expand the responding graphlet if the coming event e is the same with the current type
     * If the graph is not initialized, add event to corresponding graphlet and set the flag
     * If graphlet is initialized, just add event to corresponding graphlet
     *
     * @param e the coming event
     */
    public void ExpandGraphlet(Event e) {   //expand the current graphlet
        switch (currentGraphletFlag) {
            case -1:
                if (e.getEventType().getTemplateNode().isShared()) {
                    this.sharedG.addEvent(e);
                    this.sharedG.setEventType(e.getEventType());
                    currentGraphletFlag = 1;
                } else {
                    this.nonsharedG.addEvent(e);
                    currentGraphletFlag = 0;
                }
                break;
            case 0:
                this.nonsharedG.addEvent(e);
                break;
            case 1:
                this.sharedG.addEvent(e);
                break;
        }

    }

    /**
     * siwtch the current graph if coming event e is different with the current type.
     * flip the current graph flag
     * from non shared to shared:
     * 1. calculate the coefficient of shared Hamlet.Graphlet
     * 2. update the snap shot
     * 3. delete old shared graphlet
     * 4. create new shared graphlet, add the event into it.
     * from shared to non shared:
     * 1. delete old nonshared graphlet
     * 2. add e to new non shared graphlet
     * 3. update the final count
     *
     * @param e the coming event
     */
    public void SwitchGraphlet(Event e) {
        currentGraphletFlag = 1 - currentGraphletFlag;
        if (currentGraphletFlag == 1) {    //from non shared to share, update the snaposhot
            this.currentSnapShot = new Snapshot(this.currentSnapShot, this.sharedG.getCoeff(), this.nonsharedG.getCountPerQueryHashMap()); //update the snapshot
            this.sharedG = new SharedGraphlet();    //empty the old shared graphlet
            sharedG.addEvent(e);    // put e into the new shared graphlet
        } else {      //from shared to non shared
            this.nonsharedG = new NonSharedGraphlet();    //empty the old non shared graphlet
            this.sharedG.CalculateCoefficient();    // calculate the coefficeint of the current shared
            updateFinalCount();         //update the final count
            nonsharedG.addEvent(e);    // put e into the new non shared graphlet
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

}

