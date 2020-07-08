package hamlet.graph;

import hamlet.event.Event;
import hamlet.event.StreamLoader;
import hamlet.graphlet.Graphlet;
import hamlet.graphlet.NonSharedGraphlet;
import hamlet.graphlet.SharedGraphlet;
import hamlet.template.EventType;
import hamlet.template.Template;
import hamlet.utils.Observable;
import hamlet.utils.Observer;
import lombok.Data;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Super class of all graphs
 * contains basic funtions for the sub-classes
 *
 */
@Data
public class Graph implements Observable{


     ArrayList<Observer> observers = new ArrayList();    //observers
     Snapshot SnapShot;
     final Template template;

    //a list of all Graphlets
     HashMap<String, Graphlet> Graphlets;

     //last shared graphlet
     SharedGraphlet lastSharedG;

    //the current active Graphlet
     String activeFlag;
     ArrayList<Event> events;
     HashMap<Integer, BigInteger> finalCount;
     long memory;
     boolean openMsg;
     HashMap<String, Integer> eventCounts;

    /**
     * construct Hamlet.Graph by hamletTemplate
     * @param template the Template
     */
    public Graph(Template template, String streamFile, int epw, boolean openMsg) {
        this.template = template;
        this.Graphlets = new HashMap<>();
        this.SnapShot = new Snapshot();
        this.activeFlag = "";
        this.events = new ArrayList<>();
        this.finalCount = new HashMap<>();
        this.openMsg = openMsg;
        this.events = new StreamLoader(streamFile,epw,template).getEvents();
        this.eventCounts = new HashMap<>();
        for (int q=1; q<= template.getQueries().size();q++){
            finalCount.put(q, new BigInteger("0"));
        }

    }


    void newSharedGraphlet(Event e) {
        SharedGraphlet sharedG = new SharedGraphlet(e);
        lastSharedG = sharedG;  //maintain the last shared G

        Graphlets.put(e.string, sharedG);
        register(sharedG);
        setActiveFlag(e.string);    //set the active flag

    }



    public boolean finishingGraphlet(){

        if (Graphlets.isEmpty()){
            return false;
        }
        // update shared G' count, based on snapshot
        if (Graphlets.get(activeFlag).isShared){
            SharedGraphlet shared = (SharedGraphlet) Graphlets.get(activeFlag);
            // for all queries that shared event in, update the count
            shared.updateCounts(SnapShot);

        }else {
            // update non shared G's count, based on the pred G's count
            NonSharedGraphlet nonsharedG = (NonSharedGraphlet)Graphlets.get(activeFlag);
            HashMap<Integer, BigInteger> predcounts = new HashMap<>();

            for (Integer qid: nonsharedG.eventType.getQids()){

                EventType pred = nonsharedG.eventType.getPred(qid); //get the pred for one query

                if (pred!=null){
                    //找到predG, update non-shared G's predcounts
                    //todo: 找到多个pred G而不是一个predG
                    Graphlet predG = Graphlets.get(nonsharedG.eventType.getPred(qid).string);

                    BigInteger predcount = new BigInteger("0");

                    // if predG appeared before
                    if (!(predG==null)&&!predG.interCounts.get(qid).equals(new BigInteger("0"))){
                        if (predG.isShared){

                            predcount = new BigInteger("2").pow(eventCounts.get(pred.string)).add(new BigInteger("-1"));

                        }else {
                            predcount = new BigInteger(eventCounts.get(pred.string)+"");
                        }
                    }

                    predcounts.put(qid, predcount);
                }else {
                    predcounts.put(qid, new BigInteger("0"));
                }

            }
            nonsharedG.setPredInterCounts(predcounts);

            // update the count for non-shared graphlet
            finishingNonshared();
        }

        updateFinalCounts();  //update final count
        return true;

    }

    /**
     * when a shared G is created, update the current snapshot
     * for each query, find the pred G of this shared G, update snapshot according to query id
     * @param e
     */

    public void updateSnapshot(Event e){

        for (Integer qid: e.eventType.getQids()){

            EventType pred = e.eventType.getPred(qid); //get the pred for one query
            if (pred == null){
                this.SnapShot.getCounts().put(qid, new BigInteger("1"));
                continue;

            }

            NonSharedGraphlet predG =(NonSharedGraphlet) Graphlets.get(pred.string);   //get the Predecessor Graphlet
            //if no pred graphlet it's 0
            if (predG == null){
                this.SnapShot.getCounts().put(qid, new BigInteger("1"));

            }

            if (predG!=null&&!predG.getIsCalculated().get(qid)){

                if (lastSharedG==null){        // no snapshot before
                    this.SnapShot.update(predG, qid);

                }else{
                    this.SnapShot.update(lastSharedG.getCoeff(),predG,qid);   //update snapshot
                }
                predG.getIsCalculated().put(qid, true);
            }
        }

        System.out.println("snapshots: "+SnapShot);
    }

    /**
     * expand the active graphlet
     * @param e the coming event
     */
    public void ExpandGraphlet(Event e, Graphlet g) {
        g.addEvent(e);

    }

    /**
     * final count = active Graphlet's intCount
     * @return
     */
    void updateFinalCounts(){
        Graphlet activeG = Graphlets.get(activeFlag);

        if (activeG.eventType.getEndQueries().isEmpty()){
        }else {

            for (Integer q : activeG.eventType.getEndQueries()) {
                BigInteger previousCount = this.finalCount.keySet().contains(q) ? this.finalCount.get(q) : new BigInteger("0");
                this.finalCount.put(q, previousCount.add(activeG.interCounts.get(q))); // pass the inter count of active G to final count
            }
        }

        System.out.println("final count"+ finalCount);

    }



    /**
     * # of relevant event*12 + snapshot* constant
     * no
     */
    public void memoryCalculate(){
        for (Event e: events){
            if (e.eventType.isShared){
                memory +=12;
            }else {
                memory += e.eventType.getQids().size()*12;
            }
        }
        memory +=12*template.getQueries().size();
    }

    /**
     * set the active flag, activeNotify every graphlet
     * @param active the active event string
     */
    public void setActiveFlag(String active){
        this.activeFlag = active;
        notifyObservers();
    }

    public void finishingNonshared(){
        for (Observer obs: observers){
            obs.finishNotify(this.activeFlag);
        }

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
        for (Observer obs: observers){
            obs.activeNotify(this.activeFlag);
        }
    }

}

