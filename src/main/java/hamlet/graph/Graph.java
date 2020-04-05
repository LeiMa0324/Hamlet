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
    private long memory;
    private boolean openMsg;
    private HashMap<String, Integer> eventCounts;

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
        this.openMsg = openMsg;
        loadStream(streamFile, epw);    //仅load relevant events
        this.eventCounts = new HashMap<>();
        for (int q=1; q<= template.getQueries().size();q++){
            finalCount.put(q, new BigInteger("0"));
        }


    }

    /**
     * run when reading events from the stream
     */
    public void run() {
        for (Event e : events) {
            Integer c = eventCounts.keySet().contains(e.string)?eventCounts.get(e.string):0;
            eventCounts.put(e.string, c+1);

            /**
             * Graphlet maintainance
            */

            if (Graphlets.get(e.string) == null || !Graphlets.get(e.string).isActive)   //if this Graphlet doesn't exist or is inactive
            {
                // update the count for the active graphlet when it's finished
                finishingGraphlet();


                //initiate new graphlet
                if (e.eventType.isShared) {  //create a shared G
                    updateSnapshot(e);       //update snapshot
                    SharedGraphlet sharedG = new SharedGraphlet(e);
                    lastSharedG = sharedG;  //maintain the last shared G

                    Graphlets.put(e.string, sharedG);
                    register(sharedG);
                    setActiveFlag(e.string);    //set the active flag

                } else {      //create a non shared G
                    NonSharedGraphlet nonsharedG = new NonSharedGraphlet(e);

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

        }
        finishingGraphlet();

        if (openMsg){
            System.out.println("hamlet final count: "+finalCount);
        }
    }

    public void loadStream(String streamFile, int epw){
        try {    //load the stream into a list of events
            Scanner scanner = new Scanner(new File(streamFile));
            int numofEvents = 0;
            boolean isStarted = false;

            while (scanner.hasNext()&&numofEvents<epw) {
                String line = scanner.nextLine();
                String[] record = line.split(",");
                numofEvents++;

                //if e is in the template, ignore all dummy events
                if (!template.eventTypeExists(record[1])) {
                    continue;
                }
                if (!isStarted){
                    if (template.getStartEvents().contains(record[1])){
                        isStarted = true;
                    }
                    else {
                        continue;
                    }
                }
                if (isStarted){
                    Event e = new Event(line, template.getEventTypebyString(record[1]));
                    this.events.add(e);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                    //todo: 如果predG.intercount不为0，则代表已经开始计数，如果为0，则该query还未开始计数
                    //找到predG, update non-shared G's predcounts
                    Graphlet predG = Graphlets.get(nonsharedG.eventType.getPred(qid).string);
                    BigInteger predcount = new BigInteger("0");
                    // if predG appeared before
                    if (!(predG==null)&&!predG.interCounts.get(qid).equals(new BigInteger("0"))){
                        if (predG.isShared){
                            //TODO: match 所有的kleene，即使中间有其他的数\依旧有问题，要找到5,4,1+,6，在4以后的1的个数作为power，而不是所有的1
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

        updateFinalCount();  //update final count
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
            NonSharedGraphlet predG =(NonSharedGraphlet) Graphlets.get(pred.string);   //get the Predecessor Graphlet
            //if no pred graphlet it's 0
            if (predG==null){
                this.SnapShot.getCounts().put(qid, new BigInteger("0"));

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
     * final count = active Graphlet's intCount
     * @return
     */
    public boolean updateFinalCount(){
        Graphlet activeG = Graphlets.get(activeFlag);

        if (activeG.eventType.getEndQueries().isEmpty()){
            return false;
        }

        for (Integer q: activeG.eventType.getEndQueries()){
            BigInteger previousCount = this.finalCount.keySet().contains(q)?this.finalCount.get(q):new BigInteger("0");
            this.finalCount.put(q, previousCount.add(activeG.interCounts.get(q))); // pass the inter count of active G to final count
        }
        return true;

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

