package baselines.mcep;

import hamlet.event.Event;
import hamlet.event.StreamLoader;
import hamlet.template.EventType;
import hamlet.template.Template;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class McepGraph {


    Template template;
    ArrayList<Event> events;
    HashMap<String, ArrayList<McepNode>> nodes;
    HashMap<Integer, BigInteger> finalcounts;
    public long memory;
    Integer edgenum = 0;


    public McepGraph(Template template, String streamFile, int epw) {
        this.template = template;
        this.events = new ArrayList<>();
        this.nodes = new HashMap<>();
        this.finalcounts = new HashMap<>();

        for (String et : template.getEventTypes().keySet()) {
            this.nodes.put(et, new ArrayList<McepNode>());

        }
        for (Integer qid = 1; qid <= template.getQueries().size(); qid++) {
            finalcounts.put(qid, BigInteger.ZERO);
        }


        this.events = new StreamLoader(streamFile, epw, template).getEvents();
        build();

    }

    /**
     * build graph structure
     */
    public void build() {

        int numofsharedRead = 0;

        for (Event e : events) {

            McepNode newNode = new McepNode(e);

            //maintain predecessors
            for (Integer qid : e.eventType.getQids()) {

                ArrayList<EventType> preds = e.eventType.getEdges().get(qid);  //找到predecessor的类型

                for (EventType pred : preds) {
                    newNode.connectPreds(qid, nodes.get(pred.string));   //connect the predecessors for a query
                    edgenum += preds.size();
                }
            }

            if (e.eventType.isShared) {
                numofsharedRead++;
                newNode.intercount.put(-1, new BigInteger("2").pow(numofsharedRead).subtract(BigInteger.ONE));  //maintain intercounts for shared events
//                System.out.println(newNode.intercount);
            }

            ArrayList<McepNode> nodelist = nodes.get(e.string);
            nodelist.add(newNode);
            nodes.put(e.string, nodelist);      //update nodelist

        }
    }

    public void run() {

        //traverse the shared events only once

        System.out.println("======other events traverse=====");

        //traverse other events
        for (Integer qid = 1; qid <= template.getQueries().size(); qid++) {

            //找到所有的end events
            String[] events = template.getQueries().get(qid - 1).split(",");
            ArrayList<McepNode> endnodes = nodes.get(events[events.length - 1]);

            int maxLength = 0;

            /**
             * update final count for each end node
             */
            for (McepNode node : endnodes) {
//                System.out.println(endnodes.indexOf(node) + " out of " + endnodes.size() + " traversing...");
                Stack<McepNode> trend = new Stack<McepNode>();
//                System.out.println("node second:" +node.event.getSec()+" node: " +node.event.string);
                traverse(qid, node, trend);
                BigInteger previousCount = finalcounts.get(qid);
                finalcounts.put(qid, previousCount.add(node.intercount.get(qid)));
            }

        }

//        System.out.println("final count :" + finalcounts);


    }


    /**
     * traverse from the end event,
     * update the intercount for every node
     *
     * @param qid
     * @param node
     * @param trend
     */
    public void traverse(Integer qid, McepNode node, Stack<McepNode> trend) {

        if (!node.traversed.get(qid)) {

            trend.push(node);

            boolean sharedNode = node.event.eventType.isShared;
            boolean boundaryNode = (!sharedNode) &&(!node.previous.get(qid).isEmpty()) &&node.previous.get(qid).get(0).event.eventType.isShared;
            boolean innerNode = (!sharedNode)&&(!node.previous.get(qid).isEmpty())&&(!boundaryNode);

            /*** Boundary nodes that connect to the shared nodes ***/

            if (boundaryNode) {
                traverse(qid, node.latestSharedPred, trend);
                node.intercount.put(qid,node.latestSharedPred.intercount.get(-1));
            }

            if (innerNode){
                /** Other nodes ***/
                BigInteger predSum = BigInteger.ZERO;

                for (McepNode previous : node.previous.get(qid)) {
                    traverse(qid, previous, trend);
                    predSum = predSum.add(previous.intercount.get(qid));

                }
                node.intercount.put(qid, predSum);  //intercount = sum(pred.intercount)
            }
            trend.pop();

            node.traversed.put(qid,true);

        }

    }

    public void memoryCalculate(){

        for (Event e: events){
            if (e.eventType.isShared){
                memory +=12;
            }else {
                memory += e.eventType.getQids().size()*12;
            }
        }
        memory +=12*template.getQueries().size();

        this.memory += edgenum*4 ;
    }
}





