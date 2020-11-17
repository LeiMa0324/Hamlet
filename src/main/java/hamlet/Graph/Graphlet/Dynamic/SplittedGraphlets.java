package hamlet.Graph.Graphlet.Dynamic;

import hamlet.Graph.Graphlet.Graphlet;
import hamlet.Graph.tools.Utils;
import hamlet.base.Event;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * an array of split graphlets,
 * every query is run independently
 */
@Data
public class SplittedGraphlets extends Graphlet {

    //maintain a list of events for every query, to calculate values
    private HashMap<Integer, ArrayList<Event>> eventListForQuery;
    private HashMap<Integer, Value> inputValues;


    public SplittedGraphlets(ArrayList<Event> events){
        super(events);
        eventListForQuery = new HashMap<>();


        //get all the valid queries
        for (Event e: events){
            for (int qid: e.getValidQueries()){

                //if eventListForQuery has it, add directly
                //if not, create a new event list
                ArrayList<Event> eventList = eventListForQuery.containsKey(qid)?eventListForQuery.get(qid):
                        new ArrayList<Event>();
                eventList.add(e);
                eventListForQuery.put(qid, eventList);
            }
        }

        this.type = GraphletType.splits;
    }


    //for the first burst of events
    public void propagate(){ }

    public void extend(ArrayList<Event> burst){

        for (Event e: burst){
            for (int qid: e.getValidQueries()){

                //if eventListForQuery has it, add directly
                //if not, create a new event list
                ArrayList<Event> eventList = eventListForQuery.containsKey(qid)?eventListForQuery.get(qid):
                        new ArrayList<Event>();
                eventList.add(e);
                eventListForQuery.put(qid, eventList);
            }
        }

    }

    public void calculateValues(){

        for (int qid: Utils.getInstance().getQueryIds()){

            //update the values by the input values
            if (eventListForQuery.containsKey(qid)){
                updateValues(qid);
            }else {
                //copy the values
                copyValues(qid);
            }


        }
    }

    public void updateValues(int qid){
        BigInteger graphletCountCoeff = new BigInteger("2").pow(eventListForQuery.get(qid).size()).subtract(BigInteger.ONE);


        BigInteger graphletCount = inputValues.get(qid).getCount().multiply(graphletCountCoeff);
        BigDecimal graphletSum = inputValues.get(qid).getSum();

        for (int i=0; i< eventListForQuery.get(qid).size(); i++){
            BigInteger eventCoeff = new BigInteger("2").pow(i);
            BigInteger eventCount = inputValues.get(qid).getCount().multiply(eventCoeff);

            //EVENT SUM = ATTR VALUE* EVENT COUNT
            String attrValueStr = (String) events.get(i).getAttributeValueByName(Utils.getInstance().getAggregator().getAttributeName());
            BigDecimal arrtValue = attrValueStr==null? BigDecimal.ZERO: new BigDecimal(attrValueStr);
            BigDecimal eventSum = arrtValue.multiply(
                    new BigDecimal(eventCount)
            );
            graphletSum = graphletSum.add(eventSum);
        }
        this.graphletValues.put(qid, new Value(graphletCount, graphletSum));

    }

    private void copyValues(int qid){
        this.graphletValues.put(qid, this.inputValues.get(qid));
    }

}
