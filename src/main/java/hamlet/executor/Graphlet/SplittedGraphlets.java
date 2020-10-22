package hamlet.executor.Graphlet;

import hamlet.base.Event;
import hamlet.executor.tools.Utils;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

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

        for (int qid: events.get(0).getValidQueries()){
            eventListForQuery.put(qid, events);
        }
        this.type = GraphletType.splits;
    }


    //for the first burst of events
    public void propagate(){ }

    public void extend(ArrayList<Event> burst){

        //new valid queries = old valid queries UNION burst valid queries
        Set<Integer> validQueries = eventListForQuery.keySet();
        validQueries.addAll(burst.get(0).getValidQueries());

        for (int qid: validQueries){
            ArrayList<Event> eventsForQ = eventListForQuery.get(qid);
            if (burst.get(0).getValidQueries().contains(qid)){
                eventsForQ.addAll(burst);
            }
            eventListForQuery.put(qid, eventsForQ);
        }

    }

    public void calculateValues(){

        for (int qid: eventListForQuery.keySet()){
            BigInteger graphletCountCoeff = new BigInteger("2").pow(eventListForQuery.get(qid).size()).subtract(BigInteger.ONE);


            BigInteger graphletCount = inputValues.get(qid).getCount().multiply(graphletCountCoeff);
            BigDecimal graphletSum = inputValues.get(qid).getSum();

            for (int i=0; i< eventListForQuery.get(qid).size(); i++){
                BigInteger eventCoeff = new BigInteger("2").pow(i);
                BigInteger eventCount = inputValues.get(qid).getCount().multiply(eventCoeff);

                //EVENT SUM = ATTR VALUE* EVENT COUNT
                BigDecimal eventSum = new BigDecimal ((String)events.get(i).getAttributeValueByName(Utils.getInstance().getAggregator().getAttributeName())).multiply(
                    new BigDecimal(eventCount)
                );
                graphletSum = graphletSum.add(eventSum);
            }
            this.graphletValues.put(qid, new Value(graphletCount, graphletSum));
        }
    }

}
