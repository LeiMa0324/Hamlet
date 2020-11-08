package hamlet.Graph.tools;

import hamlet.base.Event;
import hamlet.query.aggregator.Aggregator;
import lombok.Data;
import hamlet.users.stockUser.stockAttributeEnum;

import java.util.ArrayList;

@Data
public class BurstLoader {

    private ArrayList<Event> events;
    private final PredicateManager predicateManager;
    private Aggregator aggregator;

    public BurstLoader(PredicateManager predicateManager){
        this.predicateManager = predicateManager;
        this.aggregator = aggregator;

    }

    public ArrayList<ArrayList<Event>> load(ArrayList<Event> events){

        ArrayList<ArrayList<Event>> bursts = new ArrayList<>();
        ArrayList<Event> validEvents = new ArrayList<>();

        String latestEvent = "";
        String lastTimeStamp = "";
        ArrayList<Event> tempBurst = new ArrayList<>();

        for (int i =0; i<events.size(); i++) {

            Event event = setValidQueries(events.get(i));

            //skip the events that satisfy no predicates
            if (event.getValidQueries().isEmpty()) {
                continue;
            }

            //get the last event
            latestEvent =latestEvent.equals("")? event.getType().getName():latestEvent;

            //get the last time stamp
            lastTimeStamp = lastTimeStamp.equals("")?(String) event.getAttributeValueByName(stockAttributeEnum.date.toString()):lastTimeStamp;

            event.setEventIndex(validEvents.size());

            //if the event has the same name and time stamp, add it into the burst
            if (event.getType().getName().equals(latestEvent)&&
                    ((String) event.getAttributeValueByName(stockAttributeEnum.date.toString())).equals(lastTimeStamp)) {
                tempBurst.add(event);
            } else {
                latestEvent = event.getType().getName();
                lastTimeStamp = (String)event.getAttributeValueByName(stockAttributeEnum.date.toString());
                ArrayList<Event> burst = (ArrayList<Event>) tempBurst.clone();

                bursts.add(burst);
                tempBurst.clear();
                tempBurst.add(event);
            }

            validEvents.add(event);

        }
        bursts.add(tempBurst);
        ArrayList<ArrayList<Event>> newBursts = setFlag(bursts);
        this.events = allEventsFromBursts(newBursts);
        return newBursts;

    }

    /**
     * set the valid queries for event
     * @param event
     */
    private Event setValidQueries(Event event){

        //kleene events, check the predicates
        if (event.getType().isKleene()){
            event.setValidQueries(this.predicateManager.getValidQueriesForPredicate(event));

        }
        // none kleene events, get the queries
        if (!event.getType().isKleene()){
            //get the qid for this event
            event.setValidQueries(event.getType().getQueries());
        }
        return event;
    }

    private ArrayList<ArrayList<Event>> setFlag(ArrayList<ArrayList<Event>> bursts){

        ArrayList<ArrayList<Event>> newBursts = new ArrayList<>();
        for (ArrayList<Event> burst: bursts){

            if (Math.random()<0.3){
                for (Event event: burst){
                    event.setHasSnapshot(true);
                }
            }
            newBursts.add(burst);
        }
        return newBursts;
    }

    private ArrayList<Event> allEventsFromBursts(ArrayList<ArrayList<Event>> bursts){
        ArrayList<Event> events = new ArrayList<>();

        for (ArrayList<Event> burst: bursts){
            events.addAll(burst);
        }
        return events;
    }
}
