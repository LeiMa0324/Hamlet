package hamlet.executor.tools;

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

            setValidQueries(events.get(i));

            //skip the events that satisfy no predicates
            if (events.get(i).getValidQueries().isEmpty()) {
                continue;
            }

            //get the last event
            latestEvent =latestEvent.equals("")? events.get(i).getType().getName():latestEvent;

            //get the last time stamp
            lastTimeStamp = lastTimeStamp.equals("")?(String) events.get(i).getAttributeValueByName(stockAttributeEnum.date.toString()):lastTimeStamp;

            events.get(i).setEventIndex(validEvents.size());

            //if the event has the same name and time stamp, add it into the burst
            if (events.get(i).getType().getName().equals(latestEvent)&&
                    ((String) events.get(i).getAttributeValueByName(stockAttributeEnum.date.toString())).equals(lastTimeStamp)) {
                tempBurst.add(events.get(i));
            } else {
                latestEvent = events.get(i).getType().getName();
                lastTimeStamp = (String)events.get(i).getAttributeValueByName(stockAttributeEnum.date.toString());
                ArrayList<Event> burst = (ArrayList<Event>) tempBurst.clone();

                bursts.add(burst);
                tempBurst.clear();
                tempBurst.add(events.get(i));
            }

            validEvents.add(events.get(i));

        }
        bursts.add(tempBurst);
        this.events = validEvents;
        return bursts;

    }

    /**
     * set the valid queries for event
     * @param event
     */
    private void setValidQueries(Event event){

        //kleene events, check the predicates
        if (event.getType().isKleene()){
            event.setValidQueries(this.predicateManager.getValidQueriesForPredicate(event));

        }
        // none kleene events, get the queries
        if (!event.getType().isKleene()){
            //get the qid for this event
            event.setValidQueries(event.getType().getQueries());
        }
    }


}
