package hamlet.executor;

import hamlet.base.Event;
import lombok.Data;

import java.util.ArrayList;

@Data
public class BurstLoader {

    private ArrayList<Event> events;
    private final PredicateManager predicateManager;

    public BurstLoader(PredicateManager predicateManager){
        this.predicateManager = predicateManager;

    }

    public ArrayList<ArrayList<Event>> load(ArrayList<Event> events){

        ArrayList<ArrayList<Event>> bursts = new ArrayList<>();
        ArrayList<Event> validEvents = new ArrayList<>();

        String latestEvent = "";
        ArrayList<Event> tempBurst = new ArrayList<>();


        for (int i =0; i<events.size(); i++) {

            setValidQueries(events.get(i));

            //skip the events that satisfy no predicates
            if (events.get(i).getValidQueries().isEmpty()) {
                continue;
            }

            latestEvent =latestEvent.equals("")? events.get(i).getType().getName():latestEvent;
            events.get(i).setEventIndex(validEvents.size());

            if (events.get(i).getType().getName().equals(latestEvent)) {
                tempBurst.add(events.get(i));
            } else {
                latestEvent = events.get(i).getType().getName();
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
