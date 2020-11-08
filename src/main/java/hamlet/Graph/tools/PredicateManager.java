package hamlet.Graph.tools;

import hamlet.base.Event;
import hamlet.query.predicate.Predicate;

import java.util.ArrayList;

public class PredicateManager {
    private ArrayList<Predicate> predicates;

    public PredicateManager(ArrayList<Predicate> predicates){
        this.predicates = predicates;
    }

    public ArrayList<Integer> getValidQueriesForPredicate(Event event){
        ArrayList<Event> events = new ArrayList<>();
        events.add(event);
        ArrayList<Integer> res = new ArrayList<>();

        for (int i=0; i<this.predicates.size(); i++) {
            Predicate p = this.predicates.get(i);
            if (p.verify(events)) {
                res.add(i);
            }
        }

        return res;
    }

    public boolean isAPredicateEvent(Event event){
        return predicates.get(0).getEventTypes().get(0).equals(event.getType());
    }
}
