package revision;


import lombok.Data;

import java.util.ArrayList;

@Data
public class SubStream {

    private ArrayList<Event> events;

    // the event type
    private int vendorID;

    // the group-attribute;
    private int paymentType;

    //the predicate
    private float tripDistance;

    public SubStream(int vendorID, int paymentType, float tripDistance){
        this.vendorID = vendorID;
        this.paymentType = paymentType;
        this.tripDistance = tripDistance;
        this.events = new ArrayList<>();

    }

    /**
     * add an event into the sub stream
     * @param e
     */
    public void addEvent(Event e){
        events.add(e);
    }
}
