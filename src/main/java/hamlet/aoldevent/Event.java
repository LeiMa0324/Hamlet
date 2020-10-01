package hamlet.aoldevent;


import hamlet.aoldtemplate.EventType;
import lombok.Data;

@Data

/**
 * the hamlet event class
 *
 */
public class Event {
    private int id; //id in the graphlet
    private int sec;
    public final String string;
    public final EventType eventType;


    /**
     * take a line of the stream，convert it to a Hamlet.Event according to the event type
     * @param line a line from the stream
     * @param et the relevant event type
     */
    public Event(String line, EventType et){
        // event type
        this.eventType = et;
        String[] record = line.split(",");

        // second id
        this.sec = Integer.parseInt(record[0]);

        // event string
        this.string = record[1];


    }



}
