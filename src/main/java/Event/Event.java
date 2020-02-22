package Event;

import HamletTemplate.EventType;
import lombok.Data;

@Data
public class Event {
    private int id; //id in the graphlet
    private int sec;
    private String eventString;
    private EventType eventType;

    /**
     * take a line of record，convert it to a Event
     * @param line line of record
     */
    public Event(String line){
        String[] record = line.split(",");
        this.sec = Integer.parseInt(record[0]);
        this.eventString = record[1];
    }

}
