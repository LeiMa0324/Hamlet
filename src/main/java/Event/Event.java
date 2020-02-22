package Event;

import HamletTemplate.EventType;
import lombok.Data;

@Data
public class Event {
    private int id; //id in the graphlet
    private int sec;
    private String eventString;
    //该event的event type,由template查找后返回给event
    private EventType eventType;

    /**
     * 接受一行record，将其转化为event类型
     * @param line 一行数据
     */
    public Event(String line){
        String[] record = line.split(",");
        this.sec = Integer.parseInt(record[0]);
        this.eventString = record[1];
    }

}
