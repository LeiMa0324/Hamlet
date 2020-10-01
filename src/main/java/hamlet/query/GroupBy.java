package hamlet.query;

import hamlet.base.EventType;
import lombok.Data;

/**
 * the group by class
 */
@Data
public class GroupBy {
    private String attributeName;
    private EventType eventType;

    public GroupBy(String attributeName, EventType eventType){
        this.eventType = eventType;
        this.attributeName = attributeName;
    }

}
