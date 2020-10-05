package hamlet.base;

import lombok.Data;

/**
 * an event type
 * contains the event type name, a list of attributes
 */
@Data
public class EventType {
    private final String name;
    private final DatasetSchema schema;
    private boolean isKleene;

    public EventType(String name, DatasetSchema schema, boolean isKleene){
        this.name = name;
        this.schema = schema;
        this.isKleene = isKleene;
    }

    public boolean equals(EventType eventType){
        return this.name.equals(eventType.name)&&(this.schema.equals(eventType.schema));
    }

}
