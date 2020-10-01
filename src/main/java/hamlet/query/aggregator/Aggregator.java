package hamlet.query.aggregator;

import hamlet.base.Event;
import lombok.Data;

import java.util.ArrayList;

@Data
public abstract class Aggregator {

    protected Aggregfunctions func;

    protected ArrayList<Object> values;
    // the attribute that the aggregation is on
    protected String attributeName;

    protected Aggregator(String funcName, String attributeName){
        this.values = new ArrayList<>();
        this.attributeName = attributeName;
        switch (funcName){
            case "AVG":
                func = Aggregfunctions.AVG;
                break;
            case "SUM":
                func = Aggregfunctions.SUM;
                break;
            case "COUNT":
                func = Aggregfunctions.COUNT;
                break;
        }
    }

    public abstract void aggregate(ArrayList<Event> events);
    public abstract String toString();

    public enum Aggregfunctions {
        AVG,
        SUM,
        COUNT;

        private Aggregfunctions() {
        }
    }
}

