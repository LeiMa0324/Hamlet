package hamlet.Graph.tools;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.Graph.tools.GraphletManager.GraphletManager;
import hamlet.Graph.tools.countManager.KleeneEventCountManager;
import hamlet.Graph.tools.countManager.NoneKleeneEventCountManager;
import hamlet.query.aggregator.Aggregator;
import lombok.Data;

import java.util.ArrayList;

@Data
public class Utils {
    private static Utils instance;

    private  ArrayList<Event> events;
    private Template template;
    private  Aggregator aggregator;
    private  PredecessorManager predecessorManager;
    private  SnapshotManager snapshotManager;
    private GraphletManager graphletManager;
    private  ArrayList<Integer> queryIds;
    private  NoneKleeneEventCountManager noneKleeneEventCountManager;
    private  KleeneEventCountManager kleeneEventCountManager;
    private GraphType graphType;

    private Utils(ArrayList<Event> events,
                  Template template,
                  Aggregator aggregator,
                  ArrayList<Integer> queryIds){
        this.events = events;
        this.template = template;
        this.queryIds = queryIds;
        this.aggregator = aggregator;

    }

    public static void newInstance(ArrayList<Event> events,
                            Template template,
                            Aggregator aggregator,
                            ArrayList<Integer> queryIds){
        if (instance ==null){
            instance = new Utils(events,
                    template,
                    aggregator,
                    queryIds);
        }
    }


    public static Utils getInstance(){
        return instance;
    }

    public enum GraphType{
        STATIC,
        DYNAMIC
    }

    public static void reset(){
        instance = null;
    }

}
