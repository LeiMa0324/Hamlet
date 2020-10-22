package hamlet.executor.tools;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.executor.tools.GraphletManager.GraphletManager_StaticHamlet;
import hamlet.executor.tools.countManager.KleeneEventCountManager;
import hamlet.executor.tools.countManager.NoneKleeneEventCountManager;
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
    private GraphletManager_StaticHamlet graphletManagerStaticHamlet;
    private  ArrayList<Integer> queryIds;
    private  NoneKleeneEventCountManager noneKleeneEventCountManager;
    private  KleeneEventCountManager kleeneEventCountManager;

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
}
