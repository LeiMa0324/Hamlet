package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.executor.Graphlet.Graphlet;
import hamlet.executor.tools.GraphletManager.GraphletManager_DynamicHamlet;
import hamlet.optimizer.DynamicOptimizer;
import hamlet.query.aggregator.Value;

import java.util.ArrayList;
import java.util.HashMap;

public class DynamicGraph extends Graph{

    private final DynamicOptimizer optimizer;
    private GraphletManager_DynamicHamlet graphletManagerDynamicHamlet;


    public DynamicGraph(Template template, ArrayList<Event> events){
        super(template, events);
        optimizer = new DynamicOptimizer();
        graphletManagerDynamicHamlet = new GraphletManager_DynamicHamlet();
        printWorkload();

    }

    public void run() {

        for (ArrayList<Event> burst: this.bursts){

            boolean isKleeneBurst = burst.get(0).getType().isKleene();
            HashMap<Integer, Value> burstValues = new HashMap<>();

            if (isKleeneBurst){
                HashMap<String, Integer> params = this.graphletManagerDynamicHamlet.getParams(burst);
                boolean shareDecision = this.optimizer.isToShare(params);

                if (shareDecision){
                    //todo：返回一个burst values，加到final上
                    this.graphletManagerDynamicHamlet.share(burst);
                }
                if (!shareDecision){
                    //todo：返回一个burst values，加到final上

                    this.graphletManagerDynamicHamlet.split(burst);
                }

            }else {
                //todo：返回一个burst values，加到final上

                this.graphletManagerDynamicHamlet.newNoneSharedGraphlet(burst);
            }


        }

    }

    //todo: extend的graphlet不能重复update
    public void updateFinalValues(Graphlet graphlet){

    }



    public enum ActiveFlag{
        SPLITS,
        MERGED,
        NONSHARED
    }

}

