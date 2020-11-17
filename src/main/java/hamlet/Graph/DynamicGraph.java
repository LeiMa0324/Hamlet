package hamlet.Graph;

import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.Graph.tools.GraphletManager.GraphletManager_DynamicHamlet;
import hamlet.Graph.tools.Utils;
import hamlet.optimizer.DynamicOptimizer;
import hamlet.query.aggregator.Value;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class DynamicGraph extends Graph{

    private final DynamicOptimizer optimizer;
    private GraphletManager_DynamicHamlet graphletManagerDynamicHamlet;
    int mergeNum;
    int splitNum;
    private long optimizerTime = 0;
    private long executionTime = 0;


    public DynamicGraph(Template template, ArrayList<Event> events, ArrayList<ArrayList<Event>> bursts){
        super(template, events, bursts);
        optimizer = new DynamicOptimizer();
        graphletManagerDynamicHamlet = new GraphletManager_DynamicHamlet();
        printWorkload();
        this.utils.setGraphletManager(graphletManagerDynamicHamlet);
        this.utils.setGraphType(Utils.GraphType.DYNAMIC);


    }

    public void run() {

        this.windowManager.initAllWindows(this.events.get(0).getTimeStamp());


        for (int i =0;i< this.bursts.size(); i++){
            ArrayList<Event> burst = bursts.get(i);
            System.out.printf("Burst number: "+i+"\n");


            boolean isKleeneBurst = burst.get(0).getType().isKleene();
            HashMap<Integer, Value> burstValues = new HashMap<>();

            if (isKleeneBurst){

                long optimizerStart = System.currentTimeMillis();

                HashMap<String, Integer> params = this.graphletManagerDynamicHamlet.getParams(burst);
                boolean shareDecision = this.optimizer.isToShare(params);
                System.out.printf("share?" + shareDecision);

                long optimizerEnd = System.currentTimeMillis();

                optimizerTime += (optimizerEnd - optimizerStart);

                if (shareDecision){
                    //返回一个burst values，加到final上
                    burstValues = this.graphletManagerDynamicHamlet.share(burst);
                    this.memory += burst.size()*12;
                    this.mergeNum +=1;
                }
                if (!shareDecision){
                    //返回一个burst values，加到final上

                    burstValues = this.graphletManagerDynamicHamlet.split(burst);
                    this.memory += burst.size()*this.utils.getQueryIds().size()*12;
                    this.splitNum += 1;

                }

                long executionEnd = System.currentTimeMillis();
                this.executionTime += (executionEnd-optimizerEnd);

            }else {
                //返回一个burst values，加到final上
                long executionStart = System.currentTimeMillis();
                burstValues = this.graphletManagerDynamicHamlet.newNoneSharedGraphlet(burst);
                this.memory += burst.size()*12;
                long executionEnd = System.currentTimeMillis();
                this.executionTime += (executionEnd - executionStart);


            }

            long beforeFinal = System.currentTimeMillis();

            //如果该burst是end type， update final values
            updateFinalValues(burstValues, burst);

            long afterFinal = System.currentTimeMillis();
            this.executionTime +=(afterFinal - beforeFinal);

            windowProcess(burst);

        }

        this.memory += this.utils.getSnapshotManager().getSnapshots().size()*this.utils.getQueryIds().size()*12;

    }

    public void updateFinalValues(HashMap<Integer, Value> burstValues, ArrayList<Event> burst ){

        ArrayList<Integer> endQueries = (ArrayList<Integer>) burst.get(0).getType().getQueriesEndWith().clone();
        endQueries.retainAll(burst.get(0).getValidQueries());

        Value newFinalValue;

        for (int qid: endQueries){
            newFinalValue = this.finalValues.get(qid).add(burstValues.get(qid));
            this.finalValues.put(qid, newFinalValue);
        }
//        printFinalCount();
    }

    public enum ActiveFlag{
        SPLITS,
        MERGED,
        NONSHARED
    }

}

