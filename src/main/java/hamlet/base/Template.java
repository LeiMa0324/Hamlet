package hamlet.base;

import hamlet.executor.PredicateManager;
import hamlet.executor.WindowManager;
import hamlet.query.Query;
import hamlet.query.aggregator.Aggregator;
import hamlet.query.predicate.Predicate;
import hamlet.query.window.Window;
import hamlet.workload.Workload;
import lombok.Data;

import java.util.ArrayList;

@Data
public class Template {
    private Workload workload;
    private Aggregator aggregator;
    private PredicateManager predicateManager;
    private WindowManager windowManager;

    public Template(){}
    public Template(Workload workload){
        this.workload = workload;
        this.aggregator = workload.getQueries().get(0).getAggregator();


        ArrayList<Window> windows = new ArrayList<>();
        ArrayList<Predicate> predicates = new ArrayList<>();
        for (int i=0; i<workload.getQueries().size(); i++){
            predicates.add(workload.getQueries().get(i).getPredicates().get(0));
            windows.add(workload.getQueries().get(i).getWindow());

            //set event types
            setEventTypes(i);

            }
        this.windowManager = new WindowManager(windows);
        this.predicateManager = new PredicateManager(predicates);

    }


    public EventType getNoneKleenePredecessorByEventTypeAndQueryId(EventType eventType, Integer qid){
        return workload.getQueries().get(qid).getPattern().getNoneKleenePredecessor(eventType);
    }


    public ArrayList<EventType> getAllPredecessorsByEventTypeAndQueryId(EventType eventType, Integer qid){
        return workload.getQueries().get(qid).getPattern().getAllPredecessors(eventType);
    }

    private void setEventTypes(int qid){

        if (this.workload.getQueries().get(qid).getPattern().getEventTypes().size()==1){
            this.workload.getQueries().get(qid).getPattern().getEventTypes().get(0).getPosType().put(qid, EventType.Type.STARTANDEND);
        }else {
            int endIndex = this.workload.getQueries().get(qid).getPattern().getEventTypes().size()-1;
            for (int i=0;i< this.workload.getQueries().get(qid).getPattern().getEventTypes().size(); i++){

                Query query = this.workload.getQueries().get(qid);
                if (i ==0 ){
                    query.getPattern().getEventTypes().get(i).getPosType().put(qid, EventType.Type.START);
                }
                if (i == endIndex){
                    query.getPattern().getEventTypes().get(i).getPosType().put(qid, EventType.Type.END);
                }
                if (i!=0 && i!=endIndex){
                    query.getPattern().getEventTypes().get(i).getPosType().put(qid, EventType.Type.OTHER);
                }

                }
            }
        }
    }

