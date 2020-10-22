package hamlet.executor;

import hamlet.Graph.staticGraph;
import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.stream.streamLoader;
import hamlet.stream.streamPartitioner;
import hamlet.workload.Workload;
import hamlet.workload.WorkloadAnalyzer;
import hamlet.workload.WorkloadTemplate;

import java.util.ArrayList;
import java.util.HashMap;

public class Executor {

    private DatasetSchema schema;
    private Workload wholeWorkload;
    private HashMap<String, Workload> stringMiniWorkloadHashMap;
    private HashMap<String, Template> stringTemplateHashMap;

    private HashMap<Workload, HashMap<Object, ArrayList<Event>>> substreams;

    public Executor(DatasetSchema schema){
        this.schema = schema;
        this.stringTemplateHashMap = new HashMap<>();
    }


    public void workloadAnalysis(String workloadFile){

        WorkloadAnalyzer wa = new WorkloadAnalyzer(schema);
        wholeWorkload = wa.analyzeToWorkload(workloadFile);
        System.out.printf("workload size:");
        System.out.printf(wholeWorkload.getAllEventTypes().size()+"");

//        for (EventType et: workload.getAllEventTypes()){
//            System.out.printf(et.getName()+"\n");
//        }
        stringMiniWorkloadHashMap = wa.analyzeToMiniWorkloads(workloadFile);

        for (String et: stringMiniWorkloadHashMap.keySet()){
            Template miniTemplate = new Template(stringMiniWorkloadHashMap.get(et));
            stringTemplateHashMap.put(et, miniTemplate);

        }
        System.out.printf("Workload Analysis finished!");

    }

    public void streamPartitioning(String streamFile){
        ArrayList<Event> events = new streamLoader(streamFile, schema, wholeWorkload).stream();
        substreams = new HashMap<>();
        for (Workload miniWorkload: stringMiniWorkloadHashMap.values()){
            streamPartitioner sp = new streamPartitioner(miniWorkload, events);
            HashMap<Object, ArrayList<Event>> subStreamsForOneMiniWorkload = sp.partition();
            substreams.put(miniWorkload, subStreamsForOneMiniWorkload);

        }
//
//        System.out.printf(substreams.toString());


    }

    public void stockWorkloadGeneration(String workloadFile){
        WorkloadTemplate workloadtemplate = new WorkloadTemplate();
        workloadtemplate.generate(50, 5);
        workloadtemplate.shuffle();
        workloadtemplate.toFile(workloadFile);

    }

    public void staticHamlet(){
        for (String et: this.stringMiniWorkloadHashMap.keySet()){

            for (ArrayList<Event> substream: substreams.get(this.stringMiniWorkloadHashMap.get(et)).values()){
                staticGraph staticGraph = new staticGraph(this.stringTemplateHashMap.get(et),  substream);
                staticGraph.run();
            }

        }
    }

    public void singleRun(){
        String et = (String )this.stringMiniWorkloadHashMap.keySet().toArray()[1];
        ArrayList<Event> substream = (ArrayList<Event>)substreams.get(this.stringMiniWorkloadHashMap.get(et)).values().toArray()[1];
        staticGraph staticGraph = new staticGraph(this.stringTemplateHashMap.get(et),  substream);
        staticGraph.run();




    }

}
