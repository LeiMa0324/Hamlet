package hamlet.executor;

import hamlet.Graph.DynamicGraph;
import hamlet.Graph.staticGraph;
import hamlet.Graph.tools.GraphletManager.GraphletManager_DynamicHamlet;
import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.Template;
import hamlet.Graph.tools.BurstLoader;
import hamlet.stream.streamLoader;
import hamlet.stream.streamPartitioner;
import hamlet.workload.Workload;
import hamlet.workload.WorkloadAnalyzer;
import hamlet.workload.WorkloadTemplate;
import hamlet.Graph.Graphlet.Dynamic.MergedGraphlet;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

@Data
public class Executor {

    private DatasetSchema schema;
    private Workload wholeWorkload;
    private HashMap<String, Workload> stringMiniWorkloadHashMap;
    private HashMap<String, Template> stringTemplateHashMap;
    private int epw;
    private String workloadFile;
    private String streamFile;

    private HashMap<String, HashMap<Object, ArrayList<Event>>> substreams;

    private long staticLatency;
    private long staticMemory;

    //dynamic measurements
    private long dynamicOptTime;
    private long dynamicExcTime;

    private long dynamicMemory;
    private int splitNum;
    private int mergeNum;
    private int graphletNum;
    private int mergedGraphletNum;

    //overhead measurements
    private long workloadAnalyzeDuration;
    private long templateGenerationDuration;

    //relevant events
    private int staticRelevantEvents;
    private int dynamicRelevantEvents;

    //# snapshots
    private int staticSnapshotNum;
    private int dynamicSnapshotNum;


    public Executor(DatasetSchema schema, int epw, String workloadFile, String streamFile){
        this.schema = schema;
        this.stringTemplateHashMap = new HashMap<>();
        this.epw = epw;
        this.workloadFile = workloadFile;
        this.streamFile = streamFile;
    }


    public void workloadAnalysis(String workloadFile){

        long analyzeStart = System.currentTimeMillis();
        WorkloadAnalyzer wa = new WorkloadAnalyzer(schema);
        wholeWorkload = wa.analyzeToWorkload(workloadFile);
        long analyzeEnd = System.currentTimeMillis();

        this.workloadAnalyzeDuration = analyzeEnd - analyzeStart;

        System.out.printf("workload size:");
        System.out.printf(wholeWorkload.getAllEventTypes().size()+"");

//        for (EventType et: workload.getAllEventTypes()){
//            System.out.printf(et.getName()+"\n");
//        }

        long templateStart = System.currentTimeMillis();

        stringMiniWorkloadHashMap = wa.analyzeToMiniWorkloads(workloadFile);

        for (String et: stringMiniWorkloadHashMap.keySet()){
            Template miniTemplate = new Template(stringMiniWorkloadHashMap.get(et));
            stringTemplateHashMap.put(et, miniTemplate);

        }

        long templateEnd = System.currentTimeMillis();

        this.templateGenerationDuration = templateEnd-templateStart;

        System.out.printf("Workload Analysis finished!");

    }

    public void streamPartitioning(String streamFile){
        ArrayList<Event> events = new streamLoader(streamFile, schema, wholeWorkload, this.epw).stream();
        substreams = new HashMap<>();
        for (String kleeneET: stringMiniWorkloadHashMap.keySet()){
            Workload miniWorkload = stringMiniWorkloadHashMap.get(kleeneET);
            streamPartitioner sp = new streamPartitioner(miniWorkload, events);
            HashMap<Object, ArrayList<Event>> subStreamsForOneMiniWorkload = sp.partition();
            substreams.put(kleeneET, subStreamsForOneMiniWorkload);

        }
//
//        System.out.printf(substreams.toString());


    }

    public void stockWorkloadGeneration(int queryNum, int groupNum){

        WorkloadTemplate workloadtemplate = new WorkloadTemplate();
        workloadtemplate.generateCandidateQueries( 5);

        for (int i = 20; i<110; i+=10) {
            String workloadFile = "src/main/resources/Revision/Workload_"+i+".txt";
            workloadtemplate.generateWorkload(i, workloadFile);
        }

    }


    public void staticRun() {

        //for each mini workload
        for (String kleeneEt: this.stringMiniWorkloadHashMap.keySet()){

            //for each substream
            for (ArrayList<Event> substream: substreams.get(kleeneEt).values()){

                Template template = this.stringTemplateHashMap.get(kleeneEt);
                BurstLoader burstLoader = new BurstLoader(template.getPredicateManager());
                ArrayList<ArrayList<Event>> bursts = burstLoader.load(substream);
                ArrayList<Event> events = burstLoader.getEvents();

                this.staticRelevantEvents += events.size();

                if (!events.isEmpty()){
                    staticGraph staticGraph = new staticGraph(template,  events, bursts);
                    long start =  System.currentTimeMillis();
                    staticGraph.run();
                    long end =  System.currentTimeMillis();

                    this.staticLatency += (end-start);
                    this.staticMemory += staticGraph.getMemory();

                    this.staticSnapshotNum += staticGraph.getUtils().getSnapshotManager().getSnapshots().size();
                }

//                break;
            }
        }

//        System.out.printf("Static total snapshot number:"+ eventSnapshots);
    }

    public void run(){

        staticRun();
        dynamicRun();


    }

    public void dynamicRun() {


        //for each mini workload
        for (String kleeneEt: this.stringMiniWorkloadHashMap.keySet()){

            //for each substream
            for (ArrayList<Event> substream: substreams.get(kleeneEt).values()){

                Template template = this.stringTemplateHashMap.get(kleeneEt);
                BurstLoader burstLoader = new BurstLoader(template.getPredicateManager());
                ArrayList<ArrayList<Event>> bursts = burstLoader.load(substream);
                ArrayList<Event> events = burstLoader.getEvents();

                this.dynamicRelevantEvents += events.size();

                if (!events.isEmpty()){
                    DynamicGraph dynamicGraph = new DynamicGraph(template,  events, bursts);
                    dynamicGraph.run();

                    this.dynamicOptTime += dynamicGraph.getOptimizerTime();
                    this.dynamicExcTime += dynamicGraph.getExecutionTime();
                    this.dynamicMemory += dynamicGraph.getMemory();
                    this.splitNum += dynamicGraph.getSplitNum();
                    this.mergeNum += dynamicGraph.getMergeNum();

                    GraphletManager_DynamicHamlet Gmanager= (GraphletManager_DynamicHamlet) dynamicGraph.getUtils().getGraphletManager();
                    this.graphletNum += Gmanager.getGraphlets().size();

                    for (int i =0; i< Gmanager.getKleeneGraphlets().size(); i++){
                        if (Gmanager.getKleeneGraphlets().get(i).getClass()== MergedGraphlet.class){
                            this.mergedGraphletNum +=1;
                        }
                    }

                    this.dynamicSnapshotNum += dynamicGraph.getUtils().getSnapshotManager().getSnapshots().size();
                }

            break;
            }
        }
//
//        System.out.printf("Dynamic total snapshot number:"+ snapshotNum);
//        System.out.printf("Split Num:"+splitNum+"\n");
//        System.out.printf("Merged Num:"+mergeNum+"\n");


    }

}
