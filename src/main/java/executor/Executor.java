package executor;

import baselines.commons.event.Stream;
import baselines.commons.event.StreamPartitioner;
import baselines.commons.templates.SingleQueryTemplate;
import baselines.commons.transactions.TransactionMQ;
import baselines.greta.GretaMQ;
import baselines.mcep.McepGraph;
import baselines.sharon.Sharon;
import hamlet.graph.DynamicGraph;
import hamlet.graph.StaticGraph;
import hamlet.graph.HamletGraph;
import hamlet.template.Template;
import lombok.Data;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import baselines.sharon.newSharon;

//import java.util.ArrayList;


/**
 * Executor includes executions of all the approaches.
 * It takes settings of the stream file, workload, epw etc. to run the corresponding models together once
 * and store the throughput and latency, memory of each model.
 * 
 * It's called in the experiment.
 */
@Data
public class Executor {
	/**
	 * global settings of the execution
	 */
	//the stream file
	private String streamFile;

	//the workload file
	private String queryFile;
	
	//the queries in the workload
	private ArrayList<String> queries;
	
	//the events per window
	private int epw;
	
	/**
	 * settings of hamlet
	 */
	//hamlet template
	private Template hamletTemplate;
	
	//hamlet graph
	private HamletGraph hamletG;
	
	/**
	 * settings of greta
	 */
	//greta executor
	private GretaMQ greta;
	
	
	/**
	 * latency, memory of each model
	 */
	private long hamletLatency;
	private long gretaLatency;
	private long sharonLatency;
	private long mcepLatency;

	private long hamletMemory;
	private long gretaMemory;
	private long sharonMemory;
	private long mcepMemory;

	/**
	 * settings of static and dynamic hamlet
	 */
	
	//static hamlet graph
	private StaticGraph staticHamlet;

	//dynamic hamlet graph
	private DynamicGraph dynamicHamelt;
	
	private long staticHamletLatency;
	private long staticHamletMemory;
	private long dynamicHamletLatency;
	private long dynamicHamletMemory;

	/**
	 * the constructor of executor
	 * @param streamFile the stream file
	 * @param queryFile the workload file
	 * @param epw the events per window
	 * @param openMsg 
	 */
	public Executor(String streamFile, String queryFile,  int epw, boolean openMsg){
		this.streamFile = streamFile;
		this.queryFile = queryFile;
		this.epw = epw;
		this.queries = new ArrayList<>();

		//read query file
		try {
			Scanner query_scanner = new Scanner(new File(queryFile));
			while (query_scanner.hasNextLine()) {
				queries.add(query_scanner.nextLine());
			}
			query_scanner.close();
		} catch(FileNotFoundException e) {e.printStackTrace();}


		//initiate Hamlet
		this.hamletTemplate = new Template(queries);

		// TODO: 2020-07-05 统一graph 
		this.hamletG = new HamletGraph(hamletTemplate,streamFile, epw, openMsg);
		this.sharonLatency = -1;
		this.mcepLatency = -1;
		this.sharonMemory = -1;
		this.mcepMemory = -1;

	}

	/**
	 * Hamlet versus state-of-the-art approaches
	 * run hamlet, greta（sharon, mcep）once
	 * @param isBaseline whether to run sharon and mcep
	 */
	public void run(boolean isBaseline){

	    hamletRun();		//run hamlet
		gretaRun();			//run greta

		if (isBaseline){
			sharonRun();	//run sharon
			mcepRun();	//run mcep

		}

	}

	/**
	 * Dynamic versus Static Sharing Decision
	 * run static and dynamic hamlet once
	 * @param burstsize the burst size
	 * @param snapshotNum the number of snapshots
	 */
	public void decisionRun(int burstsize, int snapshotNum){
		staticHamletRun(burstsize, snapshotNum);
		dynamicHamletRun(burstsize, snapshotNum);
	}

	/**
	 * single run of the static hamlet
	 * @param burstsize the burst size
	 * @param snapshotNum the number of snapshots
	 */
	public void staticHamletRun( int burstsize, int snapshotNum){

		//initiate the static hamlet graph
		staticHamlet = new StaticGraph(this.hamletTemplate, streamFile, epw,burstsize,snapshotNum,0.6,false);
		long start =  System.currentTimeMillis();

		//run static hamlet
		staticHamlet.staticRun();
		staticHamlet.memoryCalculate();
		long end = System.currentTimeMillis();

		//calculate the latency
		this.staticHamletLatency = end - start;

		//calculate the memory
		this.staticHamletMemory = staticHamlet.getMemory();

		System.out.println("Static Hamlet latency: "+ staticHamletLatency);
		for (int qid=1; qid<= staticHamlet.getTemplate().getQueries().size(); qid++){
			System.out.println("Static Hamlet final count: "+ staticHamlet.getFinalCount().get(qid));

		}

	}

	/**
	 * single run of the dynamic hamlet
	 * @param burstsize the burst size
	 * @param snapshotNum the number of snapshots
	 */
	public void dynamicHamletRun(int burstsize, int snapshotNum){

		//initiate the dynamic hamlet graph
		dynamicHamelt = new DynamicGraph(this.hamletTemplate, streamFile, epw, burstsize, snapshotNum,0.6,false);

		long start =  System.currentTimeMillis();

		//run dynamic hamlet
		dynamicHamelt.dynamicRun();
		long end = System.currentTimeMillis();

		//calculate the latency
		this.dynamicHamletLatency = end - start;

		//calculate the memory
		this.dynamicHamletMemory = dynamicHamelt.getMemory();

		System.out.println("Dynamic Hamlet latency: "+ dynamicHamletLatency);
		for (int qid=1; qid<= dynamicHamelt.getTemplate().getQueries().size(); qid++){
			System.out.println("Dynamic Hamlet final count: "+ dynamicHamelt.getFinalCount().get(qid));

		}




	}

	/**
	 * a single run of Hamlet
	 */
	public void hamletRun(){

		long start =  System.currentTimeMillis();

		//run hamlet
		hamletG.run();

		long end =  System.currentTimeMillis();

		//calculate the latency
		hamletLatency = end - start;

		//calculate the memory
		hamletG.memoryCalculate();
		hamletMemory = hamletG.getMemory();

		System.out.println("Hamlet latency: "+ hamletLatency);
		System.out.println("Hamlet Memory: "+ hamletMemory);

	}

	/**
	 * a single run of Greta
	 */
	public void gretaRun(){

		this.gretaLatency =0;
		try {
			CountDownLatch done = new CountDownLatch(1);
			AtomicLong latency = new AtomicLong(0);
			AtomicInteger memory = new AtomicInteger(0);
			StreamPartitioner sp = new StreamPartitioner("gen", streamFile, epw);
			Stream stream = sp.partition();
			TransactionMQ TrS;

			//initiate greta
			TrS = new GretaMQ(done, latency, memory, sp);
			this.greta = (GretaMQ)TrS;

			//add queries into greta's template
			for (int i = 0; i < queries.size(); i++) {
				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
				((GretaMQ) TrS).addQuery(query);
			}

			// run greta
			TrS.run();
			done.await();

			//store latency
			this.gretaLatency = latency.get();

			//store memory
			this.gretaMemory = ((GretaMQ) TrS).memory.longValue();

			System.out.println("Greta latency: "+ gretaLatency);
			System.out.println("Greta Memory: "+ gretaMemory);

		} catch (InterruptedException e) { e.printStackTrace(); }

	}

	/**
	 * a single run of Sharon
	 */
	public void sharonRun(){

		try{

			CountDownLatch done = new CountDownLatch(1);
			AtomicLong latency = new AtomicLong(0);
			AtomicInteger memory = new AtomicInteger(0);

			// run sharon for each query
			for (String q:queries){
				StreamPartitioner sp = new StreamPartitioner("gen", streamFile, epw);
				TransactionMQ TrS;
				Stream stream = sp.partition();
				TrS = new Sharon(stream, done, latency, memory, q, 1);
				TrS.run();
				done.await();

				//increment the latency
				sharonLatency += TrS.latency.get();

				//increment memory
				sharonMemory += ((Sharon)TrS).memory.get();

			}

			System.out.println("newSharon latency: "+ sharonLatency);
			System.out.println("newSharon Memory: "+ sharonMemory);

		}catch (InterruptedException e) { e.printStackTrace(); }


	}


	/**
	 * a single run of MCEP
	 */
	public void mcepRun(){

		//initiate MCEP
		McepGraph mcepGraph = new McepGraph(this.hamletTemplate, streamFile, epw);

		long start =  System.currentTimeMillis();

		//run MCEP
		mcepGraph.run();
		long end =  System.currentTimeMillis();

		//calculate the latency
		mcepLatency = end - start;

		//calculate the memory
		mcepGraph.memoryCalculate();
		this.mcepMemory = mcepGraph.memory;


		System.out.println("MCEP latency: "+ mcepLatency);
		System.out.println("MCEP Memory: "+ mcepMemory);

	}
}
