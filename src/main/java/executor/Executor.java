package executor;

import baselines.commons.event.Stream;
import baselines.commons.event.StreamPartitioner;
import baselines.commons.templates.SingleQueryTemplate;
import baselines.greta.GretaMQ;
import baselines.commons.transactions.TransactionMQ;
//import baselines.sharon.newSharon;
import baselines.mcep.McepGraph;
import baselines.sharon.Sharon;
import hamlet.graph.*;
import hamlet.template.Template;
import lombok.Data;

import java.io.File;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import java.util.ArrayList;


/**
 * Executor takes settings of the experiment to run hamlet and greta once
 */
@Data
public class Executor {
	/**
	 * settings of experiment
	 */
	private String streamFile;
	private String queryFile;
	private String throuputFile;
	private String latencyFile;
	private String memoryFile;
	private ArrayList<String> queries;
	private int epw;
	/**
	 * settings of hamlet
	 */
	private Template hamletTemplate;
	private Graph hamletG;


	/**
	 * greta
	 */
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
	 * static and dynamic hamlet
	 */
	private StaticGraph staticHamlet;
	private long staticHamletLatency;
	private long staticHamletMemory;

	private DynamicGraph dynamicHamelt;
	private long dynamicHamletLatency;
	private long dynamicHamletMemory;


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


		//Hamlet
		this.hamletTemplate = new Template(queries);
		this.hamletG = new prefixGraph(hamletTemplate,streamFile, epw, openMsg);
		this.sharonLatency = -1;
		this.mcepLatency = -1;
		this.sharonMemory = -1;
		this.mcepMemory = -1;

	}

	/**
	 * run hamlet, greta, sharon, mcep and log
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
	 * run static and dynamic hamlet
	 * @param batchsize
	 */
	public void decisionRun(int batchsize, int snapshotNum){
		staticHamletRun(batchsize, snapshotNum);
		dynamicHamletRun(batchsize, snapshotNum);
	}

	public void staticHamletRun( int batchsize, int snapshotNum){
		//static and dynamic Hamlet
		staticHamlet = new StaticGraph(this.hamletTemplate, streamFile, epw,batchsize,snapshotNum,0.6,false);
		long start =  System.currentTimeMillis();
		staticHamlet.staticRun();
		staticHamlet.memoryCalculate();	//calculate the memory

		long end = System.currentTimeMillis();

		this.staticHamletLatency = end - start;
		this.staticHamletMemory = staticHamlet.getMemory();

		System.out.println("Static Hamlet latency: "+ staticHamletLatency);

	}

	public void dynamicHamletRun(int batchsize, int snapshotNum){
		//static and dynamic Hamlet
		dynamicHamelt = new DynamicGraph(this.hamletTemplate, streamFile, epw, batchsize, snapshotNum,0.6,false);

		long start =  System.currentTimeMillis();
		dynamicHamelt.dynamicRun();
		long end = System.currentTimeMillis();

		this.dynamicHamletLatency = end - start;
		this.dynamicHamletMemory = dynamicHamelt.getMemory();

		System.out.println("Dynamic Hamlet latency: "+ dynamicHamletLatency);



	}

	/**
	 * a single run of Hamlet
	 */
	public void hamletRun(){

		long start =  System.currentTimeMillis();
		hamletG.run();

		long end =  System.currentTimeMillis();
		hamletLatency = end - start;
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

			TrS = new GretaMQ(done, latency, memory, sp);
			this.greta = (GretaMQ)TrS;

			for (int i = 0; i < queries.size(); i++) {      //
				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
				((GretaMQ) TrS).addQuery(query);
			}
			TrS.run();
			done.await();
			this.gretaLatency = latency.get();
			this.gretaMemory = ((GretaMQ) TrS).memory.longValue();
			System.out.println("Greta latency: "+ gretaLatency);
			System.out.println("Greta Memory: "+ gretaMemory);

		} catch (InterruptedException e) { e.printStackTrace(); }

	}

	public void sharonRun(){

		try{

			CountDownLatch done = new CountDownLatch(1);
			AtomicLong latency = new AtomicLong(0);
			AtomicInteger memory = new AtomicInteger(0);

			for (String q:queries){
				StreamPartitioner sp = new StreamPartitioner("gen", streamFile, epw);
				TransactionMQ TrS;
				Stream stream = sp.partition();
				TrS = new Sharon(stream, done, latency, memory, q, 1);
				TrS.run();
				done.await();
				sharonLatency += TrS.latency.get();
				sharonMemory += ((Sharon)TrS).memory.get();

			}

			System.out.println("newSharon latency: "+ sharonLatency);
			System.out.println("newSharon Memory: "+ sharonMemory);

		}catch (InterruptedException e) { e.printStackTrace(); }


	}



	public void mcepRun(){

		McepGraph mcepGraph = new McepGraph(this.hamletTemplate, streamFile, epw);

		long start =  System.currentTimeMillis();
		mcepGraph.run();
		long end =  System.currentTimeMillis();

		mcepLatency = end - start;
		mcepGraph.memoryCalculate();
		this.mcepMemory = mcepGraph.memory;
		System.out.println("MCEP latency: "+ mcepLatency);
		System.out.println("MCEP Memory: "+ mcepMemory);

	}
}
