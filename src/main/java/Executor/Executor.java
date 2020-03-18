package Executor;

import Greta.event.Stream;
import Greta.event.StreamPartitioner;
import Greta.template.SingleQueryTemplate;
import Greta.transaction.*;
import Hamlet.Graph.*;
import Hamlet.Template.Template;
import lombok.Data;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

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
	 * latency, memory of each model
	 */
	private long hamletLatency;
	private long gretaLatency;
	private long hamletMemory;
	private long gretaMemory;

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
		this.hamletG = new Graph(hamletTemplate,streamFile, epw, openMsg);

	}

	/**
	 * run hamlet, greta and log
	 */
	public void run(){

	    hamletRun();		//run hamlet
		System.out.println("Hamlet latency: "+ hamletLatency);
		System.out.println("Hamlet Memory: "+ hamletMemory);


		gretaRun();
		System.out.println("Greta latency: "+ gretaLatency);
		System.out.println("Greta Memory: "+ gretaMemory);

	}
	/**
	 * a single run of Hamlet
	 */
	public void hamletRun(){

		long start =  System.currentTimeMillis();
		hamletG.run();

		long end =  System.currentTimeMillis();
		hamletLatency = end - start;
		hamletMemory = hamletG.getMemory();

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

			for (int i = 0; i < queries.size(); i++) {      //
				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
				((GretaMQ) TrS).addQuery(query);
			}
			TrS.run();
			done.await();
			this.gretaLatency = latency.get();
			this.gretaMemory = ((GretaMQ) TrS).memory.longValue();

		} catch (InterruptedException e) { e.printStackTrace(); }

	}

}
