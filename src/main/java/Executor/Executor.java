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

	public Executor(String streamFile, String queryFile,  int epw, String throuputFile, String latencyFile, String memoryFile){
		this.streamFile = streamFile;
		this.queryFile = queryFile;
		this.throuputFile = throuputFile;
		this.latencyFile = latencyFile;
		this.memoryFile = memoryFile;
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


	}

	/**
	 * run hamlet, greta and log
	 */
	public void run(){

		long memory1, memory2, memory3, memory4;
		Runtime gfg = Runtime.getRuntime();

		gfg.gc();		//garbage collection
		memory1 = gfg.freeMemory();		//calculate free memory
		hamletRun();		//run hamlet
		memory2 = gfg.freeMemory();		//calculate free memory after hamlet

		hamletMemory = memory1 - memory2;
		System.out.println("Hamlet: latency "+ hamletLatency);
		System.out.println("Hamlet: memory " + hamletMemory);
		String readableHamletMemory = FileUtils.byteCountToDisplaySize(hamletMemory);
		System.out.println("Hamlet: readable memory " + readableHamletMemory);


		gfg.gc();

		memory3 = gfg.freeMemory();
		gretaRun();
		memory4 = gfg.freeMemory();

		gretaMemory = memory3 - memory4;
		System.out.println("Greta: latency "+gretaLatency);
		System.out.println("Greta: byte memory " + gretaMemory);
		String readableGretaMemory = FileUtils.byteCountToDisplaySize(gretaMemory);
		System.out.println("Greta: readable memory " + readableGretaMemory);

		gfg.gc();


		logging("thru");
		logging("lat");
		logging("mem");


	}
	/**
	 * a single run of Hamlet
	 */
	public void hamletRun(){

		//Hamlet
		this.hamletTemplate = new Template(queries);
		this.hamletG = new Graph(hamletTemplate,streamFile, epw);
		long start =  System.currentTimeMillis();
		hamletG.run();

		long end =  System.currentTimeMillis();
		hamletLatency = end - start;

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

			for (int i = 0; i < queries.size(); i++) {      //将一个query创建一个query
				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
				((GretaMQ) TrS).addQuery(query);
			}
			TrS.run();
			done.await();
			this.gretaLatency = latency.get();

		} catch (InterruptedException e) { e.printStackTrace(); }

	}

	/**
	 * logging
	 */
	public void logging(String choice) {
		String filename = "";
		String[] header = new String[3];
		String[] data = new String[3];
		switch (choice){
			case "thru":
				filename = throuputFile;
				header[0]= "epw";
				header[1]= "Hamlet throughput";
				header[2]= "Greta throughput";
				data[0] = epw+"";
				data[1] = epw*1000/ hamletLatency +"";
				data[2] = epw*1000/ gretaLatency +"";

				break;
			case "lat":
				filename = latencyFile;
				header[0]= "epw";
				header[1]= "Hamlet latency";
				header[2]= "Greta latency";
				data[0] = epw+"";
				data[1] = hamletLatency +"";
				data[2] = gretaLatency +"";
				break;
			case "mem":
				filename = memoryFile;
				header[0]= "epw";
				header[1]= "Hamlet memory";
				header[2]= "Greta memory";
				data[0] = epw+"";
				data[1] = hamletMemory+"";
				data[2] = gretaMemory +"";
				break;
		}
		File file = new File("output/"+ filename);


		try {
			if(!file.exists()){
				file.createNewFile();
				FileWriter outputfile = new FileWriter(file, true);
				CSVWriter writer = new CSVWriter(outputfile);
				writer.writeNext(header);
				writer.close();
			}
			FileWriter fileWriter = new FileWriter(file, true);
			CSVWriter writer = new CSVWriter(fileWriter);
			//write the data
			writer.writeNext(data);
			writer.close();


		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
