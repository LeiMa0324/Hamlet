package executor;

import Greta.event.Stream;
import Greta.event.StreamPartitioner;
import Greta.template.SingleQueryTemplate;
import Greta.transaction.*;
import Hamlet.Graph.Graph;
import Hamlet.Template.Template;
import lombok.Data;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.opencsv.CSVWriter;

import javax.naming.ldap.PagedResultsControl;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
//import java.util.ArrayList;

@Data
public class Executor {
	private String streamFile;
	private String queryFile;
	private String logFile;
	private Template template;
	private Graph g;
	private ArrayList<String> queries;
	private int epw;
	private long hamletDuration;
	private long gretaDuration;

	public Executor(String streamFile, String queryFile, String logFile){
		this.streamFile = "src/main/resources/Streams/SampleStream.txt";
		this.queryFile = "src/main/resources/Queries/SampleQueries.txt";
		this.logFile = "throughput.csv";
		this.epw = 500000;

		this.queries = new ArrayList<String>();
		//read query file
		try {
			Scanner query_scanner = new Scanner(new File(queryFile));
			while (query_scanner.hasNextLine()) {
				queries.add(query_scanner.nextLine());
			}
			query_scanner.close();
		} catch(FileNotFoundException e) {e.printStackTrace();}

		hamletRun();
//		gretaRun();
//		logging();

	}

	public void hamletRun(){

		//Hamlet
		System.out.println("===============================HAMLET====================================");
		this.template = new Template(queries);
		this.g = new Graph(template,streamFile, epw);	//epw==400k
		long start =  System.currentTimeMillis();
		g.run();
		long end =  System.currentTimeMillis();
		hamletDuration = end - start;
		System.out.println("Hamlet: duaration is "+hamletDuration);

	}

	public void gretaRun(){

		System.out.println("===============================GRETA====================================");
		this.gretaDuration =0;
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
			System.out.println("GRETA: duaration is "+latency.get());
			this.gretaDuration = latency.get();

		} catch (InterruptedException e) { e.printStackTrace(); }

	}

	public void logging() {

		System.out.println("===============================Logging====================================");
		File file = new File("output/"+logFile);

		try {
			if(!file.exists()){
				file.createNewFile();
				FileWriter outputfile = new FileWriter(file, true);
				CSVWriter writer = new CSVWriter(outputfile);
				String[] header = {"#epw", "Hamlet throughput", "Greta throughput"};
				writer.writeNext(header);
			}
			FileWriter outputfile = new FileWriter(file, true);
			CSVWriter writer = new CSVWriter(outputfile);
			//write the data
			String[] data = {epw+"", epw*1000/hamletDuration+"", epw*1000/gretaDuration+""};
			writer.writeNext(data);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
