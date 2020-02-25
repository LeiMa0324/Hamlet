package executor;

import Greta.event.Stream;
import Greta.event.StreamPartitioner;
import Greta.template.SingleQueryTemplate;
import Greta.transaction.GretaMQ;
import Greta.transaction.TransactionMQ;
import Hamlet.Graph.Graph;
import Hamlet.Template.Template;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
//import java.util.ArrayList;


public class Test {

	public static void main(String[] args) throws FileNotFoundException {
		String streamFile_format = "src/main/resources/Streams/GenStream_%d.txt";
		String queryFile = "src/main/resources/Queries/SampleQueries.txt";
		String logFile = "latency.csv";
		for (int i =100; i<1100;i+=100){
			String streamFile = String.format(streamFile_format,i);
			SingleRun(streamFile,queryFile, logFile,i);
		}


	}

	static void SingleRun(String streamFile, String queryFile,String logFile,int numofSnapshots){

		ArrayList<String> queries = new ArrayList<String>();
		//read query file
		try {
			Scanner query_scanner = new Scanner(new File(queryFile));
			while (query_scanner.hasNextLine()) {
				queries.add(query_scanner.nextLine());
			}
			query_scanner.close();
		} catch(FileNotFoundException e) {e.printStackTrace();}


		//Hamlet
		System.out.println("===============================HAMLET====================================");
		Template template = new Template(queries);
		System.out.println(template.getNodeList());
		Graph g = new Graph(template,streamFile);
		long start =  System.currentTimeMillis();
		g.run();
		long end =  System.currentTimeMillis();
		long hamletDuration = end - start;
		System.out.println("Hamlet: duaration is "+hamletDuration);


		//GRETA
		System.out.println("===============================GRETA====================================");
		long gretaDuration = 0;
		try {
			CountDownLatch done = new CountDownLatch(1);
			AtomicLong latency = new AtomicLong(0);
			AtomicInteger memory = new AtomicInteger(0);
			StreamPartitioner sp = new StreamPartitioner("gen", streamFile, 22004);
			Stream stream = sp.partition();
			TransactionMQ TrS;        //虚类，所有的算法都implement这个虚类

			TrS = new GretaMQ(done, latency, memory, sp);

			for (int i = 0; i < queries.size(); i++) {      //将一个query创建一个query
				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
				((GretaMQ) TrS).addQuery(query);
			}
			TrS.run();
			done.await();
			System.out.println("GRETA: duaration is "+latency.get());
			gretaDuration = latency.get();
		} catch (InterruptedException e) { e.printStackTrace(); }

		System.out.println("===============================Logging====================================");
		logging(logFile,numofSnapshots, hamletDuration, gretaDuration);

	}

	public static void logging(String outputFile,int numofSnapshots, long hamletDuration, long gretaDuration ) {
		// first create file object for file placed at location
		// specified by filepath
		File file = new File("output/"+outputFile);
		if(!file.exists()){
			try {
				file.createNewFile();
				FileWriter outputfile = new FileWriter(file, true);
				CSVWriter writer = new CSVWriter(outputfile);
				String[] header = {"# of snapshot", "Hamlet", "Greta"};
				writer.writeNext(header);
				//write the first line data
				String[] data = {numofSnapshots+"", hamletDuration+"", gretaDuration+""};
				writer.writeNext(data);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				FileWriter outputfile = new FileWriter(file, true);	//trUe 开启append 模式
				// create CSVWriter object filewriter object as parameter
				CSVWriter writer = new CSVWriter(outputfile);
				// add data to csv
				String[] data = {numofSnapshots+"", hamletDuration+"", gretaDuration+""};
				writer.writeNext(data);
				// closing writer connection
				writer.close();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}
}
