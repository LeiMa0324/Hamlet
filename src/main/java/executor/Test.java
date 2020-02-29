package executor;

import Greta.event.Event;
import Greta.event.Stream;
import Greta.event.StreamPartitioner;
import Greta.template.SingleQueryTemplate;
import Greta.transaction.GretaMQ;
import Greta.transaction.TransactionMQ;

import Hamlet.Graph.Graph;
import Hamlet.Template.EventType;
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
		String streamFile = "src/main/resources/Streams/ShortStream.txt";
		String queryFile = "src/main/resources/Queries/SampleQueries.txt";
		String logFile = "throughput.csv";
//		for (int epw =200000; epw<600000;epw+=100000){		//200k-500k， 50k step
//			SingleRun(streamFile,queryFile, logFile,epw);
//		}
//		ArrayList<String> queries = new ArrayList<String>();
//		//read query file
//		try {
//			Scanner query_scanner = new Scanner(new File(queryFile));
//			while (query_scanner.hasNextLine()) {
//				queries.add(query_scanner.nextLine());
//			}
//			query_scanner.close();
//		} catch(FileNotFoundException e) {e.printStackTrace();}
//		Template tmp = new Template(queries);
//		for (String e: tmp.getStrToEventTypeHashMap().keySet()){
//			System.out.println(tmp.getEventTypebyString(e).toString());
//			System.out.println(tmp.getEventTypebyString(e).getTypes());
//		}
//		System.out.println(tmp.toString());
		SingleRun(streamFile, queryFile, logFile, 500000);


	}

	static void SingleRun(String streamFile, String queryFile,String logFile,int epw){

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
		Graph g = new Graph(template,streamFile, epw);	//epw==400k
		long start =  System.currentTimeMillis();
		g.run();
		long end =  System.currentTimeMillis();
		long hamletDuration = end - start;
		System.out.println("Hamlet: duaration is "+hamletDuration);
//
//
//		//GRETA
////		System.out.println("===============================GRETA====================================");
//		long gretaDuration =0;
//		try {
//			CountDownLatch done = new CountDownLatch(1);
//			AtomicLong latency = new AtomicLong(0);
//			AtomicInteger memory = new AtomicInteger(0);
//			StreamPartitioner sp = new StreamPartitioner("gen", streamFile, epw);
//			Stream stream = sp.partition();
//			TransactionMQ TrS;        //虚类，所有的算法都implement这个虚类
//
//			TrS = new GretaMQ(done, latency, memory, sp);
//
//			for (int i = 0; i < queries.size(); i++) {      //将一个query创建一个query
//				SingleQueryTemplate query = new SingleQueryTemplate(queries.get(i));
//				((GretaMQ) TrS).addQuery(query);
//			}
//			TrS.run();
//			done.await();
//			System.out.println("GRETA: duaration is "+latency.get());
//			gretaDuration = latency.get();
//		} catch (InterruptedException e) { e.printStackTrace(); }
//
//		System.out.println("===============================Logging====================================");
//		logging(logFile,epw, hamletDuration, gretaDuration);
//
	}
//
//	public static void logging(String outputFile,int epw, long hamletDuration, long gretaDuration ) {
//		// first create file object for file placed at location
//		// specified by filepath
//		File file = new File("output/"+outputFile);
//		if(!file.exists()){
//			try {
//				file.createNewFile();
//				FileWriter outputfile = new FileWriter(file, true);
//				CSVWriter writer = new CSVWriter(outputfile);
//				String[] header = {"#epw", "Hamlet throughput", "Greta throughput"};
//				writer.writeNext(header);
//				//write the first line data
//				String[] data = {epw+"", epw*1000/hamletDuration+"", epw*1000/gretaDuration+""};
//				writer.writeNext(data);
//				writer.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		else {
//			try {
//				FileWriter outputfile = new FileWriter(file, true);	//trUe 开启append 模式
//				// create CSVWriter object filewriter object as parameter
//				CSVWriter writer = new CSVWriter(outputfile);
//				// add data to csv
//				String[] data = {epw+"", epw*1000/hamletDuration+"", epw*1000/gretaDuration+""};
//				writer.writeNext(data);
//				// closing writer connection
//				writer.close();
//			}catch (IOException e){
//				e.printStackTrace();
//			}
//		}
//	}
}
