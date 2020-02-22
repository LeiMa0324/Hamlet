package executor;

import Graph.Graph;
import HamletTemplate.Template;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
//import java.util.ArrayList;

public class Test {

	public static void main(String[] args) throws FileNotFoundException {
		String q1="A,B+";
		String q2 ="C,B+";

		ArrayList<String> queries = new ArrayList<String>();
		queries.add(q1);
		queries.add(q2);
		Template template = new Template(queries);
		System.out.println(template.getNodeList());

		Graph g = new Graph(template);
		try{
			Scanner scanner = new Scanner(new File("src/main/resources/Streams/SampleStream.txt"));
			int event_number = 1;
			while (scanner.hasNext()){
				String line = scanner.nextLine();
				g.run(line);
			}
			System.out.println("The final count is");
			System.out.println(g.getCurrentSnapShot());
		}catch (IOException e){
		e.printStackTrace();
	}

	}


}
