package generator;

import java.util.ArrayList;
import java.util.Random;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PatternGenerator {

	public static void main(String[] args) {
		/*** Set default input parameters ***/
		int k = 50; // number of patterns
		int l = 20; // max length of patterns
		int t = 100; // number of event types
		double Kleene_perc = 1.0;
		String file_of_queries = "";
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-k"))		k = Integer.parseInt(args[++i]);
			if (args[i].equals("-l"))		l = Integer.parseInt(args[++i]);
			if (args[i].equals("-t")) 		t = Integer.parseInt(args[++i]);
			if (args[i].equals("-kleene"))	Kleene_perc = Double.parseDouble(args[++i]);
			
			if (args[i].equals("-queries")) file_of_queries = args[++i];
		 }

		Random random = new Random();
		
		try {		 
			File output_file = new File(file_of_queries);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			
			for (int i=0; i<k; i++) {
				String p = "";
				ArrayList<Integer> used_types = new ArrayList<Integer>();
				int length = random.nextInt(l) + 1;
				if (length < 2) { length = 2; }
				for (int j=0; j<length; j++) {
					int event_type=-1;
					while (event_type<0 || used_types.contains(event_type)) { event_type = random.nextInt(t); }
					p += event_type;
					if (random.nextDouble() < Kleene_perc) { p += "+"; }
					p += ",";
					used_types.add(event_type);
				}
				output.append(p + "\n");
			}
			output.close();
		} catch (IOException e) { e.printStackTrace(); }
		
		System.out.println("Done!");
	}
}
