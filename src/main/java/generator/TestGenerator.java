package generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//import java.util.ArrayList;
import java.util.Random;

public class TestGenerator {
	//-l 30 -t 100 -stream  ../../Documents/datasets/muse/streamshort.txt

	public static void main(String[] args) {
		/*** Set default input parameters ***/
		int l = 50; // length of stream
		int t = 100; // number of event types
		int a = 20; // attributes
		int eps = 3000; // events per second
		String file_of_stream = "";
		
		/*** Read input parameters ***/
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-l"))		l = Integer.parseInt(args[++i]);
			if (args[i].equals("-t")) 		t = Integer.parseInt(args[++i]);
			if (args[i].equals("-a")) 		a = Integer.parseInt(args[++i]);
			if (args[i].equals("-eps"))		eps = Integer.parseInt(args[++i]);
			if (args[i].equals("-stream")) file_of_stream = args[++i];
		 }

		Random random = new Random();
		int second_id = 0;
		int events_per_current_sec = 0;
//		double prob = 0.1;
		int type = 0;
		
		try {		 
			File output_file = new File(file_of_stream);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			for (int j=0; j<l; j++) {
				if (events_per_current_sec >= eps) {
					second_id++;
					events_per_current_sec = 0;
				}
				// make event types 0-9 less frequent than others
//				if (t > 15) {
//					prob = random.nextDouble();
//					if (prob > 0.05) {
//						type = (random.nextInt(t-10)+10);
//					} else {
//						type = random.nextInt(10);
//					}
//				} else {
//					type = random.nextInt(t);
//				}
				type = random.nextInt(t);
				output.append(second_id + "," + type + "," + random.nextInt(a) + "\n");
				events_per_current_sec++;
			}	
			output.close();
		} catch (IOException e) { e.printStackTrace(); }
		
		System.out.println("Done!");
	}

}
