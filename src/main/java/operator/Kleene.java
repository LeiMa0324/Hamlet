package operator;

import java.util.ArrayList;
import event.*;

public class Kleene {

public static ArrayList<EventSequence> generate (ArrayList<EventSequence> input) {
		
		ArrayList<EventSequence> all_results = new ArrayList<EventSequence>();
		ArrayList<EventSequence> prev_results = new ArrayList<EventSequence>();
		ArrayList<EventSequence> new_results = new ArrayList<EventSequence>();
		
		// Base case: Length 1
		all_results.addAll(input);
		prev_results.addAll(input);		
		
		// Inductive case: Length 2 - input.length 
		for (int iteration = 1; iteration < input.size(); iteration++) {
			for (int i=0; i<prev_results.size(); i++) {
				EventSequence complex = prev_results.get(i);
				for (int j=0; j<input.size(); j++) {					
					EventSequence simple = input.get(j);				
					if (complex.getEnd() < simple.getStart()) {		
						ArrayList<Event> events = new ArrayList<Event>();
						events.addAll(complex.events);
						events.addAll(simple.events);
						EventSequence e = new EventSequence(events);
						new_results.add(e);
					}
				}
			}
			all_results.addAll(new_results);
			prev_results.clear();
			prev_results.addAll(new_results);
			new_results.clear();			
		}
		return all_results;	
	}
}