package operator;

import java.util.ArrayList;
import event.*;

public class Sequence {
	
	public static ArrayList<EventSequence> generate (ArrayList<EventSequence> input1, ArrayList<EventSequence> input2) {
		
		ArrayList<EventSequence> results = new ArrayList<EventSequence>();
		
		for (EventSequence e1 : input1) {
			for (EventSequence e2 : input2) {
				if (e1.getEnd() < e2.getStart()) {
					ArrayList<Event> events = new ArrayList<Event>();
					events.addAll(e1.events);
					events.addAll(e2.events);
					EventSequence e = new EventSequence(events);
					results.add(e);
				}
			}
		}		
		return results;	
	}
}