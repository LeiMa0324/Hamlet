package event;

import java.util.ArrayList;

public class EventSequence {
	
	public ArrayList<Event> events;
	
	public EventSequence (ArrayList<Event> es) {
		events = es;
	}
	
	public int getStart() {
		return events.get(0).getStart();
	}
	
	public int getEnd() {
		return events.get(events.size()-1).getEnd();
	}
	
	public boolean allFlagged () {
		for (Event event : events) {
			if (!event.flagged) return false;
		}
		return true;
	}
	
	public boolean equals (Object o) {
		if (o instanceof EventSequence) {
			EventSequence seq = (EventSequence) o;
			if (events.size() != seq.events.size()) return false;
			for (int i=0; i<events.size(); i++) {
				if (!events.get(i).equals(seq.events.get(i))) return false;
			}
			return true;
		} else {
			return false;
		}		
	}

	public String toString() {
		
		String result = "";
		
		for (Event e : events) {
			result += e.toString();
		}
		
		return result;
	}
}