package Greta.event;

import java.util.ArrayList;

public abstract class Event {
	
	public int id;
	public int sec;
	public ArrayList<Event> pointers;
	public boolean flagged;
	public boolean marked;
	public int actual_count;
	public int type;
			
	public Event (int i, int s) {
		id = i;		
		sec = s;
		pointers = new ArrayList<Event>();
		flagged = false;
		marked = false;
		actual_count = 0;
	}
	
	public int getStart() {
		return sec;
	}
	
	public int getEnd() {
		return sec;
	}	
	
	public static Event parse (int id, String line, String type) {
		Event event;
		if (type.equals("gen")) {
			event = GeneratedEvent.parse(id, line);
		} else if (type.equals("stock")) { 
			event = StockEvent.parse(id, line); 
		} else if (type.equals("transport")) { 
			event = TransportEvent.parse(line); 
		} else {
			event = PositionReport.parse(line);
		}
		return event;
	}
	
	public boolean equals (Event other) {
		return id == other.id;
	}
	
	public abstract String getSubstreamid();
	public abstract boolean isRelevant();
	public abstract boolean up(Event next);
	public abstract boolean down(Event next);
	public abstract String toString();
	
	/** Print this event with pointers to console **/
	public String toStringWithPointers() {
		String s = id + " : ";
		for (Event predecessor : pointers) {
			s += predecessor.id + ",";
		}
		return s;
	}
}
