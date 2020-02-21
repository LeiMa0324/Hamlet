package graph;

import java.math.BigInteger;
import java.util.ArrayList;
import event.*;

public class Node {
	
	public Event event;
	public BigInteger count;
//	public boolean marked;
	public ArrayList<Node> previous;
	public ArrayList<EventTrend> results;
			
	public Node (Event e) {
		event = e;
		count = new BigInteger("0");
//		marked = false;
//		previous = new ArrayList<Node>();
		results = new ArrayList<EventTrend>();
	}
	
	public boolean equals (Node other) {
		return event.equals(other.event);
	}
	
	public void connect (Node old_event) {
		if (!previous.contains(old_event)) previous.add(old_event);
	}
	
	public String toString() {
		return event.id + " COUNT: " + count;
	}
	
	public int getMemoryForResults () {
		int memory = 0;
		for (EventTrend trend : results) {
			memory += trend.sequence.split(";").length;
		}				
		return memory;
	}
	
	public String resultsToString () {
		String result = "";
		for(EventTrend trend : results) { 				
			result += trend.sequence + "\n"; 				
		}
		return result;
	}
}
