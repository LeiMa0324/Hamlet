package event;

import graph.Node;

public class EventTrend {
	
	public Node first_node;
	public Node last_node;
		
	// A sequence is a string of comma separated event ids
	public String sequence;
		
	public EventTrend (Node fn, Node ln, String seq) {
		first_node = fn;
		last_node = ln;
		sequence = seq;
	}
		
	public int getEventNumber () {
		
		String[] ids = sequence.split(";");		
		return ids.length;
	}
}
