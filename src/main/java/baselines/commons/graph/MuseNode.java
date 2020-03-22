package baselines.commons.graph;

import baselines.commons.event.Event;

public abstract class MuseNode {
	
	// to get id: node_name.event.id
	public Event event;
	
	public MuseNode(Event e) {
		event = e;
	}
	
	public abstract int getMemReq();
}