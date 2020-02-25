package Greta.graph;

import Greta.event.*;

public abstract class MuseNode {
	
	// to get id: node_name.event.id
	public Event event;
	
	public MuseNode(Event e) {
		event = e;
	}
	
	public abstract int getMemReq();
}