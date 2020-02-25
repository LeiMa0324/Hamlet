package Greta.graph;

import Greta.event.*;

import java.math.BigInteger;

public class Node {
	
	public Event event;
	public BigInteger count;
			
	public Node (Event e) {
		event = e;
		count = new BigInteger("0");
	}
	
	public String toString() {
		return event.id + " COUNT: " + count;
	}
}