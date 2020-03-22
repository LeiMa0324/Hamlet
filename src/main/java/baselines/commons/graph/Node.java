package baselines.commons.graph;

import java.math.BigInteger;
import baselines.commons.*;
import baselines.commons.event.Event;

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