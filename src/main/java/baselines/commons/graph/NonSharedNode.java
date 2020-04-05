package baselines.commons.graph;

import baselines.commons.event.Event;

import java.math.BigInteger;

public class NonSharedNode extends MuseNode {
	
	public BigInteger count;
	
	public NonSharedNode(Event e) {
		super(e);
		count = new BigInteger("1");
	}
	
	public int getMemReq() {
		return 12;
	}
	
	public String toString() {
		return event.id + " COUNT: " + count;
	}
}