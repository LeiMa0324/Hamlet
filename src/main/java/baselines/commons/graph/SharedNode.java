package baselines.commons.graph;

import baselines.commons.event.Event;

import java.math.BigInteger;
import java.util.HashMap;

public class SharedNode extends MuseNode {
	
	public HashMap<Integer, BigInteger> expression;
	
	public SharedNode(Event e) {
		super(e);
		expression = new HashMap<Integer, BigInteger>();
	}
	
	public int getMemReq() {
		return 8 + 8*expression.size();
	}
	
	public String toString() {
		String out = event.id + "... ";
		for (int id : expression.keySet()) {
			out += id + ": " + expression.get(id) + ", ";
		}
		return out;
	}
}