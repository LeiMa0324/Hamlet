package graph;

import event.*;
import java.math.BigInteger;

public class MuseNode extends Node {
	
	// to get id: node_name.event.id
//	public HashMap<Integer, BigInteger> counts; // key can be either query id or variable node event id
	public BigInteger[] counts;

	public MuseNode(Event e, int slots) {
		super(e);
//		counts = new HashMap<Integer, BigInteger>();
		counts = new BigInteger[slots];
	}

}
