package graph;

import java.util.ArrayList;

public class NodesPerSecond {
	
	public int id;
	public int second;
	public ArrayList<Node> nodes_per_second;
	public boolean marked;
	
	public NodesPerSecond (int i, int sec) {
		id = i;
		second = sec;
		nodes_per_second = new ArrayList<Node>();
		marked = false;
	}
}
