package Greta.template;

import java.util.ArrayList;

public class SingleQueryType {
	public int name;
	private boolean end;
	public ArrayList<Integer> predecessors;
	
	public SingleQueryType(String n, boolean e, boolean kleene) {
		name = Integer.parseInt(n);
		end = e;
		predecessors = new ArrayList<Integer>();
		if (kleene) {
			predecessors.add(name);
		}
	}
	
	public void addPredecessor(int predecessor) {
		predecessors.add(predecessor);
//		System.out.println("SingleQueryType " + name + " has predecessors " + predecessors + "; " + end + " end");
	}
	
	public boolean isEND() {
		return end;
	}
}
