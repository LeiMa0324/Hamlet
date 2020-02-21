package template;

import java.util.ArrayList;

public class SingleQueryType {
	public String name;
	private boolean end;
	public ArrayList<String> predecessors;
	
	public SingleQueryType(String n, boolean e, boolean kleene) {
		name = n;
		end = e;
		predecessors = new ArrayList<String>();
		if (kleene) {
			predecessors.add(n);
		}
	}
	
	public void addPredecessor(String predecessor) {
		predecessors.add(predecessor);
//		System.out.println("SingleQueryType " + name + " has predecessors " + predecessors + "; " + end + " end");
	}
	
	public boolean isEND() {
		return end;
	}
}
