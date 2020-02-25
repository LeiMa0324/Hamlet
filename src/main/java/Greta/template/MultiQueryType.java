package Greta.template;

import java.util.ArrayList;
import java.util.HashMap;

public class MultiQueryType {
	public int name;
	public boolean ends;
	public ArrayList<ArrayList<Integer>> instructions; // list of predecessors where predecessor is type
	public String var; // type name
	public HashMap<Integer, Integer> query_lookup; // <query id, index in instructions>
	public boolean expression;

	public MultiQueryType(String s) {
		name = Integer.parseInt(s);
		ends = false;
		instructions = new ArrayList<ArrayList<Integer>>();
		query_lookup = new HashMap<Integer, Integer>();
		expression = true;
	}
	
	public void addPredecessor(int qid, String predecessor) {
		Integer x = query_lookup.get(qid);
		int pred_int;
		if (predecessor.equals("START")) {
			pred_int = -1;
			expression = false; // first event type is not shared
		} else {
			pred_int = Integer.parseInt(predecessor);
		}
		if (x != null) {
			instructions.get(x).add(pred_int);
		} else {
			x = instructions.size();
			query_lookup.put(qid, x);
			instructions.add(new ArrayList<Integer>());
			instructions.get(x).add(pred_int);
		}
	}
	
	// kleene self
	public void addPredecessor(int qid) {
		Integer x = query_lookup.get(qid);
		if (x != null) {
			instructions.get(x).add(name);
		} else {
			x = instructions.size();
			query_lookup.put(qid, x);
			instructions.add(new ArrayList<Integer>());
			instructions.get(x).add(name);
		}
	}
	
	public void addEnd() {
		ends = true;
	}
}
