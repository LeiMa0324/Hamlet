package template;

import java.util.HashMap;
import java.util.ArrayList;

/***
 *
 */
public class MultiQueryType {
	public String name;
	public ArrayList<int[]> ends; // list of end [qid, index]
	public ArrayList<ArrayList<String[]>> instructions; // list of predecessors where predecessor is [type, index]
	public String var; // type name
	public HashMap<Integer, Integer> query_lookup; // <query id, index in instructions>

	public MultiQueryType(String s) {
		name = s;
		ends = new ArrayList<int[]>();
		instructions = new ArrayList<ArrayList<String[]>>();
		query_lookup = new HashMap<Integer, Integer>();
	}
	
	public void addPredecessor(Integer qid, String predecessor, int index) {
		Integer x = query_lookup.get(qid);
		if (x != null) {
			instructions.get(x).add(new String[] {predecessor, index+""});
		} else {
			x = instructions.size();
			query_lookup.put(qid, x);
			instructions.add(new ArrayList<String[]>());
			instructions.get(x).add(new String[] {predecessor, index+""});
		}
	}
	
	// kleene self
	public void addPredecessor(Integer qid) {
		Integer x = query_lookup.get(qid);
		if (x != null) {
			instructions.get(x).add(new String[] {name, x+""});
		} else {
			x = instructions.size();
			query_lookup.put(qid, x);
			instructions.add(new ArrayList<String[]>());
			instructions.get(x).add(new String[] {name, x+""});
		}
	}
	
	public void addEnd(int qid) {
		Integer x = query_lookup.get(qid);
		if (x != null) {
			ends.add(new int[] {qid, x});
		} else {
			System.err.println("No query found! " + qid + " cannot end " + name);
		}
	}
}
