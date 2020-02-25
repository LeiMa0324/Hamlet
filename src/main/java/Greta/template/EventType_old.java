package Greta.template;

//import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class EventType_old {
	private String name;
	private HashSet<String> end;
	private HashMap<String, HashMap<String, String>> predecessors; // event type, <qid, predicates>
	private HashMap<String, HashSet<String>> successors; // event type, queries
	private HashSet<Set<String>> GSQueries; // set of guaranteed shared query sets
	private HashSet<String> GNSQueries; // set of guaranteed not shared queries
	private HashSet<Set<String>> PSQueries; // set of  potentially shared query sets
	private HashSet<String> allQueries;
	
	EventType_old(String p) {
		name = p;
		end = new HashSet<String>();
		predecessors = new HashMap<String, HashMap<String, String>>();
		successors = new HashMap<String, HashSet<String>>();
		GSQueries = new HashSet<Set<String>>();
		GNSQueries = new HashSet<String>();
		PSQueries = new HashSet<Set<String>>();
		allQueries = new HashSet<String>();
	}
	
	public boolean containsPredecessor(String s) {
		return predecessors.keySet().contains(s);
	}
	
	public boolean containsSuccessor(String s) {
		return successors.keySet().contains(s);
	}
	
	public Set<String> getQueriesOnEdge(String p) {
		return predecessors.get(p).keySet();
	}
	
	public void addPredecessor(String p, String qid, String label) {
		// label is transition,predicate
		if (!this.containsPredecessor(p)) {
			predecessors.put(p, new HashMap<String, String>());
		}
		predecessors.get(p).put(qid, label);
	}
	
	public void add2Query(String qid) {
		allQueries.add(qid);
	}
	
	public void addGSQueries(Set<String> queries) {
		boolean addQueries = true;
		for (Set<String> oldQueries : GSQueries) {
			if (queries.containsAll(oldQueries)) {
				GSQueries.add(queries);
				GSQueries.remove(oldQueries);
				addQueries = false;
				break;
			}
		}
		if (addQueries) { GSQueries.add(queries); }
	}
	
	public void addPSQueries(Set<String> queries) {
		PSQueries.add(queries);
	}
	
	// Input: candidate gs queries
	public boolean generateGSQueries(Set<String> queries) {
		for (String e : predecessors.keySet()) {
			queries.retainAll(this.getQueriesOnEdge(e));
		}
		if (queries.size() > 1) {
			this.addGSQueries(queries);
			return true;
		}
		else { return false; }
	}
	
	public void generateGNSQueries() {
		for (String qid : allQueries) {
			boolean gns = true;
			for (Set<String> queries : GSQueries) {
				if (queries.contains(qid)) {
					gns = false;
					break;
				}
			}
			if (gns) {
				for (Set<String> queries : PSQueries) {
					if (queries.contains(qid)) {
						gns = false;
						break;
					}
				}
			}
			if (gns) { GNSQueries.add(qid); }
		}
	}
	
	public void addSuccessor(String s, String qid) {
		if (!this.containsSuccessor(s)) {
			successors.put(s, new HashSet<String>());
		}
		successors.get(s).add(qid);
	}
	
	public void addEnd(String qid) {
		end.add(qid);
	}
	
	public HashSet<String> ends() {
		return end;
	}
	
	public HashMap<String, HashMap<String, String>> getPredecessors() {
		return predecessors;
	}
	
	public HashMap<String, HashSet<String>> getSuccessors() {
		return successors;
	}
	
	public HashSet<Set<String>> getGSQueries() {
		return GSQueries;
	}
	
	public HashSet<String> getGNSQueries() {
		return GNSQueries;
	}
	
	public HashSet<Set<String>> getPSQueries() {
		return PSQueries;
	}
	
	public String toString() {
		return name;
	}
}
