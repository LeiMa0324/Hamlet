//OLD

package template;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;
import java.util.LinkedList;
//import java.util.Set;

/** Predecessor broken **/

public class Template {
	private HashSet<String> Vnames;
	private HashMap<String, EventType_old> Vertices;
	private int numQueries;
	
	public Template() {
		Vnames = new HashSet<String>();
		Vertices = new HashMap<String, EventType_old>();
		numQueries = 0;
		Vnames.add("START");
		Vertices.put("START", new EventType_old("START"));
	}
	
	public void addPattern(String p) {
		Stack<String> start_kleene = new Stack<String>();
		String[] types = p.toString().split(",");
		
		for (int i=0; i<types.length; i++) {
			boolean selfkleene = false;
			int num_nested_kleene = 0;
			boolean openP = types[i].charAt(0) == '(';
			int closeP = types[i].lastIndexOf(")+");
			String e_type = types[i];

			// If event type starts a kleene sub-pattern, push into a stack
			while (openP) {
				start_kleene.push(this.clean(e_type));
				e_type = e_type.substring(1);
				openP = e_type.charAt(0) == '(';
			}
			
			// If event type ends a kleene
			while (closeP != -1) {
				num_nested_kleene++;
				e_type = e_type.substring(0, closeP);
				closeP = e_type.lastIndexOf(")+");
			}
			
			int plus = e_type.indexOf("+");
			// Handles E+
			if (plus != -1) {
				selfkleene = true;
				e_type = e_type.substring(0, plus);
			}
			
			// Add vertex to template
			this.addVertex(e_type);
			EventType_old vertexE = Vertices.get(e_type);
			vertexE.add2Query(numQueries+"");
			
			// Add Kleene edges
			if (selfkleene) {
				vertexE.addPredecessor(e_type, numQueries+"", "none");
				Vertices.get(e_type).addSuccessor(e_type, numQueries+"");
			}
			for (int j = 0; j < num_nested_kleene; j++) {
				this.addKleeneEdge(e_type, start_kleene.pop(), numQueries+"", "none");
			}
			
			// Add SEQ edges
			if (i > 0) {
				String pr_type = this.clean(types[i-1]);
				EventType_old vertexPr = Vertices.get(pr_type);
				vertexE.addPredecessor(pr_type, numQueries+"", "none");
				vertexPr.addSuccessor(e_type, numQueries+"");
				// potentially shared queries
//				Set<String> PSqueries = new HashSet<String>(vertexE.getQueriesOnEdge(pr_type));
//				if (PSqueries.size() > 1) {
//	 				boolean potential = true;
//					for (Set<String> querySet : vertexPr.getGSQueries()) {
//						if (querySet.containsAll(PSqueries)) {
//							vertexE.addGSQueries(PSqueries);
//							potential = false;
//							break;
//						}
//					}
//					if (potential) { vertexE.addPSQueries(PSqueries); }
//				}
			} else {
				// Annotate start event type
				vertexE.addPredecessor("START", numQueries+"", "none");
				Vertices.get("START").addSuccessor(e_type, numQueries+"");
				// guaranteed shared queries
				HashSet<String> queries = new HashSet<String>(vertexE.getQueriesOnEdge("START"));
				vertexE.generateGSQueries(queries);
			}
			
		}
		
		// Annotate end event type
		Vertices.get(this.clean(types[types.length-1])).addEnd(numQueries+"");
		numQueries++;
	}
	
	private void addVertex(String e) {
		EventType_old u = Vertices.get(e);
		if (u==null) {
			Vnames.add(e);
			Vertices.put(e, new EventType_old(e));
		}
	}
	
	// This method does not check for all possible errors.
	// We assume that the user has verified that the qid and event types are correct.
	public void addKleeneEdge(String fr, String to, String qid, String predicate) {
		EventType_old u = Vertices.get(fr);
		EventType_old v = Vertices.get(to);
		if (u==null || v==null) {
			System.err.println("Edge addition failed between " + fr + " and " + to);
			return;			// the vertices don't exist
		}
		v.addPredecessor(fr, qid, predicate);
		u.addSuccessor(to, qid);
	}
	
	public String clean(String E) {
		int start_trim = E.lastIndexOf("(")+1;
		int end_trim = E.length();
		int end_par = E.indexOf(")");
		int end_plus = E.indexOf("+");

		if (end_plus != -1 && (end_par > end_plus || end_par == -1)) {
			end_trim = end_plus;
		} else if (end_par != -1 && (end_plus > end_par || end_plus == -1 )) {
			end_trim = end_par;
		}
		return E.substring(start_trim, end_trim);
	}
	
	public void cleanupAnnotations() {
		for (String v : Vnames) {
			Vertices.get(v).generateGNSQueries();
		}
	}
	
	public HashSet<String> getVnames() {
		return Vnames;
	}
	
	// Prefix identification using BFS
	public void traverseGuaranteedShared() {
		String currentV = "START";
		HashMap<String, Boolean> visited = new HashMap<String, Boolean>();
		for (String s : Vnames) {
			visited.put(s, false);
		}
		LinkedList<String> queue = new LinkedList<String>();
		visited.put(currentV, true);
		queue.add(currentV);
		
		while (queue.size() != 0) {
			currentV = queue.poll();
			
			EventType_old u = Vertices.get(currentV);
			for (String v : u.getSuccessors().keySet()) {
				HashSet<String> queries = u.getSuccessors().get(v);
				if (!visited.put(v, true) && queries.size() > 1) {
					Vertices.get(v).addGSQueries(queries);
					queue.add(v);
				}
			}
		}
	}
	
	public void checkAnnotations() {
		for (String s : Vnames) {
			EventType_old vertexS = Vertices.get(s);
			System.out.println(s + ": GS " + vertexS.getGSQueries() + "; PS " +
					vertexS.getPSQueries() + "; GNS " + vertexS.getGNSQueries());
		}
	}
	
	public String toString() {
		String s = "";
		for (EventType_old u : Vertices.values()) {
			s += u.toString() + ":";
			for (String v : u.getPredecessors().keySet()) {
				s += v + u.getPredecessors().get(v) + " ";
			}
			s += "END" + u.ends() + " ...Successors... ";
			for (String v : u.getSuccessors().keySet()) {
				s += v + u.getSuccessors().get(v) + " ";
			}
			s += "\n";
		}
		return s;
	}
}
