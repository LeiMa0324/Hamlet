package baselines.commons.templates;

import java.util.ArrayList;
import java.util.HashMap;

public class MultiQueryTemplate {
	public HashMap<Integer, MultiQueryType> Vertices;

	public MultiQueryTemplate(ArrayList<String> queries) {
		// current version
		Vertices = new HashMap<Integer, MultiQueryType>();
		int qid = 0;
		for (String P : queries) {
			addPattern(P, qid);
			qid++;
		}
	}
	
	public MultiQueryTemplate(String P) {
		// current version
		Vertices = new HashMap<Integer, MultiQueryType>();
		int qid = 0;
		addPattern(P, qid);
		qid++;
	}
	
	private void addPattern(String P, int qid) {
//		System.out.println("Adding " + P + " to hamletTemplate.");
		String[] subseq = P.split(",");
		for (int i=0; i < subseq.length; i++) {
			boolean kleene = false;
			if (subseq[i].endsWith("+")) {
				kleene = true;
				subseq[i] = subseq[i].substring(0, subseq[i].length() - 1);
			}
			int t_name = Integer.parseInt(subseq[i]);
			MultiQueryType type = Vertices.get(t_name);
			if (type == null) {
				type = new MultiQueryType(subseq[i]);
				Vertices.put(t_name, type);
			}
			if (i == 0) {
				type.addPredecessor(qid, "START");
			} else {
				type.addPredecessor(qid, subseq[i-1]);
			}
			if (kleene) {
				type.addPredecessor(qid);
			}
			if (qid == 0 && i == subseq.length-1) {
				type.addEnd();
			}
		}
	}
	
	public String toString() {
		String out = "";
		for (MultiQueryType mqt : Vertices.values()) {
			out += mqt.name + ": ";
			for (ArrayList<Integer> al : mqt.instructions) {
				for (int pred : al) {
					out += pred + ", ";
				}
			}
			out += "\n";
		}
		return out;
	}
}
