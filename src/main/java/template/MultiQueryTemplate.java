package template;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class MultiQueryTemplate {
	//一个event类型和query type的hash map
	public HashMap<String, MultiQueryType> Vertices;

	//传入quries的 list
	public MultiQueryTemplate(ArrayList<String> queries) {
		Vertices = new HashMap<String, MultiQueryType>();
		int qid = 1;	//每一个query都有一个qid
		for (String P : queries) {
			addNonSharedPattern(P, qid);
			qid++;
		}
	}
	
	public MultiQueryTemplate(ArrayList<String> queries, int suffixLength) {
		Vertices = new HashMap<String, MultiQueryType>();
		int qid = 1;
		for (String P : queries) {
			addSharedPattern(P, qid, suffixLength);
			qid++;
		}
	}
	
	public MultiQueryTemplate(String prefix, int numQueries) {
		Vertices = new HashMap<String, MultiQueryType>();
		int qid = 1;
		for (int i = 0; i < numQueries; i++) {
			addNonSharedPattern(prefix, qid);
			qid++;
		}
	}
	
	private void addNonSharedPattern(String P, int qid) {
//		System.out.println("Adding " + P + " to template.");
		String[] subseq = P.split(",");
		for (int i=0; i < subseq.length; i++) {
			boolean kleene = false;
			if (subseq[i].endsWith("+")) {
				kleene = true;
				subseq[i] = subseq[i].substring(0, subseq[i].length() - 1);
			}
			MultiQueryType type = Vertices.get(subseq[i]);		//获取当前节点的类型，start，end等
			if (type == null) {
				type = new MultiQueryType(subseq[i]);
				Vertices.put(subseq[i], type);	//将节点放入template中
			}
			if (i == 0) {
				type.addPredecessor(qid, "START", -1);
			} else {
				type.addPredecessor(qid, subseq[i-1], Vertices.get(subseq[i-1]).query_lookup.get(qid));
			}
			if (kleene) {
				type.addPredecessor(qid);
			}
			if (i == subseq.length-1) {
				type.addEnd(qid);
			}
		}
	}
	
	private void addSharedPattern(String P, int qid, int NSLength) {
		String[] subseq = P.split(",");
		if (qid == 1) {
			// assume all queries share the same prefix
			for (int i = 0; i < subseq.length-NSLength; i++) {
				boolean kleene = false;
				if (subseq[i].endsWith("+")) {
					kleene = true;
					subseq[i] = subseq[i].substring(0, subseq[i].length() - 1);
				}
				MultiQueryType type = Vertices.get(subseq[i]);
				if (type == null) {
					type = new MultiQueryType(subseq[i]);
					Vertices.put(subseq[i], type);
				}
				if (i == 0) {
					type.addPredecessor(qid, "START", -1);
				} else {
					type.addPredecessor(qid, subseq[i-1], Vertices.get(subseq[i-1]).query_lookup.get(qid));
				}
				if (kleene) {
					type.addPredecessor(qid);
				}
			}
		}
		
		// assume suffix is non-shared
		for (int i=subseq.length-NSLength; i < subseq.length; i++) {
			boolean kleene = false;
			if (subseq[i].endsWith("+")) {
				kleene = true;
				subseq[i] = subseq[i].substring(0, subseq[i].length() - 1);
			}
			MultiQueryType type = Vertices.get(subseq[i]);
			if (type == null) {
				type = new MultiQueryType(subseq[i]);
				Vertices.put(subseq[i], type);
			}
			if (subseq[i-1].endsWith("+")) {
				subseq[i-1] = subseq[i-1].substring(0, subseq[i-1].length() - 1);
			}
			if (i == subseq.length-NSLength) {
				type.addPredecessor(qid, subseq[i-1], Vertices.get(subseq[i-1]).query_lookup.get(1));
			} else {
				type.addPredecessor(qid, subseq[i-1], Vertices.get(subseq[i-1]).query_lookup.get(qid));
			}
			if (kleene) {
				type.addPredecessor(qid);
			}
			if (i == subseq.length-1) {
				type.addEnd(qid);
			}
		}
	}
	
	public Set<String> getTypes() {
		return Vertices.keySet();
	}
}
