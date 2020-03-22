package baselines.commons.templates;

import baselines.commons.event.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class SingleQueryTemplate {
	// up, down, none, required_percentage% 
	public String predicate_on_adjacent_events;
//	private HashSet<String> TypeNames;
	HashMap<Integer, SingleQueryType> Vertices;
	public Integer start_type;
	
	public SingleQueryTemplate (String patt) {
		predicate_on_adjacent_events = "none";
//		TypeNames = new HashSet<String>();
		Vertices = new HashMap<Integer, SingleQueryType>();
		buildTemplate(patt);
	}
	
	public SingleQueryTemplate (String patt, String pred) {
		predicate_on_adjacent_events = pred;
//		TypeNames = new HashSet<String>();
		Vertices = new HashMap<Integer, SingleQueryType>();
		buildTemplate(patt);
	}
	
	// this code only handles seq(a,b) and a+
	private void buildTemplate(String P) {
		String[] subseq = P.split(",");
		for (int i = 0; i < subseq.length; i++) {
			boolean kleene = false;
			if (subseq[i].endsWith("+")) {
				kleene = true;
				subseq[i] = subseq[i].substring(0, subseq[i].length() - 1);
			}
//			TypeNames.add(subseq[i]);
			boolean end = false;
			if (i == subseq.length-1) { end = true; }
			SingleQueryType type = new SingleQueryType(subseq[i], end, kleene);
			if (i == 0) {
				type.addPredecessor(-1); // START
				start_type = type.name;
			} else {
				type.addPredecessor(Integer.parseInt(subseq[i-1]));
			}
			Vertices.put(Integer.parseInt(subseq[i]), type);
		}
	}
	
	public Integer getEndType() {
		for (SingleQueryType T : Vertices.values()) {
			if (T.isEND()) { return T.name; }
		}
		return -99;
	}
	
	public boolean isEndType(Integer type) {
		return Vertices.get(type).isEND();
	}
	
	public List<Integer> getPredecessors(int type) {
		return Vertices.get(type).predecessors;
	}
	
	public Set<Integer> getTypes() {
		return Vertices.keySet();
	}
	
	public int getPercentage() {
		
		int percentage = 100;
	
		if (predicate_on_adjacent_events.endsWith("%")) {
			String str= predicate_on_adjacent_events.replaceAll("%", "");
			percentage = Integer.parseInt(str);
		}
		return percentage;
	}
	
	public boolean compatible (Event previous, Event following, int id_of_last_compatible_predecessor) {
		
		if (predicate_on_adjacent_events.endsWith("%")) {
			return previous.id <= id_of_last_compatible_predecessor;
		} else {		
		if (predicate_on_adjacent_events.equals("up")) {
			return previous.up(following);
		} else {
		if (predicate_on_adjacent_events.equals("down")) {
			return previous.down(following);	
		} else {
			return true;
		}}} 		
	}

}
