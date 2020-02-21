package query;

import event.*;

public class Query_old {
	
	// any, next, cont
	public String semantics; 
	// up, down, none, required_percentage% 
	public String predicate_on_adjacent_events;
	
	public Query_old (String sem, String pred) {
		semantics = sem;
		predicate_on_adjacent_events = pred;
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
	
	public boolean compressible () {
		return !semantics.equals("any") || predicate_on_adjacent_events.equals("none");
	}

}
