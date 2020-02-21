package event;

public class RawEvent extends Event {
	
	String type;
	
	public RawEvent (int i, int s, String t) {
		super(i,s);
		type = t;
	}	
	
	public static RawEvent parse (String line) {
		
		String[] values = line.split(":");
		
		String t = values[0];
		int i = Integer.parseInt(values[1]);
		int s = Integer.parseInt(values[1]);
	           	    	    	
    	RawEvent event = new RawEvent(i,s,t);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public String getSubstreamid() {
		return "";
	}
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean up(Event next) {
		return true;
	}
	
	public boolean down(Event next) {
		return true;
	}
	
	public String toString() {
		return type + sec;
	}
}