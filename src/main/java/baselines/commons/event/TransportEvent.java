package baselines.commons.event;

public class TransportEvent extends Event {
	
	int passenger;
	int type;
	int station;
	int duration;
		
	public TransportEvent (int id, int sec, int p, int t, int s, int d) {
		super(id, sec);
		passenger = p;
		type = t;
		station = s;			
		duration = d;
	}
	
	public static TransportEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int id = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
		int pass = Integer.parseInt(values[2]);
		int typ = Integer.parseInt(values[3]);
		int stat = Integer.parseInt(values[4]);
		int dur = Integer.parseInt(values[5]);
		
        TransportEvent event = new TransportEvent(id,sec,pass,typ,stat,dur); 
        
    	//System.out.println(event.toString());    	
        return event;
	}	
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean equals (StockEvent other) {
		return id == other.id;
	}
	
	public String getSubstreamid() {
		return passenger + "";
	}
	
	public boolean up(Event next) {
		return duration < ((TransportEvent)next).duration;
	}
	
	public boolean down(Event next) {
		return duration > ((TransportEvent)next).duration;
	}
	
	public String toString() {
		return "" + id;
	}
}
