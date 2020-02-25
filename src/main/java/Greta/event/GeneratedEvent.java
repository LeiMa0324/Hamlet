package Greta.event;

// time stamp, type, group

public class GeneratedEvent extends Event {

	int group;
	int attr;
		
	public GeneratedEvent (int id, int sec, int t, int g) {
		super(id, sec);
		type = t;
		group = g;
		attr = 0;
	}
	
	public static GeneratedEvent parse (int id, String line) {
		
		String[] values = line.split(",");
		//in case that sec in float format
		float float_sec = Float.parseFloat(values[0]);
		int sec = (int)float_sec;
		int t = Integer.parseInt(values[1]);
		int g = Integer.parseInt(values[2]);
		
        GeneratedEvent event = new GeneratedEvent(id,sec,t,g); 
           	
        return event;
	}

	@Override
	public String getSubstreamid() {
		return group+"";
	}

	@Override
	public boolean isRelevant() {
		return true;
	}

	@Override
	public boolean up(Event next) {
		return  attr < ((GeneratedEvent)next).attr;
	}

	@Override
	public boolean down(Event next) {
		return  attr > ((GeneratedEvent)next).attr;
	}

	@Override
	public String toString() {
		return "" + id;
	}

}
