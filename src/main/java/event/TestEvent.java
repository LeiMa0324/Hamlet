package event;

public class TestEvent extends Event {

	int group;
	int attr;
		
	public TestEvent (int id, int sec, int t, int g) {
		super(id, sec);
		type = t;
		group = g;
		attr = 0;
	}
	
	public static TestEvent parse (int id, String line) {
		
		String[] values = line.split(",");
		
		int sec = Integer.parseInt(values[0]);
		int t = Integer.parseInt(values[1]);
		int g = Integer.parseInt(values[2]);
		
        TestEvent event = new TestEvent(id,sec,t,g); 
           	
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
		return  attr < ((TestEvent)next).attr;
	}

	@Override
	public boolean down(Event next) {
		return  attr > ((TestEvent)next).attr;
	}

	@Override
	public String toString() {
		return "" + id;
	}

}
