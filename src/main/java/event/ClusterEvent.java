package event;

public class ClusterEvent extends Event {
	
	public int mapper;
	public int job;
	public int cpu;
	public int memory;
	public int load;
	
	public ClusterEvent (int i, int sec, int m, int j, int c, int mem, int l) {
		super(i, sec);
		mapper = m;
		job = j;
		cpu = c;
		memory = mem;
		load = l;
	}
	
	public boolean equals (ClusterEvent other) {
		return id == other.id;
	}
	
	public static ClusterEvent parse (String line) {
		
		String[] values = line.split(",");
		
		int i = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
        int m = Integer.parseInt(values[2]);
        int j = Integer.parseInt(values[3]);          	
        int c = Integer.parseInt(values[4]);
        int mem = Integer.parseInt(values[5]);
        int l = Integer.parseInt(values[6]);
    	    	    	
    	ClusterEvent event = new ClusterEvent(i,sec,m,j,c,mem,l);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public String getSubstreamid() {
		return mapper + "";
	}
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean up(Event next) {
		return load < ((ClusterEvent)next).load;
	}
	
	public boolean down(Event next) {
		return load > ((ClusterEvent)next).load;
	}
	
	public String toString() {
		return "" + id;
	}
	
	public String toStringComplete() {
		return "id " + id + "sec " + sec + " mapper " + mapper + " job " + job + " cpu " + cpu + " mem " + memory + " load " + load;
	}
}
