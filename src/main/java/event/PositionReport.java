package event;

public class PositionReport extends Event {

	public int vid; 
	public int spd; 
	public int xway; 
	public int lane;
	public int dir; 
	public int seg;
	public int pos;	
	
	public PositionReport (int id, int sec, int v, int s, int x, int l, int d, int s1, int p) {
		super(id, sec);
		vid = v;
		spd = s;
		xway = x;
		lane = l;
		dir = d;
		seg = s1;
		pos = p;		
	}
	
	/**
	 * Parse the given line and construct a position report.
	 * @param line	
	 * @return position report
	 */
	public static PositionReport parse (String line) {
		
		String[] values = line.split(",");
		
		int id = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
        int vid = Integer.parseInt(values[2]);          	
    	int spd = Integer.parseInt(values[3]);
    	int xway = Integer.parseInt(values[4]);
    	int lane = Integer.parseInt(values[5]);
    	int dir = Integer.parseInt(values[6]);
    	int seg = Integer.parseInt(values[7]);
    	int pos = Integer.parseInt(values[8]);    
    	    	    	
    	PositionReport event = new PositionReport(id,sec,vid,spd,xway,lane,dir,seg,pos);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public String getSubstreamid() {
		return lane + "_" + dir;
		//return xway + "_" + dir + "_" + seg + "_" + vid;
	}
	
	/** 
	 * Return true if this position report is correct.
	 * Return false otherwise.
	 */
	public boolean isRelevant () {
		return id>=0 && sec>=0 && spd>=0 && xway>=0 && lane>=0 && dir>=0 && seg>=0 && pos>=0;
	}
	
	public boolean up(Event next) {
		return true;
	}
	
	public boolean down(Event next) {
		return true;
	}
	
	public String toString() {
		return "" + id;
	}
	
	/** 
	 * Print this position report to file 
	 */
	public String toFile() {
		return id + "," + sec + "," + vid + "," + spd + "," + xway + "," + lane + "," + dir + "," + seg + "," + pos + "\n";		
	}	
}
