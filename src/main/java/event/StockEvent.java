package event;

public class StockEvent extends Event {
	
	public int sector;
	public String company;
	public double price;
	public int volume;
	public String trtype;
	
	public StockEvent (int i, int sec, int s, String c, int p) {
		super(i, sec);
		sector = s;
		company = c;
		price = p;
	}
	
	public StockEvent (int i, int sec, int s, String c, double p, int vol, String trt) {
		super(i, sec);
		sector = s;
		company = c;
		price = p;
		volume = vol;
		trtype = trt;
	}
	
	public boolean equals (StockEvent other) {
		return id == other.id;
	}
	
	public static StockEvent parse (String line) {
		
		String[] values = line.split(", ");
		
		int i = Integer.parseInt(values[0]);
		int sec = Integer.parseInt(values[1]);
        int s = Integer.parseInt(values[2]);
        String c = values[3];          	
        double p = Double.parseDouble(values[4]);  
        int v = Integer.parseInt(values[5]);
        String t = values[6]; 
    	    	    	
    	StockEvent event = new StockEvent(i,sec,s,c,p,v,t);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public static StockEvent parse2 (String line) {
		
		String[] values = line.split(" ");
		
		String company = values[0];
		int id = Integer.parseInt(values[1]);
        int volume = Integer.parseInt(values[2]);
        double price = Double.parseDouble(values[3]);          	
        String time_string = values[4];
        String trtype = values[5];
        
        // Parse time
        String[] split_time = time_string.split(":");
        int hour = Integer.parseInt(split_time[0]) - 8;
        int min = Integer.parseInt(split_time[1]);
	    int sec =  new Double(Double.parseDouble(split_time[2])).intValue();
	     // Convert time to seconds
        if (hour>0) sec += hour*60*60;
        if (min>0) sec += min*60;
    	    	    	
    	StockEvent event = new StockEvent(id,sec,1,company,price,volume,trtype);    	
    	//System.out.println(event.toString());    	
        return event;
	}
	
	public String getSubstreamid() {
		return sector + "_" + company;
	}
	
	public boolean isRelevant() {
		return true;
	}
	
	public boolean up(Event next) {
		return price < ((StockEvent)next).price;
	}
	
	public boolean down(Event next) {
		return price > ((StockEvent)next).price;
	}
	
	public String toString() {
		return "" + id;
	}
	
	public String toStringComplete() {
		return "id " + id + "sec " + sec + " sector " + sector + " company " + company + " price " + price;
	}
	
	public String toFile() {
		return id + ", " + sec + ", " + sector + ", " + company + ", " + price + ", " + volume + ", " + trtype + "\n";
	}
	
	public String toFile(int count) {
		return count + ", " + sec + ", " + sector + ", " + company + ", " + price + ", " + volume + ", " + trtype + "\n";
	}
}
