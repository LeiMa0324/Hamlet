package baselines.newsharon;
import java.math.BigInteger;

public class SharonType {
	public boolean isSTART;
	public boolean isTRIG;
	public SharonType predecessor;
	public int current_second;
	public int previous_second;
	public BigInteger current_second_count;
	public BigInteger previous_second_count;
	public int queryID; // if shared, set to -1
	
	// needed to keep existing current implementation.....
	public int snapshot_id;
	
	public SharonType(int qid) {
		isSTART = true;
		isTRIG = false;
		queryID = qid;
		resetTime();
	}
	
	public SharonType(boolean t, SharonType p, int qid) {
		isSTART = false;
		isTRIG = t;
		predecessor = p;
		queryID = qid;
		resetTime();
	}
	
	public void updateTime(int t) {
		previous_second = current_second;
		current_second = t;
		previous_second_count = current_second_count;
	}
	
	public void resetTime() {
		current_second = -1;
		previous_second = -2;
		current_second_count = new BigInteger("0");
		previous_second_count = new BigInteger("0");
		
		snapshot_id = -1; // unknown
	}
}
