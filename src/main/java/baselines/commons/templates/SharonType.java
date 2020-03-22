package baselines.commons.templates;
import java.math.BigInteger;

public class SharonType {
	public boolean isSTART;
	public boolean isTRIG;
	public SharonType predecessor;
	public int current_second;
	public int previous_second;
	public BigInteger current_second_count;
	public BigInteger previous_second_count;
	
	public SharonType() {
		isSTART = true;
		isTRIG = false;
		resetTime();
	}
	
	public SharonType(boolean t, SharonType p) {
		isSTART = false;
		isTRIG = t;
		predecessor = p;
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
	}
}
