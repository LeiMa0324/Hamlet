package baselines.commons.templates;
import java.math.BigInteger;

/**
 * newSharon event type
 */
public class SharonType {
	public boolean isSTART;
	public boolean isTRIG;
	public SharonType predecessor;
	public int current_second;	//当前时间
	public BigInteger current_second_count;		//当前计数
	public BigInteger previous_second_count;	//上一次的计数
	
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
		current_second = t;
		previous_second_count = current_second_count;	//将当前计数给上一次计数
	}
	
	public void resetTime() {
		current_second = -1;
		current_second_count = new BigInteger("0");
		previous_second_count = new BigInteger("0");
	}
}
