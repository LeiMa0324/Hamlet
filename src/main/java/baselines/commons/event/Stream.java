package baselines.commons.event;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Stream {
	
	public HashMap<String,ConcurrentLinkedQueue<Event>> substreams;
					
	public Stream () {		
		substreams = new HashMap<String,ConcurrentLinkedQueue<Event>>();				
	}

	//计算stream中event k的个数
	public int generateRates(int k) {
		int rate = 0;
		for (String ssId : substreams.keySet()) {
			int count = 0;
			for (Event e : substreams.get(ssId)) {
				if (e.type == k) {
					count++;
				}
			}
			if (rate < count) {
				rate = count;
			}
		}
		return rate;
	}

	/**
	 * 计算连续k出现的次数
	 * @param k
	 * @return
	 */
	public int generateConsecutiveRates(int k) {
		int rate = 0;
		OUT:
		for (String ssId : substreams.keySet()) {
			int count = 0;
			int lasttype =0;
			for (Event e : substreams.get(ssId)) {
				//如果count已经计数且当前不为k，跳出循环
				if (count!=0 && e.type!=k){
					rate = count;
					break OUT;
				}
				if (e.type==k){
					count++;
				}
			}
		}
		return rate;
	}
}
