package baselines.newsharon;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import baselines.commons.event.Event;
import baselines.commons.event.Stream;

import baselines.commons.transactions.TransactionMQ;

public class newSharon extends TransactionMQ {
	public Stream stream;
	
	private ArrayList<HashMap<Integer, ArrayList<SharonType>>> subpatterns;
	// list of prefix counters for each flattened query
	// Prefix counter is a hashmap with key event type id, value list of sharontype objects
	
	private BigInteger[] final_counts;
	
	/** 
	 * QUERY INFORMATION
	 * CHANGE CODE BELOW FOR DIFFERENT SHARED EVENT TYPE
	 * **/
	
	// shared sub-Pattern B+

	

	private String C = "5";
	
	// q2: D, E, B+
	private String D = "12";
	private String E = "11";
	
	// q3: F, G, B+
	private String F = "10";
	private String G = "12";
	
	private int numQueries ;
	
	/**
	 * END OF QUERY INFORMATION
	 */
	
	public AtomicInteger memory;
	
	private ArrayList<String> sharedPatterns;
	
	private int subpatternsSize;
	private String sharedEvent;
	
	public newSharon(Stream str, CountDownLatch d, AtomicLong time, AtomicInteger mem, ArrayList<String> queries, String shared) {
		// given one shared Pattern and number of queries
		super(d, time);
		stream = str;
		memory = mem;
		sharedEvent = shared;
		//只support end with b+的
		sharedPatterns = flattenSubpattern(shared + "+");
		numQueries = queries.size();
		

		
		subpatterns = new ArrayList<HashMap<Integer, ArrayList<SharonType>>>();

		int qid = 1;
		for (String q: queries){
			subpatterns.add(generatePrefixCtrs(q.replace(","+shared+"+",""),qid));
			qid++;
		}
		int newqid = 1;

		for (String q:queries){
            subpatterns.add(generatePrefixCtrs(q.replace("+",""),newqid));
            newqid++;
        }

		subpatternsSize = subpatterns.size();
		

	}
	
	public ArrayList<String> flattenSubpattern(String P) {
		// P should look like B+ or 2+
		
		String type = P;
		if (P.endsWith("+")) {
			type = P.substring(0, P.length()-1);
		}

		
		ArrayList<String> repTypes = new ArrayList<String>();
		repTypes.add(type + ",");
		
		if (P.endsWith("+")) {
//			System.out.println("Shared Kleene event type");
			int R = stream.generateRates(Integer.parseInt(type));
			for (int j=1; j<R; j++) {
				repTypes.add(repTypes.get(j-1).concat(type + ","));
			}
		}
		
		// In newSharon, a sub-Pattern of length 1 is not sharable, so we need to remove it from this list.
		// This is definitely questionable. Would the optimizer really decide to share this way???
		repTypes.remove(0);
		
		return repTypes;
	}
	
	public HashMap<Integer, ArrayList<SharonType>> generatePrefixCtrs(String pattern, int qid) {
		HashMap<Integer, ArrayList<SharonType>> prefix_counters = new HashMap<Integer, ArrayList<SharonType>>();
		String[] types = pattern.split(",");
		
		SharonType pred = new SharonType(qid);
		ArrayList<SharonType> startType = new ArrayList<SharonType>();
		startType.add(pred);
		prefix_counters.put(Integer.parseInt(types[0]), startType);
		
		for (int i=1; i<types.length; i++) {
			SharonType nextPred = new SharonType(false, pred, qid);
			if (i == types.length-1) {
				nextPred.isTRIG = true;
			}
			
			int k = Integer.parseInt(types[i]);
			if (prefix_counters.containsKey(k)) {
				ArrayList<SharonType> v = prefix_counters.get(k);
				v.add(nextPred);
				prefix_counters.put(k, v);
			} else {
				ArrayList<SharonType> v = new ArrayList<SharonType>();
				v.add(nextPred);
				prefix_counters.put(k, v);
			}
			pred = nextPred;
		}
		
		return prefix_counters;
	}
	
	// this is for generating prefix counters at runtime
	public HashMap<Integer, ArrayList<SharonType>> generatePrefixCtrs(String pattern, int qid, int ss_index) {
		HashMap<Integer, ArrayList<SharonType>> prefix_counters = new HashMap<Integer, ArrayList<SharonType>>();
		String[] types = pattern.split(",");
		
		SharonType pred = new SharonType(qid);
		pred.snapshot_id = ss_index;
		pred.current_second_count = pred.current_second_count.add(new BigInteger("1"));
		
		ArrayList<SharonType> startType = new ArrayList<SharonType>();
		startType.add(pred);
		prefix_counters.put(Integer.parseInt(types[0]), startType);
		
		for (int i=1; i<types.length; i++) {
			SharonType nextPred = new SharonType(false, pred, qid);
			nextPred.snapshot_id = ss_index;
			if (i == types.length-1) {
				nextPred.isTRIG = true;
			}
			
			int k = Integer.parseInt(types[i]);
			if (prefix_counters.containsKey(k)) {
				ArrayList<SharonType> v = prefix_counters.get(k);
				v.add(nextPred);
				prefix_counters.put(k, v);
			} else {
				ArrayList<SharonType> v = new ArrayList<SharonType>();
				v.add(nextPred);
				prefix_counters.put(k, v);
			}
			pred = nextPred;
		}
		
		return prefix_counters;
	}
	
	public void run () {
		computeResults();			
		done.countDown();
	}
	
	public void computeResults () {
		Set<String> substream_ids = stream.substreams.keySet();
		for (String substream_id : substream_ids) {	
//		String substream_id = "18";
			
			long start =  System.currentTimeMillis();
			
			ConcurrentLinkedQueue<Event> events = stream.substreams.get(substream_id);
			
			final_counts = computeResults(events);
			
			// print final counts
			for (int i=0; i<numQueries; i++) {
				System.out.println("stockQuery id: " + (i+1) + " Substream id: " + substream_id +" with count " + final_counts[i]);
			}
			
			long end =  System.currentTimeMillis();
			long duration = end - start;
			latency.set(latency.get() + duration);	
			
			// clean up for next iteration
			subpatterns.subList(subpatternsSize, subpatterns.size()).clear();
			
			for (int i=0; i<subpatterns.size(); i++) {
				HashMap<Integer, ArrayList<SharonType>> ts = subpatterns.get(i);
				for (Integer tid : ts.keySet()) {
					ArrayList<SharonType> sh_list = ts.get(tid);
					for (SharonType sh : sh_list) {
						sh.resetTime();
					}
				}
			}
		}
	}
	
	public BigInteger[] computeResults (ConcurrentLinkedQueue<Event> events) {

		// Set up subpattern counters (needed to add up flattened patterns)
		ArrayList<BigInteger> counts_per_subpattern = new ArrayList<BigInteger>();
		for (int i=0; i<subpatterns.size(); i++) {
			counts_per_subpattern.add(new BigInteger("0"));
		}
		
		// Set up final counters
		BigInteger[] ret_counts = new BigInteger[numQueries];
		for (int i=0; i<numQueries; i++) {
			ret_counts[i] = new BigInteger("0");
		}
		
		// Set up snapshots
		ArrayList<HashMap<Integer, BigInteger>> snapshots = new ArrayList<HashMap<Integer, BigInteger>>();
		// < query id, intermediate count >
		// needed for START events only
		
		Event event = events.peek();
		
		while (event != null) {
			
//			System.out.println("Subpatterns size is " + subpatterns.size());
			
			event = events.poll();
			Integer type = event.type;
			Integer time = event.sec;
//			System.out.println("TIME: " + time + "--------------" + event.id + " TYPE: " + type);
			boolean snapshot_created = false;
			
			ArrayList<HashMap<Integer, ArrayList<SharonType>>> additionalPrefixCtrs = new ArrayList<HashMap<Integer, ArrayList<SharonType>>>();
			
			for (int i=0; i<subpatterns.size(); i++) {
				
				if (subpatterns.get(i).containsKey(type)) {
					for (SharonType prefix_counter : subpatterns.get(i).get(type)) {
						
						// if necessary, update current second
						if (time > prefix_counter.current_second) {
							prefix_counter.updateTime(time);
						}
						
						// START or UPD event
						if (prefix_counter.isSTART) {
							if (prefix_counter.queryID > -1) {
								// this update only applies to non-shared sub-patterns
								prefix_counter.current_second_count = prefix_counter.current_second_count.add(new BigInteger("1"));
							}
						} else {
							SharonType predecessor = prefix_counter.predecessor;
							if (time > predecessor.current_second) {
								predecessor.updateTime(time);
							}
							
							prefix_counter.current_second_count = prefix_counter.current_second_count.add(predecessor.previous_second_count);
							
							// B event
							if (type == Integer.parseInt(sharedEvent)) {
								if (!snapshot_created) {
									// create a snapshot
									HashMap<Integer, BigInteger> ss = new HashMap<Integer, BigInteger>();
									// < qid, count >
									
									if (time > subpatterns.get(0).get(Integer.parseInt(C)).get(0).current_second) {
										ss.put(0, subpatterns.get(0).get(Integer.parseInt(C)).get(0).current_second_count);
									} else {
										ss.put(0, subpatterns.get(0).get(Integer.parseInt(C)).get(0).previous_second_count);
									}
									
									if (time > subpatterns.get(0).get(Integer.parseInt(C)).get(0).current_second) {
										ss.put(1, subpatterns.get(1).get(Integer.parseInt(E)).get(0).current_second_count);
									} else {
										ss.put(1, subpatterns.get(1).get(Integer.parseInt(E)).get(0).previous_second_count);
									}
									
									if (time > subpatterns.get(0).get(Integer.parseInt(C)).get(0).current_second) {
										ss.put(2, subpatterns.get(2).get(Integer.parseInt(G)).get(0).current_second_count);
									} else {
										ss.put(2, subpatterns.get(2).get(Integer.parseInt(G)).get(0).previous_second_count);
									}
									
//									System.out.println(ss);
									
									snapshots.add(ss);
									
									snapshot_created = true;
									
									// create prefix counters for shared sub-Pattern B+ with this event as the START
									for (String q : sharedPatterns) { 
										additionalPrefixCtrs.add(generatePrefixCtrs(q, -1, snapshots.size()-1));
										counts_per_subpattern.add(new BigInteger("0"));
									}
								}
							}
						}
						
//						System.out.println(prefix_counter.current_second_count);
						
						// TRIG event
						if (prefix_counter.isTRIG) {
							// update subpattern count
							counts_per_subpattern.set(i, prefix_counter.current_second_count);
						}	
					}
				}
			}
			
			subpatterns.addAll(additionalPrefixCtrs);
			
			event = events.peek();
		}
		
		// memory
		for (int i=0; i<subpatterns.size(); i++) {
			for (int k : subpatterns.get(i).keySet()) {
				memory.set(memory.get() + subpatterns.get(i).get(k).size() * 8);
			}
		}
		
		// update final count if the event ends a completed match for shared sub-patterns
		for (int i=6; i<subpatterns.size(); i++) {
			for (int qi=0; qi<numQueries; qi++) {
				int si = subpatterns.get(i).get(Integer.parseInt(sharedEvent)).get(0).snapshot_id;
				
//				System.out.println("--------- " + counts_per_subpattern.get(i));
				
				ret_counts[qi] = ret_counts[qi].add(counts_per_subpattern.get(i).multiply(snapshots.get(si).get(qi)));
			}
//			System.out.println("updated q3 count: " + ret_counts[2]);
		}
		
		// update final count for non-shared patterns
//		System.out.println("counts per substream 5: " + counts_per_subpattern.get(5));
		ret_counts[0] = ret_counts[0].add(counts_per_subpattern.get(3));
		ret_counts[1] = ret_counts[1].add(counts_per_subpattern.get(4));
		ret_counts[2] = ret_counts[2].add(counts_per_subpattern.get(5));
		
//		return counts_per_subpattern;
			
		return ret_counts;
	}
}
