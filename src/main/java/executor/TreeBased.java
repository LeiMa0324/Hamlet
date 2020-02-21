package executor;

import java.util.ArrayList;
import event.*;
import operator.*;


// a:1,3,4,7 b:2,6,8

public class TreeBased {     
	
	public static void main (String args[]) {
		
		ArrayList<ArrayList<EventSequence>> event_streams = new ArrayList<ArrayList<EventSequence>>();
		
		// Input events
		for (int i=0; i<args.length; i++) {
			ArrayList<EventSequence> events = parse(args[i]);
			event_streams.add(events);
		}		
		ArrayList<EventSequence> as = event_streams.get(0);
		ArrayList<EventSequence> bs = event_streams.get(1);
		
		/*// A,B
		ArrayList<EventSequence> ab = Sequence.generate(as, bs);
		System.out.println("Results of A,B: " + ab.toString() + "\nCount: " + ab.size());*/
		
		// A+
		ArrayList<EventSequence> a_plus = Kleene.generate(as);
		System.out.println("Results of A+: " + a_plus.toString() + "\nCount: " + a_plus.size());
		
		/*// (A,B)+
		ArrayList<EventSequence> ab_plus = Kleene.generate(ab);
		System.out.println("Results of (A,B)+: " + ab_plus.toString() + "\nCount: " + ab_plus.size());*/
		
		// A+,B	
		ArrayList<EventSequence> a_plus_b = Sequence.generate(a_plus, bs);
		System.out.println("Results of A+,B: " + a_plus_b.toString() + "\nCount: " + a_plus_b.size());		
		
		// (A+,B)+
		ArrayList<EventSequence> a_plus_b__plus = Kleene.generate(a_plus_b);
		System.out.println("Results of (A+,B)+: " + a_plus_b__plus.toString() + "\nCount: " + a_plus_b__plus.size());	
		
		/*// A+,B+
		ArrayList<EventSequence> b_plus = Kleene.generate(bs);
		ArrayList<EventSequence> a_plus_b_plus = Sequence.generate(a_plus, b_plus);
		System.out.println("Results of A+,B+: " + a_plus_b_plus.toString() + "\nCount: " + a_plus_b_plus.size());*/
	}
	
	static ArrayList<EventSequence> parse (String input) {
		
		ArrayList<EventSequence> results = new ArrayList<EventSequence>();
		
		String[] split_input  = input.split(":");
		String type = split_input[0];
		String[] numbers = split_input[1].split(",");
		
		for (int i=0; i<numbers.length; i++) {
			int id = Integer.parseInt(numbers[i]);
			int time = Integer.parseInt(numbers[i]);			
			Event e = new RawEvent(id,time,type);
			ArrayList<Event> events = new ArrayList<Event>();
			events.add(e);
			EventSequence c = new EventSequence(events);
			results.add(c);
		}
		System.out.println("Input event stream: " + results.toString());	
		
		return results;
	}
}