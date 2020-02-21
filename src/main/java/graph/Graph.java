package graph;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
import java.util.List;

import event.*;
import template.*;

public class Graph {
	
	// Events per second 
	public HashMap<String, ArrayList<NodesPerSecond>> all_nodes; // key is event type
	
	// Memory requirement
	public int nodeNumber;
	public int start;
	public int end;
	
	// Counts
	public BigInteger final_count;
	public BigInteger count_for_current_second;
	
	// Last node
	Node last_node;
	
	// Trends
	public ArrayList<EventTrend> trends;
			
	public Graph () {
		all_nodes = new HashMap<String, ArrayList<NodesPerSecond>>();
		nodeNumber = 0;
		start = 0;
		end = 0;
		final_count = new BigInteger("0");
		count_for_current_second = new BigInteger("0");
		last_node = null;	
		trends = new ArrayList<EventTrend>();
	}	
	
//	public Graph getCompleteGraphForPercentage (ConcurrentLinkedQueue<Event> events, SingleQueryTemplate query) {		
//		
//		// int number_of_events_per_window = events.size();
//		int id = 1;
//		int curr_sec = -1;	
//		Event event = events.peek();			
//		while (event != null) {
//			
//			event = events.poll();
//			//System.out.println("--------------" + event.id);
//									
//			// Update the current second and all_nodes
//			if (curr_sec < event.sec) {
//				curr_sec = event.sec;
//				NodesPerSecond nodes_in_new_second = new NodesPerSecond(id++,curr_sec);
//				all_nodes.add(nodes_in_new_second);				
//			}
//			
//			// Create and store a new node
//			Node new_node = new Node(event);
//			NodesPerSecond nodes_in_current_second = all_nodes.get(all_nodes.size()-1);
//			nodes_in_current_second.nodes_per_second.add(new_node);
//			
//			// Compute the number of predecessors
//			int numberOfPredecessors = (nodeNumber * query.getPercentage())/100;
//			nodeNumber++;
//						
//			// Every n'th event marks all previous events as incompatible with all future events
//			/*if (negated_events_per_window > 0) {
//				int n = number_of_events_per_window/(1+negated_events_per_window);
//				if (n > 0 && nodeNumber%n == 0) {
//			
//					//System.out.println("Negated event at " + event.sec);
//					for (NodesPerSecond nodes_per_second : all_nodes) {
//						if (nodes_per_second.second < curr_sec) {
//							nodes_per_second.marked = true;						
//						}
//					}
//				}
//			}*/
//			
//			// Connect this event to all previous compatible events and compute the count of this node
//			for (NodesPerSecond nodes_per_second : all_nodes) {
//				if (nodes_per_second.second < curr_sec && !nodes_per_second.marked && event.actual_count < numberOfPredecessors) {
//					for (Node predecessor : nodes_per_second.nodes_per_second) {																				
//						if (event.actual_count < numberOfPredecessors) {		
//							new_node.connect(predecessor);
//							new_node.count = new_node.count.add(predecessor.count);						
//							event.actual_count++;
//							edgeNumber++;
//							// System.out.println(new_node.event.id + " : " + previous_node.event.id + ", ");
//						} else { break; }
//					}
//				} 
//			}	
//									
//			// Update the final count
//			final_count = final_count.add(new BigInteger(new_node.count+""));
//			//System.out.println(new_node.toString());
//			 
//			event = events.peek();
//		}	
//		return this;
//	}	
//	
//	public ArrayList<Graph> partition (int number_of_graphlets) {
//		
//		ArrayList<Graph> graphlets = new ArrayList<Graph>();
//		
//		int balanced_graphlet_size = nodeNumber/number_of_graphlets;
//		//System.out.println("Balanced partition size: " + balanced_graphlet_size + " Graph size: " + nodeNumber);
//		Graph graphlet = new Graph();
//		graphlet.start = all_nodes.get(0).second;
//		graphlet.end = all_nodes.get(0).second;
//				
//		for (NodesPerSecond nodes : all_nodes) {
//			if (graphlet.nodeNumber + nodes.nodes_per_second.size() <= balanced_graphlet_size) {
//				// Add nodes per second to the current graphlet 
//				graphlet.all_nodes.add(nodes);
//				graphlet.nodeNumber += nodes.nodes_per_second.size();
//				graphlet.end = nodes.second;
//			} else {
//				// Add previous graphlet to the result
//				if (graphlet.nodeNumber > 0) graphlets.add(graphlet);
//				// Create a new graphlet and add nodes per second to it
//				graphlet = new Graph();
//				graphlet.start = nodes.second;
//				graphlet.end = nodes.second;
//				graphlet.all_nodes.add(nodes);
//				graphlet.nodeNumber = nodes.nodes_per_second.size();
//			}
//		}
//		// Add last graphlet to the result
//		if (graphlet.nodeNumber > 0) graphlets.add(graphlet);
//		// Print resulting graphlets
//		//for (Graph g : graphlets) System.out.println("--->" + g.nodeNumber);
//				
//		return graphlets;		
//	}
	
	// UPDATED for SingleQueryTemplate
	public Graph getCompleteGraph (ConcurrentLinkedQueue<Event> events, SingleQueryTemplate query) {
		int id = 1;
		HashMap<String, Integer> curr_sec = new HashMap<String, Integer>();
		
		// set up graph with appropriate event types
		for (String s : query.getTypes()) {
			all_nodes.put(s, new ArrayList<NodesPerSecond>());
			curr_sec.put(s,  -1);
		}
		
		Event event = events.peek();			
		while (event != null) {
			
			event = events.poll();
			String type = event.type+"";
//			System.out.println("--------------" + event.id + " TYPE: " + type);
									
			// Update the current second and all_nodes
			Integer s = curr_sec.get(type);
			if (s == null) { 
				event = events.peek();
				continue; 
			} else if (s < event.sec) {
				curr_sec.put(type,event.sec);
				all_nodes.get(type).add(new NodesPerSecond(id++, event.sec));
			}
			
			// Create a new node
			Node new_node = new Node(event);
			int newEdges = 0;
					
			// Connect this event to all previous compatible events and compute the count of this node
			List<String> predecessorTypes = query.getPredecessors(type);
			for (String prType : predecessorTypes) {
				if (prType.equals("START")) {
					new_node.count = new_node.count.add(new BigInteger("1"));
					newEdges++;
					continue;
				}
				for (NodesPerSecond nodes_per_second : all_nodes.get(prType)) {
					if (nodes_per_second.second < curr_sec.get(type)) {					
						for (Node previous_node : nodes_per_second.nodes_per_second) {
							new_node.count = new_node.count.add(previous_node.count);
							newEdges++;							
//							System.out.println(previous_node.event.id + " , " + new_node.event.id);
			}}}}
			
			// Add new node to graph if it had predecessors
			if (newEdges > 0) {
				NodesPerSecond nodes_in_current_second = all_nodes.get(type).get(all_nodes.get(type).size()-1);
				nodes_in_current_second.nodes_per_second.add(new_node);		
				nodeNumber++;
			
				// Update the final count
				if (query.isEndType(event.type+"")) {
					final_count = final_count.add(new BigInteger(new_node.count+""));
				}
				
//				System.out.println(new_node.toString());
			}

			event = events.peek();
		}
//		System.out.println("Node number: " + nodeNumber);
		return this;
	}
	
	public Graph getCompleteLazyGraph (ConcurrentLinkedQueue<Event> events, SingleQueryTemplate query) {
		int id = 1;
		HashMap<String, Integer> curr_sec = new HashMap<String, Integer>();
		HashMap<String, Integer> lazy_sec = new HashMap<String, Integer>(); // for lazy evaluation
		
		// to keep track of how many events per type stored going forward
		HashMap<String, Integer> num_events = new HashMap<String, Integer>();
		
		// set up graph with appropriate event types
		for (String s : query.getTypes()) {
			all_nodes.put(s, new ArrayList<NodesPerSecond>());
			curr_sec.put(s,  -1);
			lazy_sec.put(s, Integer.MAX_VALUE);
			num_events.put(s, 0);
		}
		
		Event event = events.peek();			
		while (event != null) {
			
			event = events.poll();
			String type = event.type+"";
									
			// Update the current second and all_nodes
			Integer s = curr_sec.get(type);
			if (s == null) { 
				event = events.peek();
				continue; 
			} else if (s < event.sec) {
				curr_sec.put(type,event.sec);
				all_nodes.get(type).add(new NodesPerSecond(id++, event.sec));
			}
			
			// Create a new node
			Node new_node = new Node(event);
			
			// Add new nodes if >0 event of predecessor event type has been added
			List<String> predecessorTypes = query.getPredecessors(type);
			for (String prType : predecessorTypes) {
				if (prType == "START" || num_events.get(prType) > 0) {
					num_events.put(type, num_events.get(type)+1);
					NodesPerSecond nodes_in_current_second = all_nodes.get(type).get(all_nodes.get(type).size()-1);
					nodes_in_current_second.nodes_per_second.add(new_node);
					System.out.println("added " + event.id);
					break;
				}
			}
			event = events.peek();
		}
		
		System.out.println(num_events);
		
		/*** Backwards ***/
		String type = query.getEndType();
		
		NodesPerSecond nodes_in_last_second = all_nodes.get(type).get(all_nodes.get(type).size()-1);
		lazy_sec.put(type, nodes_in_last_second.second);
		for (Node lazy_node : nodes_in_last_second.nodes_per_second) {
			lazy_node.count = lazy_node.count.add(new BigInteger("1"));
			
			// Connect this event to all previous compatible events and compute the count of this node
			nodeNumber++;
			boolean isStart = false;
			List<String> predecessorTypes = query.getPredecessors(type);
			for (String prType : predecessorTypes) {
				if (prType == "START") {
					isStart = true;
					continue;
				}
				for (NodesPerSecond nodes_per_second : all_nodes.get(prType)) {
					if (nodes_per_second.second < lazy_sec.get(type)) {					
						for (Node previous_node : nodes_per_second.nodes_per_second) {
							lazy_node.count = lazy_node.count.add(previous_node.count);						
//										System.out.println(previous_node.event.id + " , " + new_node.event.id);
			}}}}
			if (isStart) {
				final_count = final_count.add(new BigInteger(lazy_node.count+""));
			}
			
		}
		
		return this;
	}
	
	public Graph getCompressedGraph (ConcurrentLinkedQueue<Event> events, SingleQueryTemplate query) {		
		int curr_sec = -1;
		Event event = events.peek();			
		while (event != null) {
				
			event = events.poll();
			
			// Update the current second and intermediate counts
			if (curr_sec < event.sec) {
				curr_sec = event.sec;
				final_count = final_count.add(count_for_current_second);	
				count_for_current_second = new BigInteger("0");
			} 
			BigInteger event_count = new BigInteger("1").add(final_count);
			count_for_current_second = count_for_current_second.add(event_count);				
						
			/*if (event != null && event.getSubstreamid().equals("10_7"))
				System.out.println(event.sec + " : " + event.id + " with count " + event_count);*/	
			
			event = events.peek();
		}
		// Add the count for last second to the final count
		final_count = final_count.add(count_for_current_second);
		return this;
	}
	
	/* Use the template */
//	public Graph getGraph (ConcurrentLinkedQueue<Event> events, Template template) {		
//		
//		int id = 1;
//		int curr_sec = -1;	
//		Event event = events.peek();			
//		while (event != null) {
//			
//			event = events.poll();
////			System.out.println("--------------" + event.id);
//									
//			// If the current second has passed to a new value, create a new NodesPerSecond and add it to all_nodes
//			if (curr_sec < event.sec) {
//				curr_sec = event.sec;
//				NodesPerSecond nodes_in_new_second = new NodesPerSecond(id++,curr_sec);
//				all_nodes.add(nodes_in_new_second);				
//			}
//			
//			// Create and store a new node
//			Node new_node = new Node(event);
//			NodesPerSecond nodes_in_current_second = all_nodes.get(all_nodes.size()-1);
//			nodes_in_current_second.nodes_per_second.add(new_node);		
//			nodeNumber++;			
//					
//			// Connect this event to all previous compatible events and compute the count of this node
//			for (NodesPerSecond nodes_per_second : all_nodes) {
//				if (nodes_per_second.second < curr_sec) {					
//					for (Node previous_node : nodes_per_second.nodes_per_second) {		
//						
//						new_node.count = new_node.count.add(previous_node.count);	
//						new_node.connect(previous_node);
//						edgeNumber++;							
//						//System.out.println(previous_node.event.id + " , " + new_node.event.id);
//			}}}				
//				
//			// Update the final count
//			final_count = final_count.add(new BigInteger(new_node.count+""));
//			
//			//System.out.println(new_node.toString());
//			event = events.peek();
//		}
//		return this;
//	}
}
