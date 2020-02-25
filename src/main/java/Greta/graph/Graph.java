package Greta.graph;

import Greta.event.*;
import Greta.template.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Graph {
	
	// Events per second 
	public HashMap<Integer, ArrayList<NodesPerSecond>> all_nodes; // key is event type
	
	// Memory requirement
	public int nodeNumber;
	public int start;
	public int end;
	
	// Counts
	public BigInteger final_count;
	public BigInteger count_for_current_second;
	public int weight;
	
	// Last node
	Node last_node;
			
	public Graph () {
		all_nodes = new HashMap<Integer, ArrayList<NodesPerSecond>>();
		nodeNumber = 0;
		start = 0;
		end = 0;
		final_count = new BigInteger("0");
		count_for_current_second = new BigInteger("0");
		weight = 0;
		last_node = null;
	}	
	
	// UPDATED for SingleQueryTemplate
	public Graph getCompleteGraph (ConcurrentLinkedQueue<Event> events, SingleQueryTemplate query) {
		int id = 1;
		HashMap<Integer, Integer> curr_sec = new HashMap<Integer, Integer>();
		
		// set up graph with appropriate event types
		for (Integer s : query.getTypes()) {
			all_nodes.put(s, new ArrayList<NodesPerSecond>());
			curr_sec.put(s,  -1);
		}
		
		Event event = events.peek();			
		while (event != null) {
			event = events.poll();
			Integer type = event.type;
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
			
			// Add weight for event processing
//			weight++;
			
			// Create a new node
			Node new_node = new Node(event);
			int newEdges = 0;
			
			// Connect this event to all previous compatible events and compute the count of this node
			List<Integer> predecessorTypes = query.getPredecessors(type);
			
			for (Integer prType : predecessorTypes) {
				if (prType.equals(-1)) { // START
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
						}
					}
				}
			}
			
			// Add new node to graph if it had predecessors
			if (newEdges > 0) {
				NodesPerSecond nodes_in_current_second = all_nodes.get(type).get(all_nodes.get(type).size()-1);
				nodes_in_current_second.nodes_per_second.add(new_node);		
				nodeNumber++;
			
				// Update the final count
				if (query.isEndType(event.type)) {
					final_count = final_count.add(new BigInteger(new_node.count+""));
				}
//				System.out.println(new_node.toString());
			}

			event = events.peek();
		}
//		System.out.println("Node number: " + nodeNumber);
		return this;
	}
}
