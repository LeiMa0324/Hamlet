package graph;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Set;

import event.*;
import template.*;

public class MuseGraph {
	
	// Events per second 
	public HashMap<String, ArrayList<NodesPerSecond>> all_nodes; // key is event type
	
	public int nodeNumber;
	
	// Counts
	public HashMap<Integer, BigInteger> final_count;
	public BigInteger count_for_current_second;
			
	public MuseGraph(int numQueries) {
		all_nodes = new HashMap<String, ArrayList<NodesPerSecond>>();
		nodeNumber = 0;
		final_count = new HashMap<Integer, BigInteger>();
		count_for_current_second = new BigInteger("0");
		
		for (int i=1; i <= numQueries; i++) {
			final_count.put(i, new BigInteger("0"));
		}
	}	
	
	public MuseGraph getCompleteGraph (ConcurrentLinkedQueue<Event> events, MultiQueryTemplate T) {
		int id = 1;
		HashMap<String, Integer> curr_sec = new HashMap<String, Integer>();
		
		// set up graph with appropriate event types
		for (String s : T.getTypes()) {
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
			MuseNode new_node = new MuseNode(event, T.Vertices.get(type).instructions.size());
			int newEdges = 0;
					
			// Connect this event to all previous compatible events and compute the count of this node
			ArrayList<ArrayList<String[]>> instructions = T.Vertices.get(type).instructions;
			for (int i=0; i < instructions.size(); i++) {
			    ArrayList<String[]> predecessorTypes = instructions.get(i);
			    BigInteger node_count = new BigInteger("0");

				for (String[] pr : predecessorTypes) {
					if (pr[0].equals("START")) {
						node_count = node_count.add(new BigInteger("1"));
						newEdges++;
						continue;
					}
					for (NodesPerSecond nodes_per_second : all_nodes.get(pr[0])) {
						if (nodes_per_second.second < curr_sec.get(type)) {					
							for (Node previous_node : nodes_per_second.nodes_per_second) {
								node_count = node_count.add(((MuseNode)previous_node).counts[Integer.parseInt(pr[1])]);
								newEdges++;
							}
						}
					}
				}
				new_node.counts[i] = node_count;
			}
			
			// Add new node to graph if it had predecessors
			if (newEdges > 0) {
				NodesPerSecond nodes_in_current_second = all_nodes.get(type).get(all_nodes.get(type).size()-1);
				nodes_in_current_second.nodes_per_second.add(new_node);
				nodeNumber++;
				// Update the final count
				for (int[] query : T.Vertices.get(event.type+"").ends) {
					final_count.put(query[0], final_count.get(query[0]).add(new_node.counts[query[1]]));
//					System.out.println("Incremented final count for query " + qid + " by " + new_node.counts.get(qid));
				}
//				System.out.println(new_node.toString());
			}

			event = events.peek();
		}
		
//		System.out.println("Node number: " + nodeNumber);
		return this;
	}
}
