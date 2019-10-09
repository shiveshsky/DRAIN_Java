package com.online.parser.DRAIN;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Node implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3158538684222340831L;
	HashMap<String, Node> childD = new HashMap<>();
	HashMap<String, Logcluster> childC = new HashMap<>();
	ArrayList<Logcluster> childL = new ArrayList<>(); 
	int depth = 0;
	String digitOrtoken = "";
	
	public Node(HashMap<String, Node> childD, HashMap<String, Logcluster> childC, int depth, String digitOrtoken) {
		this.childD = childD;
		this.depth = depth;
		this.digitOrtoken = digitOrtoken; 
		this.childC = childC;
	}
	public Node(int depth, String digitOrtoken) {
		this.depth = depth;
		this.digitOrtoken = digitOrtoken; 
	}
}
