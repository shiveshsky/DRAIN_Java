package com.online.parser.DRAIN;

import java.io.Serializable;
import java.util.ArrayList;

public class Logcluster implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9108417722230707162L;
	ArrayList<String> logTemplate = new ArrayList<String>();
	ArrayList<Integer> logIDL;
	public Logcluster(ArrayList<String> logTemplate, ArrayList<Integer> logIDL) {
		this.logTemplate = logTemplate;
		if (logIDL == null) {
			logIDL = new ArrayList<Integer>();
		}
		else {
			this.logIDL = logIDL;
		}

	}

}
