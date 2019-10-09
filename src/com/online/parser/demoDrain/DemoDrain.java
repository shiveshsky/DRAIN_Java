package com.online.parser.demoDrain;

import java.util.ArrayList;

import com.online.parser.DRAIN.LogParser;

public class DemoDrain {

	public static void main(String[] args) {
		String input_dir  = "logs/";  // The input directory of log file
		String	output_dir = "My_Drain_result/";  // The output directory of parsing results
		String log_file   = "various_log_lines.txt";
//		String log_format = "<date>, <level> <info> <Content>";
		String log_format = "<Content>";
		float st = 0.5f;
		int depth = 4;
		int maxChild = 100;
		String currentDirectory = System.getProperty("user.dir");
		
		LogParser lp = new LogParser(log_format, input_dir, output_dir, depth, st, maxChild, new ArrayList<String>(), true);
		lp.parse(log_file);
	}

}
