package com.online.parser.DRAIN;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;

import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class LogParser {
	String log_format; 
	String path="./";
	String savePath="./result/";
	int depth=4;
	double st=0.4;
	int maxChild = 100;
	Table df_log;
    ArrayList<String> rex= new ArrayList<String>();
    boolean keep_para= true;
    String logName;
    /**
     *  Attributes
        ----------
            rex : regular expressions used in preprocessing (step1)
            path : the input path stores the input log file name
            depth : depth of all leaf nodes
            st : similarity threshold
            maxChild : max number of children of an internal node
            logName : the name of the input file containing raw log messages
            savePath : the output path stores the file containing structured logs
     * **/
    public LogParser(String log_format, String indir, String outdir, int depth, double st, int maxChild, ArrayList<String> rex, boolean keep_para) {
        this.path = indir;
	    this.depth = depth - 2;
	    this.st = st;
	    this.maxChild = maxChild;
	    this.logName = null;
	    this.savePath = outdir;
	    this.df_log = null;
	    this.log_format = log_format;
	    this.rex = rex;
	    this.keep_para = keep_para;
    }
    @SuppressWarnings("unused")
	private boolean hasNumbers(String s) {
    	for (char ch: s.toCharArray()) {
    		if (Character.isDigit(ch)) {
    			return true;
    		}
    	}
    	return false;
	}
    private Logcluster treeSearch(Node rn, ArrayList<String> seq) {
    	Logcluster retLogClust = null;
    	int seqLen = seq.size();
        if (! rn.childD.containsKey(seqLen+"")) {
        	return retLogClust;
        }
        Node parentn = rn.childD.get(seqLen+"");
        int currentDepth = 1;
        for (String token: seq) {
            if ((currentDepth >= this.depth) || (currentDepth > seqLen)){
            	break;
            }
            if(parentn.childD.containsKey(token)) {
            	parentn = parentn.childD.get(token);
            }
            else if(parentn.childD.containsKey("<*>")) {
            	parentn = parentn.childD.get("<*>");
            }
            else {
            	return retLogClust;
            }
            currentDepth += 1;
        }
         ArrayList<Logcluster>logClustL = parentn.childL;
        		
        retLogClust = fastMatch(logClustL, seq);

        return retLogClust;
}
    // seq1 is template
    private void addSeqToPrefixTree(Node rn, Logcluster logClust) {
    	String seqLen = logClust.logTemplate.size()+"";
    	Node firtLayerNode;
    	if (! rn.childD.containsKey(seqLen+"")) {
    		firtLayerNode = new Node(1, seqLen+"");
    		rn.childD.put(seqLen+"", firtLayerNode);
    	}
    	else {
    		firtLayerNode = rn.childD.get(seqLen);
    	}
    	Node parentn = firtLayerNode;
    	int currentDepth = 1;
    	for (String token: logClust.logTemplate) {
    		//Add current log cluster to the leaf node
            if (currentDepth >= depth || currentDepth > Integer.parseInt(seqLen)) {
                if (parentn.childL.size() == 0) {
                	ArrayList<Logcluster> clustL = new ArrayList<>();
                	clustL.add(logClust);
                	parentn.childL = clustL;
                	
                }
                else {
                	parentn.childL.add(logClust);
                }
                break;
            }
            // If token not matched in this layer of existing tree.
            if (!parentn.childD.containsKey(token)){
                if (! hasNumbers(token)) {
                	if (parentn.childD.containsKey("<*>")) {
                        if (parentn.childD.size() < maxChild) {
                            Node newNode = new Node(currentDepth + 1, token);
                            parentn.childD.put(token, newNode);
                            parentn = newNode;
                        }
                        else {
                            parentn = parentn.childD.get("<*>");
                		}
                	}
                	else {
                        if ((parentn.childD.size())+1 < maxChild) {
                            Node newNode = new Node(currentDepth+1, token);
                            parentn.childD.put(token, newNode);
                            parentn = newNode;
                        }
                        else if((parentn.childD.size())+1 == maxChild) {
                        	Node newNode = new Node(currentDepth+1, "<*>");
                        	parentn.childD.put("<*>", newNode);
                        	parentn = newNode;
                        }
                        else {
                        	parentn = parentn.childD.get("<*>");
                        }

                	}
                	
                }
                else {
                	if (! parentn.childD.containsKey("<*>")){
                		Node newNode = new Node(currentDepth+1, "<*>");
                		parentn.childD.put("<*>", newNode);
                    	parentn = newNode;
                	}
                	else {
                		parentn = parentn.childD.get("<*>");
                	}
                }
            }
            else {
            	parentn = parentn.childD.get(token);
            }
            currentDepth++;
            
    	}
//    	rn.childD.put(seqLen+"", parentn);
    	
    	serialize_tree(rn);
    }
    public float[] seqDist(ArrayList<String> seq1, ArrayList<String> seq2) {
        if (seq1.size() == seq2.size()) {
            int simTokens = 0;
            int numOfPar = 0;
            
            for (int i=0;i<seq1.size(); i++) {
            	if (seq1.get(i).equals("<*>")) {
            		numOfPar ++;
            		continue;
            	}
            	if (seq1.get(i).equals(seq2.get(i))) {
            		simTokens ++;
            	}
            }
            
           float retVal = (simTokens*1.0f)/(seq1.size()*1.0f);
           float ret[] = {retVal, (numOfPar*1.0f)};
           return ret;
        }
        else {
        	return null;
        }

    }
    public Logcluster fastMatch(ArrayList<Logcluster> logClustL, ArrayList<String> seq) {
        Logcluster retLogClust = null;

        float maxSim = -1f;
        float maxNumOfPara = -1;
        Logcluster maxClust = null;
        for (Logcluster logClust: logClustL) {
        	float ret [] = seqDist(logClust.logTemplate, seq);
            float curSim = ret[0];
    		float curNumOfPara = ret[1];
            if(curSim>maxSim || (curSim==maxSim && curNumOfPara>maxNumOfPara)) {
                maxSim = curSim;
                maxNumOfPara = curNumOfPara;
                maxClust = logClust;
            }
        }
        if (maxSim >= this.st)
            retLogClust = maxClust;  

        return retLogClust;

    }
    public ArrayList<String> getTemplate(ArrayList<String> seq1, ArrayList<String> seq2){
        assert seq1.size() == seq2.size();
        ArrayList<String>retVal = new ArrayList<String>();
        int i = 0;
        for (String word: seq1) {
            if (word.equals(seq2.get(i)))
                retVal.add(word);
            else
                retVal.add("<*>");

            i += 1;
        }
        return retVal;
    }
    public void outputResult(ArrayList<Logcluster> logClustL) {
    	String log_templates[] = new String[df_log.rowCount()];
    	String log_templateids[] = new String[df_log.rowCount()];
    	String occurence_lst[] = new String[logClustL.size()];
    	String template_id_lst[] = new String[logClustL.size()];
    	String template_str_lst[] = new String[logClustL.size()];
    	Table df_event = Table.create("df_event");
    	int i=0;
    	for (Logcluster logClust :logClustL) {
    		String template_str = String.join(" ", logClust.logTemplate);
    		int occurrence = logClust.logIDL.size();
    		HashFunction hashFunction = Hashing.md5();
    		HashCode hash = hashFunction.hashString(template_str, StandardCharsets.UTF_8);
    		String template_id = hash.toString().substring(0, 8);
            for (int logID : logClust.logIDL) {
                logID -= 1;
                log_templates[logID] = template_str;
                log_templateids[logID] = template_id;
            }            
            occurence_lst[i] = occurrence+"";
            template_id_lst[i] = template_id;
            template_str_lst[i] = template_str;
            i++;
    	}
    	
    	StringColumn template_ids = StringColumn.create("Event_id", template_id_lst);
        StringColumn template_Str = StringColumn.create("Event_template_str", template_str_lst);
        StringColumn occurences_col = StringColumn.create("occurrence", occurence_lst);
        df_event.addColumns(template_ids);
        df_event.addColumns(template_Str);
        df_event.addColumns(occurences_col);
        
        StringColumn event_id_col = StringColumn.create("EventId",log_templateids);
        StringColumn log_id_col = StringColumn.create("EventTemplate",  log_templates);
        
        df_log.addColumns(event_id_col);
        df_log.addColumns(log_id_col);
        
        if (keep_para) {
        	StringColumn parameterList = StringColumn.create("ParameterList");
        	for (Row row: df_log) {
        		String parameter_json = get_parameter_list(row);
        		parameterList.append(parameter_json);
        	}
        	df_log.addColumns(parameterList);
        }
        try {
			df_log.write().csv(savePath+File.pathSeparator+logName +" _structured.csv");
			df_event.write().csv(savePath+File.pathSeparator+logName + "_templates.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        

    }
    public void printTree(Node node, int dep) {}
    public void parse(String logName) {
    	System.out.println("Parsing file: "+logName);
    	Instant start_time = Instant.now();
    	this.logName = logName;
    	Node rootNode = new Node(new HashMap<>(), new HashMap<>(), 0, null);
    	ArrayList<Logcluster> logCluL = new ArrayList<Logcluster>();
    	File ser_tree = new File("./serialized/tree.tmp");
    	File ser_logClus = new File("./serialized/logCluL.tmp"); 
    	if (ser_tree.exists() && ser_logClus.exists()) {
    		rootNode = deserialize_tree();
    		logCluL = deserialize_logCluL();
    		System.out.println("Used serialized data");
    	}
    	
    	load_data();
    	int count = 0;
    	for(Row row: df_log) {
    		int logID = row.getInt("LineId");
    		String logmessage_arr[] = preprocess(row.getString("Content")).trim().split("\\s+");
    		ArrayList<String> logmessageL = new ArrayList(Arrays.asList(logmessage_arr));
    		logmessageL.removeIf(s -> s.equals(""));
    		Logcluster matchCluster = treeSearch(rootNode, logmessageL);
    		
    		if (matchCluster == null){
    			ArrayList<Integer> logIDL = new ArrayList<Integer>();
    			logIDL.add(logID);
    			Logcluster newCluster = new Logcluster(logmessageL, logIDL);
                logCluL.add(newCluster);
                addSeqToPrefixTree(rootNode, newCluster);
                serialize_logCluL(logCluL);
//                json_serialize(rootNode);
                //cluster match was not found to serialize tree here
    		}
    		
    		// Add the new log message to the existing cluster
            else {
                ArrayList<String> newTemplate = getTemplate(logmessageL, matchCluster.logTemplate);
                matchCluster.logIDL.add(logID);
                if (!String.join(" ", newTemplate).equals(String.join(" ", matchCluster.logTemplate))) {
                	matchCluster.logTemplate = newTemplate;
                }
            }
    		count ++;
    		if (count % 1000 == 0 || count == df_log.rowCount()) {
    			System.out.println("Processed "+(count * 100.0 / df_log.rowCount())+"% of log lines.");
    		}

    	}
    	File directory = new File(savePath);
    	if (!directory.exists()) {
    		directory.mkdir();
    	}
    	outputResult(logCluL);
    	Instant finish = Instant.now();
    	long timeElapsed = Duration.between(start_time, finish).toMillis();
    	System.out.println("Parsing done finished in "+ timeElapsed+"" + "mili seconds");
    }
    private void json_serialize(Node rootNode) {
    	Gson gson = new Gson();
    	String serialized = gson.toJson(rootNode);
    	System.out.println(serialized);
    	try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("./serialized/tree.json"));
			writer.write(serialized);
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    private Node json_deserialize() {
    	Gson gson = new Gson();
    	String serialized = "";
    	File file = new File("./serialized/tree.json");
    	try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String data = br.readLine();
			while(data != null) {
				serialized += data;
				data = br.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	Node tree = gson.fromJson(serialized, Node.class);
    	return tree;
    }
    private void serialize_tree(Node rootNode) {
		// TODO Auto-generated method stub
    	/* Create a file to write the serialized tree to. */
    	File directory = new File("./serialized/");
    	if (!directory.exists()) {
    		directory.mkdir();
    	}
        try {
        	System.out.println("serializing tree");
        	FileOutputStream ostream = new FileOutputStream("./serialized/tree.tmp");
            /* Create the output stream */
            ObjectOutputStream p = new ObjectOutputStream(ostream);
			p.writeObject(rootNode);
	        p.flush();
	        ostream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // Write the tree to the stream.
    }
    private void serialize_logCluL(ArrayList<Logcluster> logCluL) {

    	File directory = new File("./serialized/");
    	if (!directory.exists()) {
    		directory.mkdir();
    	}
        try {
        	System.out.println("serializing tree");
        	FileOutputStream ostream = new FileOutputStream("./serialized/logCluL.tmp");
            /* Create the output stream */
            ObjectOutputStream p = new ObjectOutputStream(ostream);
			p.writeObject(logCluL);
	        p.flush();
	        ostream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    private ArrayList<Logcluster> deserialize_logCluL(){
    	File directory = new File("./serialized/");
    	if (directory.exists()) {
        	//returns root node if a serialized tree found else null
        	try {
        		FileInputStream istream = new FileInputStream("./serialized/logCluL.tmp");
                ObjectInputStream ois = new ObjectInputStream(istream);
                
                /* Read a tree object, and all the subtrees */
                ArrayList<Logcluster> logCluL =  (ArrayList<Logcluster>) ois.readObject();
                ois.close();
                istream.close();
                return logCluL;
        	}
        	catch(IOException e) {
        		e.printStackTrace();
        	} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}

    	}
    	return null;
    }
    private Node deserialize_tree() {
    	File directory = new File("./serialized/");
    	if (directory.exists()) {
        	//returns root node if a serialized tree found else null
        	try {
        		FileInputStream istream = new FileInputStream("./serialized/tree.tmp");
                ObjectInputStream ois = new ObjectInputStream(istream);
                
                /* Read a tree object, and all the subtrees */
                Node new_tree = (Node) ois.readObject();
                ois.close();
                istream.close();
                return new_tree;
        	}
        	catch(IOException e) {
        		e.printStackTrace();
        	} catch (ClassNotFoundException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}

    	}
    	return null;
    	
    }
	public void load_data() {
    	ArrayList<Object> logform_reg  = generate_logformat_regex(this.log_format);
    	ArrayList<String> headers = (ArrayList)logform_reg.get(0);
    	Pattern regex = (Pattern)logform_reg.get(1);
        this.df_log = log_to_dataframe(this.path+File.separator+this.logName, regex, headers, this.log_format);

    	
    	
    }
    public String preprocess(String line) {
        for (String currentRex : rex) {
        	
        	line = line.replaceAll(currentRex, "<*>");
        }
        return line;
    }
    public Table log_to_dataframe(String path, Pattern regex, ArrayList<String> headers, String log_format) {
    	ArrayList<Object> log_messages = new ArrayList<>();
    	int linecount = 0;
    	BufferedReader br;
    	try {
    		br= new BufferedReader(new FileReader(path));
    		String line = br.readLine();
    		while(line != null) {
    			Matcher match = regex.matcher(line.trim());
    			
                //match = regex.search(line.trim());
    			ArrayList<String> message = new ArrayList<String>();
    			while(match.find()) {
        			for (String header:headers){
        				message.add(match.group(header));
        			}
                    log_messages.add(message);
                    linecount += 1;
    			}
    			line = br.readLine();

           }
    		Table logdf = Table.create("logdf");
    		for (int i=0; i<headers.size();i++) {
    			ArrayList<String> col = new ArrayList<String>();
    			for(int j=0; j<linecount; j++) {
    				@SuppressWarnings("unchecked")
					ArrayList<String> log_col = (ArrayList<String>) log_messages.get(j);
    				String val = log_col.get(i);
    				col.add(val);
    			}
    			StringColumn column = StringColumn.create(headers.get(i), col);
    			logdf.addColumns(column);
    		}
    		int line_num [] = new int[linecount];
    		for(int i=0; i<linecount;i++) {
    			line_num[i] = i+1;
    		}
    		IntColumn line_col = IntColumn.create("LineId", line_num);
    		logdf.addColumns(line_col);
    		br.close();
    		return logdf;
    		
    	}
    	catch(IOException ioe) {
    		ioe.printStackTrace();
    	}

    	
    	return null;
    }
    public ArrayList<Object> generate_logformat_regex(String log_format) {
    	ArrayList<String> headers = new ArrayList<String>();
    	Pattern pt = Pattern.compile("(<[^<>]+>)");
    	Matcher mt = pt.matcher(log_format);
    	while(mt.find())
    	{
    	  String header = mt.group(1); //group 0 is always the entire match
    	  // new code
    	  log_format = log_format.replace(header, "<>");
    	  header = header.replace("<", "");
    	  header = header.replace(">", "");
    	  headers.add(header);
    	  //System.out.println(token);
    	}
    	//String splitters[] = log_format.split("(<[^<>]+>)");
    	ArrayList<String> splitters = new ArrayList(Arrays.asList(log_format.split("<>")));
    	if (splitters.size() == 0){
    		splitters.add("");
    		splitters.add("");
    	}
    	else {
    		if (!splitters.get(0).equals("")) {
    			splitters.add(0, "");
    		}
    		if (!splitters.get(splitters.size()-1).equals("")) {
    			splitters.add(splitters.size()-1, "");
    		}
    	}
    	ArrayList<String> splitters_lst = new ArrayList<String>();
    	for(int i=0;i<splitters.size();i++) {
    		splitters_lst.add(splitters.get(i));
    		if(i<headers.size()) {
    			splitters_lst.add(headers.get(i));
    		}
    	}
    	String regex = "";
    	for(int k=0; k<splitters_lst.size(); k++) {
    		if(k%2 == 0) {
    			String splitter = splitters_lst.get(k).replaceAll(" +", "\\\\s+");
    			regex += splitter;
    		}
    		else {
    			String header = splitters_lst.get(k);
    			header = header.replace("<", "");
    			header = header.replace(">", "");
    			// remove P from python regex as named group is done this way in java
    			regex += "(?<"+header+">.*?)";    
    		}
    	}
    	Pattern regex_pattern = Pattern.compile("^" + regex + "$");
    	ArrayList<Object> rets = new ArrayList<Object>();
    	rets.add(headers);
    	rets.add(regex_pattern);
    	return rets;
    }
    public String get_parameter_list(Row row) {
    	String template_regex = row.getString("EventTemplate").replaceAll("<.{1,5}>", "<*>");
    	if (! template_regex.contains("<*>")) {return "[]";}
    	template_regex = template_regex.replaceAll("([^A-Za-z0-9])", "\\\\$1");
    	template_regex=template_regex.replaceAll("\\ +", "\\s+");
    	String str = "\\\\<\\\\\\*\\\\>";
//    	Pattern ptr = Pattern.compile("\\\\<\\\\\*\\\\>");
//    	String see = template_regex.replace("\\<\\*\\>", "(.*?)");
    	template_regex = "^"+template_regex.replace("\\<\\*\\>", "(.*?)")+ "$";
    	JSONArray parameter_list = new JSONArray();
    	
    	Matcher m = Pattern.compile(template_regex).matcher(row.getString("Content"));
    	while(m.find()) {
    		parameter_list.put(m.group());
    	}
    	return parameter_list.toString();
    }

    


}
