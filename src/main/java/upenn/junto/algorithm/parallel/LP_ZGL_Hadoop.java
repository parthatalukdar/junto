package upenn.junto.algorithm.parallel;

/**
 * Copyright 2011 Partha Pratim Talukdar
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import upenn.junto.util.*;
import upenn.junto.config.*;

public class LP_ZGL_Hadoop {
	
   private static String _kDelim = "\t";
	
   public static class LP_ZGL_Map extends MapReduceBase 
   			implements Mapper<LongWritable, Text, Text, Text> {
     private Text word = new Text();

     public void map(LongWritable key, Text value,
    		 		 OutputCollector<Text, Text> output,
    		 		 Reporter reporter) throws IOException {
       /////
       // Constructing the vertex from the string representation
       /////
       String line = value.toString();
 
       // id gold_label injected_labels estimated_labels neighbors rw_probabilities 
       String[] fields = line.split(_kDelim);       
       TObjectDoubleHashMap neighbors = CollectionUtil.String2Map(fields[4]);
       
       boolean isSeedNode = fields[2].length() > 0 ? true : false;
       
       // If the current node is a seed node but there is no
       // estimate label information yet, then transfer the seed label
       // to the estimated label distribution. Ideally, this is likely
       // to be used in the map of the very first iteration.
       if (isSeedNode && fields[3].length() == 0) {
    	   fields[3] = fields[2];
       }

       // Send two types of messages:
       //   -- self messages which will store the injection labels and
       //        random walk probabilities.
       //   -- messages to neighbors about current estimated scores
       //        of the node.
       //
       // message to self
       output.collect(new Text(fields[0]), new Text(line));

       // message to neighbors
       TObjectDoubleIterator neighIterator = neighbors.iterator();
       while (neighIterator.hasNext()) {
    	   neighIterator.advance();
    	   
    	   // message (neighbor_node, current_node + DELIM + curr_node_label_scores
    	   output.collect(new Text((String) neighIterator.key()),
    			   		  new Text(fields[0] + _kDelim + fields[3]));
       }
     }
   }
	 	
   public static class LP_ZGL_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	 private static double mu1;
	 private static double mu2;
	 private static int keepTopKLabels;
	 
	 public void configure(JobConf conf) {
		 mu1 = Double.parseDouble(conf.get("mu1"));
		 mu2 = Double.parseDouble(conf.get("mu2"));
		 keepTopKLabels = Integer.parseInt(conf.get("keepTopKLabels"));
	 }
	 
     public void reduce(Text key, Iterator<Text> values,
    		 			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {    	 
       // new scores estimated for the current node
       TObjectDoubleHashMap newEstimatedScores = new TObjectDoubleHashMap();
       
       // set to true only if the message sent to itself is found.
       boolean isSelfMessageFound = false;
       
       String vertexId = key.toString();
       String vertexString = "";
       
       TObjectDoubleHashMap neighbors = null;
       TObjectDoubleHashMap randWalkProbs = null;
       
       HashMap<String, String> neighScores =
    	   				new HashMap<String, String>();
       
       int totalMessagesReceived = 0;
       boolean isSeedNode = false;
       
       // iterate over all the messages received at the node
       while (values.hasNext()) {
    	   ++totalMessagesReceived;

    	   String val = values.next().toString();
    	   String[] fields = val.split(_kDelim);
    	   
    	   // System.out.println("src: " + fields[0] + " dest: " + vertexId +
    	   //		   "MESSAGE>>" + val + "<<");

    	   // self-message check
    	   if (vertexId.equals(fields[0])) {
    		   isSelfMessageFound = true;
    		   vertexString = val;
    		   
    		   // System.out.println("Reduce: " + vertexId + " " + val + " " + fields.length);

    		   TObjectDoubleHashMap injLabels = CollectionUtil.String2Map(fields[2]);
    		   neighbors = CollectionUtil.String2Map(neighbors, fields[4]);
    		   randWalkProbs = CollectionUtil.String2Map(fields[5]);
    		   
    		   if (injLabels.size() > 0) {
    			   isSeedNode = true;

	    		   // add injected labels to the estimated scores.
	    		   ProbUtil.AddScores(newEstimatedScores,
	    				   		   	  mu1, injLabels);
    		   }
    	   } else {
    		   // an empty second field represents that the
    		   // neighbor has no valid label assignment yet.
    		   if (fields.length > 1) {
    			   neighScores.put(fields[0], fields[1]);
    		   }
    	   }
       }

       // terminate if message from self is not received.
       if (!isSelfMessageFound) {
    	   throw new RuntimeException("Self message not received for node " + vertexId);
       }
       
       // Add neighbor label scores to current node's label estimates only if the
       // current node is not a seed node. In case of seed nodes, clamp back the 
       // injected label distribution, which is already done above when processing
       // the self messages
       if (!isSeedNode) {
	       // collect neighbors label distributions and create one single
	       // label distribution
	       TObjectDoubleHashMap weightedNeigLablDist = new TObjectDoubleHashMap();
	       Iterator<String> neighIter = neighScores.keySet().iterator();
	       while (neighIter.hasNext()) {
	    	   String neighName = neighIter.next();
	    	   ProbUtil.AddScores(weightedNeigLablDist, // newEstimatedScores,
	    			    	   		mu2 * neighbors.get(neighName),
	    			    	   		CollectionUtil.String2Map(neighScores.get(neighName)));
	       }
	       ProbUtil.Normalize(weightedNeigLablDist, keepTopKLabels);
	       
	       // now add the collective neighbor label distribution to
	       // the estimate of the current node's labels.
	       ProbUtil.AddScores(newEstimatedScores,
	    		              1.0, weightedNeigLablDist);
       }
       
       // normalize the scores
       ProbUtil.Normalize(newEstimatedScores);
       
       // now reconstruct the vertex representation (with the new estimated scores)
       // so that the output from the current mapper can be used as input in next
       // iteration's mapper.
       String[] vertexFields = vertexString.split(_kDelim);
       
       // replace estimated scores with the new ones.
       String[] newVertexFields = new String[vertexFields.length - 1];
       for (int i = 1; i < vertexFields.length; ++i) {
    	   newVertexFields[i - 1] = vertexFields[i]; 
       }
       newVertexFields[2] = CollectionUtil.Map2String(newEstimatedScores);

       output.collect(key, new Text(CollectionUtil.Join(newVertexFields, _kDelim)));
     }
   }
 	
   public static void main(String[] args) throws Exception {
	 Hashtable config = ConfigReader.read_config(args);
	 
     String baseInputFilePat = Defaults.GetValueOrDie(config, "hdfs_input_pattern");
     String baseOutputFilePat = Defaults.GetValueOrDie(config, "hdfs_output_base");
     int numIterations = Integer.parseInt(Defaults.GetValueOrDie(config, "iters")); 
	   
     String currInputFilePat = baseInputFilePat;
     String currOutputFilePat = "";
     for (int iter = 1; iter <= numIterations; ++iter) {
	     JobConf conf = new JobConf(LP_ZGL_Hadoop.class);
	     conf.setJobName("lp_zgl_hadoop");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(LP_ZGL_Map.class);
	     // conf.setCombinerClass(LP_ZGL_Reduce.class);
	     conf.setReducerClass(LP_ZGL_Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	     
	     // hyperparameters
	     conf.set("mu1", Defaults.GetValueOrDie(config, "mu1"));
	     conf.set("mu2", Defaults.GetValueOrDie(config, "mu2"));
	     conf.set("keepTopKLabels",
	    		  Defaults.GetValueOrDefault((String) config.get("keep_top_k_labels"),
	    				  					 Integer.toString(Integer.MAX_VALUE)));

    	 if (iter > 1) {
    		 // output from last iteration is the input for current iteration
    		 currInputFilePat = currOutputFilePat + "/*";
    	 }
    	 FileInputFormat.setInputPaths(conf, new Path(currInputFilePat));
 
    	 currOutputFilePat = baseOutputFilePat + "_" + iter;
	     FileOutputFormat.setOutputPath(conf, new Path(currOutputFilePat));

    	 JobClient.runJob(conf);
     }
   }
}
