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
import org.apache.hadoop.mapred.jobcontrol.Job;

import upenn.junto.util.*;
import upenn.junto.config.*;

public class MADHadoop {
	
  private static String _kDelim = "\t";
	
  public static class MADHadoopMap extends MapReduceBase 
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
      TObjectDoubleHashMap rwProbabilities = CollectionUtil.String2Map(fields[5]);
       
      // If the current node is a seed node but there is no
      // estimate label information yet, then transfer the seed label
      // to the estimated label distribution. Ideally, this is likely
      // to be used in the map of the very first iteration.
      boolean isSeedNode = fields[2].length() > 0 ? true : false;
      if (isSeedNode && fields[3].length() == 0) {
	fields[3] = fields[2];
      }

      // TODO(partha): move messages to ProtocolBuffers
       
      // Send two types of messages:
      //   -- self messages which will store the injection labels and
      //        random walk probabilities.
      //   -- messages to neighbors about current estimated scores
      //        of the node.
      //
      // message to self
      output.collect(new Text(fields[0]), new Text("labels" + _kDelim + line));

      // message to neighbors
      TObjectDoubleIterator neighIterator = neighbors.iterator();
      while (neighIterator.hasNext()) {
	neighIterator.advance();
    	   
	// message (neighbor_node, current_node + DELIM + curr_node_label_scores
	output.collect(new Text((String) neighIterator.key()),
		       new Text("labels" + _kDelim + fields[0] + _kDelim + fields[3]));
    	   
	// message (neighbor_node, curr_node + DELIM + curr_node_edge_weights + DELIM curr_node_cont_prob
	assert(neighbors.containsKey((String) neighIterator.key()));
	output.collect(new Text((String) neighIterator.key()),
		       new Text("edge_info" + _kDelim +
				fields[0] + _kDelim +
				neighbors.get((String) neighIterator.key()) + _kDelim +
				rwProbabilities.get(Constants._kContProb)));
      }
    }
  }
	 	
  public static class MADHadoopReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {	   
    private static double mu1;
    private static double mu2;
    private static double mu3;
    private static int keepTopKLabels;

    public void configure(JobConf conf) {
      mu1 = Double.parseDouble(conf.get("mu1"));
      mu2 = Double.parseDouble(conf.get("mu2"));
      mu3 = Double.parseDouble(conf.get("mu3"));
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
       
      TObjectDoubleHashMap incomingEdgeWeights = new TObjectDoubleHashMap();
      TObjectDoubleHashMap neighborContProb = new TObjectDoubleHashMap();
       
      int totalMessagesReceived = 0;
       
      // iterate over all the messages received at the node
      while (values.hasNext()) {
	++totalMessagesReceived;

	String val = values.next().toString();
	String[] fields = val.split(_kDelim);
    	   
	// first field represents the type of message
	String msgType = fields[0];
    	   
	if (fields[0].equals("labels")) {    		   
	  // self-message check
	  if (vertexId.equals(fields[1])) {
	    isSelfMessageFound = true;
	    vertexString = val;	    		 
	
	    TObjectDoubleHashMap injLabels = CollectionUtil.String2Map(fields[3]);
	    neighbors = CollectionUtil.String2Map(neighbors, fields[5]);
	    randWalkProbs = CollectionUtil.String2Map(fields[6]);
	    		   
	    if (injLabels.size() > 0) {    		   
	      // add injected labels to the estimated scores.
	      ProbUtil.AddScores(newEstimatedScores,
				 mu1 * randWalkProbs.get(Constants._kInjProb),
				 injLabels);
	    }
	  } else {
	    // an empty third field represents that the
	    // neighbor has no valid label assignment yet.
	    if (fields.length > 2) {
	      neighScores.put(fields[1], fields[2]);
	    }
	  }
	} else if (msgType.equals("edge_info")) {
	  // edge_info neigh_vertex incoming_edge_weight cont_prob
	  String neighId = fields[1];
    		   
	  if (!incomingEdgeWeights.contains(neighId)) {
	    incomingEdgeWeights.put(neighId, Double.parseDouble(fields[2]));
	  }
    		   
	  if (!neighborContProb.contains(neighId)) {
	    neighborContProb.put(neighId, Double.parseDouble(fields[3]));
	  }
	} else {
	    throw new RuntimeException("Invalid message: " + val);
	}
      }

      // terminate if message from self is not received.
      if (!isSelfMessageFound) {
          throw new RuntimeException("Self message not received for node " + vertexId);
      }
       
      // collect neighbors' label distributions and create one single
      // label distribution
      TObjectDoubleHashMap weightedNeigLablDist = new TObjectDoubleHashMap();
      Iterator<String> neighIter = neighScores.keySet().iterator();
      while (neighIter.hasNext()) {
	String neighName = neighIter.next();
    	   
	double mult = randWalkProbs.get(Constants._kContProb) * neighbors.get(neighName) +
	  neighborContProb.get(neighName) * incomingEdgeWeights.get(neighName);
     	   
	ProbUtil.AddScores(weightedNeigLablDist, // newEstimatedScores,
			   mu2 * mult,
			   CollectionUtil.String2Map(neighScores.get(neighName)));
      }
       
      // now add the collective neighbor label distribution to
      // the estimate of the current node's labels.
      ProbUtil.AddScores(newEstimatedScores,
			 1.0, weightedNeigLablDist);
       
      // add dummy label scores
      ProbUtil.AddScores(newEstimatedScores,
			 mu3 * randWalkProbs.get(Constants._kTermProb),
			 Constants.GetDummyLabelDist());
       
      if (keepTopKLabels < Integer.MAX_VALUE) {
	ProbUtil.KeepTopScoringKeys(newEstimatedScores, keepTopKLabels);
      }

      ProbUtil.DivScores(newEstimatedScores, 
			 GetNormalizationConstant(neighbors, randWalkProbs,
						  incomingEdgeWeights, neighborContProb,
						  mu1, mu2, mu3));
       
      // now reconstruct the vertex representation (with the new estimated scores)
      // so that the output from the current mapper can be used as input in next
      // iteration's mapper.
      String[] vertexFields = vertexString.split(_kDelim);
       
      // replace estimated scores with the new ones.
      // Skip the first two fields as they contained the message header and
      // vertex id respectively.
      String[] newVertexFields = new String[vertexFields.length - 2];
      for (int i = 2; i < vertexFields.length; ++i) {
	newVertexFields[i - 2] = vertexFields[i]; 
      }
      newVertexFields[2] = CollectionUtil.Map2String(newEstimatedScores);

      output.collect(key, new Text(CollectionUtil.Join(newVertexFields, _kDelim)));
    }
     
    public double GetNormalizationConstant(
					   TObjectDoubleHashMap neighbors,
					   TObjectDoubleHashMap randWalkProbs,
					   TObjectDoubleHashMap incomingEdgeWeights,
					   TObjectDoubleHashMap neighborContProb,
					   double mu1, double mu2, double mu3) {
      double mii = 0;
      double totalNeighWeight = 0;
      TObjectDoubleIterator nIter = neighbors.iterator();
      while (nIter.hasNext()) {
	nIter.advance();
	totalNeighWeight +=
	  randWalkProbs.get(Constants._kContProb) * nIter.value();

	String neighName = (String) nIter.key();
	totalNeighWeight += neighborContProb.get(neighName) *
	  incomingEdgeWeights.get(neighName);
      }
			
      // mu1 x p^{inj} +
      //   0.5 * mu2 x \sum_j (p_{i}^{cont} W_{ij} + p_{j}^{cont} W_{ji}) + 
      //   mu3
      mii = mu1 * randWalkProbs.get(Constants._kInjProb) +
	/*0.5 **/ mu2 * totalNeighWeight +
	mu3;

      return (mii);
    }
  }
 	
  public static void main(String[] args) throws Exception {
    Hashtable config = ConfigReader.read_config(args);  

    String baseInputFilePat = Defaults.GetValueOrDie(config, "hdfs_input_pattern");
    String baseOutputFilePat = Defaults.GetValueOrDie(config, "hdfs_output_base");
    int numIterations = Integer.parseInt(Defaults.GetValueOrDie(config, "iters"));
    int numReducers = Defaults.GetValueOrDefault((String) config.get("num_reducers"), 10);

    String currInputFilePat = baseInputFilePat;
    String currOutputFilePat = "";
    for (int iter = 1; iter <= numIterations; ++iter) {
      JobConf conf = new JobConf(MADHadoop.class);
      conf.setJobName("mad_hadoop");
	
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
	
      conf.setMapperClass(MADHadoopMap.class);
      // conf.setCombinerClass(MADHadoopReduce.class);
      conf.setReducerClass(MADHadoopReduce.class);
      conf.setNumReduceTasks(numReducers);
	
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	     
      // hyperparameters
      conf.set("mu1", Defaults.GetValueOrDie(config, "mu1"));
      conf.set("mu2", Defaults.GetValueOrDie(config, "mu2"));
      conf.set("mu3", Defaults.GetValueOrDie(config, "mu3"));
      conf.set("keepTopKLabels",
	       Defaults.GetValueOrDefault((String) config.get("keep_top_k_labels"),
					  Integer.toString(Integer.MAX_VALUE)));

      if (iter > 1) {
	// output from last iteration is the input for current iteration
	currInputFilePat = currOutputFilePat + "/*";
      }
      FileInputFormat.setInputPaths(conf, new Path(currInputFilePat));
 
      currOutputFilePat = baseOutputFilePat + "_iter_" + iter;
      FileOutputFormat.setOutputPath(conf, new Path(currOutputFilePat));

      JobClient.runJob(conf);
    }
  }
}
