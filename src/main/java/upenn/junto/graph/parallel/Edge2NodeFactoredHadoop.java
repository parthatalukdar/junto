package upenn.junto.graph.parallel;

import upenn.junto.util.*;
import upenn.junto.graph.Vertex;

import java.io.IOException;
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

public class Edge2NodeFactoredHadoop {
	private static String _kDelim = "\t";
	private static int kMaxNeighorsPerLine_ = 1000;	
	private static double _kBeta = 2.0;
	
   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
     public void map(LongWritable key, Text value,
    		 		 OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       /////
       // Constructing the vertex from the string representation
       /////
       String line = value.toString();
 
       // node1 node2 edge_weight
       String[] fields = line.split(_kDelim);

       // source --> dest
       output.collect(new Text(fields[0]),
    		   		  new Text(fields[1] + _kDelim + fields[2]));
       
	   // dest --> source
       // generate this message only if source and destination
       // are different, as otherwise a similar message has already
       // been generated above.
       if (!fields[0].equals(fields[1])) {
    	   output.collect(new Text(fields[1]),
    			   		  new Text(fields[0] + _kDelim + fields[2]));
       }
     }
   }
	 	
   public static class Reduce extends MapReduceBase 
   				implements Reducer<Text, Text, Text, Text> {
     public void reduce(Text key, Iterator<Text> values,
    		 			OutputCollector<Text, Text> output, Reporter reporter) 
     					throws IOException {
       String vertexId = key.toString();       
       Vertex v = new Vertex(vertexId);

       while (values.hasNext()) {
    	   // neighbor/self edge_weight/inject_score
    	   String val = values.next().toString();    	  
    	   String[] fields = val.split(_kDelim);
    	   
    	   if (vertexId.equals(fields[0])) {
    		   v.SetInjectedLabelScore(fields[0], Double.parseDouble(fields[1]));
    	   } else {
    		   v.AddNeighbor(fields[0], Double.parseDouble(fields[1]));
    	   }
       }
       
       // remove dummy labels
       v.SetInjectedLabelScore(Constants.GetDummyLabel(), 0);
       v.SetEstimatedLabelScore(Constants.GetDummyLabel(), 0);

       // calculate random walk probabilities
       v.CalculateRWProbabilities(_kBeta);
       
       // generate the random walk probability string of the node
       String rwProbStr =
			Constants._kInjProb + " " + v.GetInjectionProbability() + " " +
			Constants._kContProb + " " + v.GetContinuationProbability() + " " +					
			Constants._kTermProb + " " + v.GetTerminationProbability();
       
		// represent neighborhood information as a string
		Object[] neighNames = v.GetNeighborNames();
		String neighStr = "";
		int totalNeighbors = neighNames.length;
		for (int ni = 0; ni < totalNeighbors; ++ni) {
			// if the neighborhood string is already too long, then
			// print it out. It is possible to split the neighborhood
			// information of a node into multiple lines. However, all
			// other fields should be repeated in all the split lines.
			if (neighStr.length() > 0 && (ni % kMaxNeighorsPerLine_ == 0)) {
				// output format
				// id gold_label injected_labels estimated_labels neighbors rw_probabilities
				output.collect(key, 
						new Text(CollectionUtil.Map2String(v.GetGoldLabel()) + _kDelim +
							CollectionUtil.Map2String(v.GetInjectedLabelScores()) + _kDelim +
							CollectionUtil.Map2String(v.GetEstimatedLabelScores()) + _kDelim + 
											 neighStr.trim() + _kDelim +
											 rwProbStr));

				// reset the neighborhood string
				neighStr = "";
			}

			neighStr += neighNames[ni] + " " +
							v.GetNeighborWeight((String) neighNames[ni]) + " ";
		}
		
		// print out any remaining neighborhood information, plus all other info
		if (neighStr.length() > 0) {
			// output format
			// id gold_label injected_labels estimated_labels neighbors rw_probabilities
			output.collect(key, new Text(CollectionUtil.Map2String(v.GetGoldLabel()) + _kDelim +
										 CollectionUtil.Map2String(v.GetInjectedLabelScores()) + _kDelim +
										 CollectionUtil.Map2String(v.GetEstimatedLabelScores()) + _kDelim +
										 neighStr.trim() + _kDelim +
										 rwProbStr));
		}
     }
   }
	
   public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Edge2NodeFactoredHadoop.class);
		conf.setJobName("edge2node_hadoop");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
