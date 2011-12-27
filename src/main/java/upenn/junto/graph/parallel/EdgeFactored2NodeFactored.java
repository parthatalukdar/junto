package upenn.junto.graph.parallel;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import upenn.junto.graph.*;
import upenn.junto.util.*;
import upenn.junto.config.*;

public class EdgeFactored2NodeFactored {
  private static String kDelim_ = "\t";
  private static int kMaxNeighorsPerLine_ = 100;
	
  public static void main(String[] args) {
    Hashtable config = ConfigReader.read_config(args);
    Graph g = GraphConfigLoader.apply(config);
		
    // save graph in file
    if (config.containsKey("hadoop_graph_file")) {
      WriteToFile(g, (String) config.get("hadoop_graph_file"));
    }
  }
	
  public static void WriteToFile(Graph g, String outputFile) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
      Iterator<String> vIter = g.vertices().keySet().iterator();			
      while (vIter.hasNext()) {
        String vName = vIter.next();
        Vertex v = g.vertices().get(vName);
				
        // remove dummy label from injected and estimated labels
        v.setGoldLabel(Constants.GetDummyLabel(), 0.0);
        v.SetEstimatedLabelScore(Constants.GetDummyLabel(), 0);
				
        String rwProbStr =
          Constants._kInjProb + " " + v.pinject() + " " +
          Constants._kContProb + " " + v.pcontinue() + " " +					
          Constants._kTermProb + " " + v.pabandon();			
				
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
            bw.write(v.name() + kDelim_ +
                     CollectionUtil.Map2String(v.goldLabels()) + kDelim_ +
                     CollectionUtil.Map2String(v.injectedLabels()) + kDelim_ +
                     CollectionUtil.Map2String(v.estimatedLabels()) + kDelim_ +
                     neighStr.trim() + kDelim_ +
                     rwProbStr + "\n");

            // reset the neighborhood string
            neighStr = "";
          }

          Vertex n = g.vertices().get(neighNames[ni]);
          neighStr += neighNames[ni] + " " +
            v.GetNeighborWeight((String) neighNames[ni]) + " ";
        }
				
        // print out any remaining neighborhood information, plus all other info
        if (neighStr.length() > 0) {
          // output format
          // id gold_label injected_labels estimated_labels neighbors rw_probabilities
          bw.write(v.name() + kDelim_ +
                   CollectionUtil.Map2String(v.goldLabels()) + kDelim_ +
                   CollectionUtil.Map2String(v.injectedLabels()) + kDelim_ +
                   CollectionUtil.Map2String(v.estimatedLabels()) + kDelim_ +
                   neighStr.trim() + kDelim_ +
                   rwProbStr + "\n");
        }
      }
      bw.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
