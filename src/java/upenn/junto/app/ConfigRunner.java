package upenn.junto.app;

/**
 * Copyright 2011 Partha Talukdar
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

import upenn.junto.algorithm.*;

import upenn.junto.graph.ConfigReader;
import upenn.junto.graph.Graph;
import upenn.junto.graph.GraphLoader;
import upenn.junto.graph.Vertex;
import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Defaults;
import upenn.junto.util.MessagePrinter;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

/**
 * This is a general class which is shared by various
 * implementations of different label propagation
 * algorithms. Most importantly, this class provides
 * the main method, the starting point.
 */
public class ConfigRunner {
	
  public static Graph Run(Hashtable config, ArrayList resultList) {
    // pretty print the configs
    System.out.println(CollectionUtil.Map2StringPrettyPrint(config));
		
    // load the graph
    Graph g = GraphLoader.LoadGraph(config);

    int maxIters = Integer.parseInt((String) config.get("iters"));
    boolean verbose = true;
    if (config.containsKey("verbose")) {
      verbose = Boolean.parseBoolean((String) config.get("verbose"));
    }

    double mu1 = Defaults.GetValueOrDefault((String) config.get("mu1"), 1.0);
    double mu2 = Defaults.GetValueOrDefault((String) config.get("mu2"), 1.0);
    double mu3 = Defaults.GetValueOrDefault((String) config.get("mu3"), 1.0);
    int keepTopKLabels =
      Defaults.GetValueOrDefault((String) config.get("keep_top_k_labels"), Integer.MAX_VALUE);
    MessagePrinter.Print("Using keep_top_k_labels value: " + keepTopKLabels);
		
    // this flag should be set to false (the default), unless you really
    // know what you are doing
    boolean useBipartiteOptimizaition =
      Defaults.GetValueOrDefault((String) config.get("use_bipartite_optimization"), false);
		
    // decide on the algorithm to use
    String algo = Defaults.GetValueOrDefault((String) config.get("algo"), "adsorption");

    if (algo.equals("adsorption") || algo.equals("mad")) {
      // if the specified algorithm is MAD, then add "modified" as the mode
      // as the implementation is currently set up that way
      if (algo.equals("mad")) { config.put("mode", "modified"); }
      if (algo.equals("adsorption")) { config.put("mode", "original"); }

      MessagePrinter.Print("Using " + algo + " ...\n");
      Adsorption.Run(g,
                     maxIters,
                     (String) config.get("mode"),
                     mu1, mu2, mu3,
                     keepTopKLabels,
                     useBipartiteOptimizaition,
                     verbose,
                     resultList);
    } else if (algo.equals("lp_zgl")) {
      MessagePrinter.Print("Using Label Propagation (ZGL) ...\n");
      LP_ZGL.Run(g,
                 maxIters,
                 mu2,
                 keepTopKLabels,
                 useBipartiteOptimizaition,
                 verbose,
                 resultList);
    } else {
      MessagePrinter.PrintAndDie("Unknown algorithm: " + algo);
    }
		
    if (config.containsKey("output_file") && ((String) config.get("output_file")).length() > 0) {
      g.SaveEstimatedScores((String) config.get("output_file"));
    } else if (config.containsKey("output_base") &&
               ((String) config.get("output_base")).length() > 0) {			
      g.SaveEstimatedScores((String) config.get("output_base") +
                            ".mu2_" + mu2 +
                            ".mu3_" + mu3);
    }

    return(g);
  }

  public static void main(String[] args) {
    Hashtable config = ConfigReader.read_config(args);
    Run(config, new ArrayList());
  }
}
