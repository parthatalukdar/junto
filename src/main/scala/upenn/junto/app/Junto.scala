package upenn.junto.app

/**
 * Copyright 2011 Partha Talukudar, Jason Baldridge
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

import java.util.{ArrayList,Hashtable}
import gnu.trove.TObjectDoubleHashMap
import upenn.junto.algorithm._
import upenn.junto.graph._
import upenn.junto.util._

object JuntoRunner {

  def apply (algo: String, graph: Graph, maxIters: Int, 
             mu1: Double, mu2: Double, mu3: Double,
             keepTopKLabels: Int, useBipartiteOptimization: Boolean,
             verbose: Boolean, resultList: ArrayList[TObjectDoubleHashMap[String]]) {

    algo match {
      case "adsorption" => {
        MessagePrinter.Print("Using " + algo + " ...\n")
        Adsorption.Run(graph, maxIters, "original", mu1, mu2, mu3, keepTopKLabels, 
                       useBipartiteOptimization, verbose, resultList)
      }
      case "mad" => {
        MessagePrinter.Print("Using " + algo + " ...\n")
        Adsorption.Run(graph, maxIters, "modified", mu1, mu2, mu3, keepTopKLabels, 
                       useBipartiteOptimization, verbose, resultList)
      }
      case "lp_zgl" => {
        MessagePrinter.Print("Using Label Propagation (ZGL) ...\n")
        LP_ZGL.Run(graph, maxIters, mu2, keepTopKLabels, 
                   useBipartiteOptimization, verbose, resultList)
      }
      case _ => MessagePrinter.PrintAndDie("Unknown algorithm: " + algo)
    }

  }

}

object JuntoConfigRunner {

  def apply (config: Hashtable[String,String], 
             resultList: ArrayList[TObjectDoubleHashMap[String]]) = {

    // pretty print the configs
    println(CollectionUtil.Map2StringPrettyPrint(config))
		
    // load the graph
    val g = GraphConfigLoader(config)

    val maxIters = Integer.parseInt(config.get("iters"))

    var verbose = true
    if (config.containsKey("verbose"))
      verbose = config.get("verbose").toBoolean

    val mu1 = Defaults.GetValueOrDefault(config.get("mu1"), 1.0)
    val mu2 = Defaults.GetValueOrDefault(config.get("mu2"), 1.0)
    val mu3 = Defaults.GetValueOrDefault(config.get("mu3"), 1.0)
    val keepTopKLabels =
      Defaults.GetValueOrDefault(config.get("keep_top_k_labels"), Integer.MAX_VALUE)
    MessagePrinter.Print("Using keep_top_k_labels value: " + keepTopKLabels)
		
    // this flag should be set to false (the default), unless you really
    // know what you are doing
    val useBipartiteOptimization =
      Defaults.GetValueOrDefault(config.get("use_bipartite_optimization"), false)
		
    // decide on the algorithm to use
    val algo = Defaults.GetValueOrDefault(config.get("algo"), "adsorption")

    JuntoRunner(algo, g, maxIters, mu1, mu2, mu3, keepTopKLabels,
                useBipartiteOptimization, verbose, resultList)
		
    if (config.containsKey("output_file") && (config.get("output_file")).length > 0)
      g.SaveEstimatedScores(config.get("output_file"))
    else if (config.containsKey("output_base") && (config.get("output_base")).length > 0)
      g.SaveEstimatedScores(config.get("output_base") + ".mu2_" + mu2 + ".mu3_" + mu3)

  }

  def main (args: Array[String]) = 
    apply(ConfigReader.read_config(args), new ArrayList[TObjectDoubleHashMap[String]])

}



object GraphConfigLoader { 

  def apply (config: Hashtable[String, String]): Graph = {
    val graph = new Graph
		
    println("Going to build graph ...")

    val dataFormat = Defaults.GetValueOrDefault(config.get("data_format"), "edge_factored")

    // this is used mostly for the data received from Mark, i.e.
    // for information extraction machine learning work.
    val maxSeedsPerClass = 
      Defaults.GetValueOrDefault(config.get("max_seeds_per_class"), Integer.MAX_VALUE)
		
    val beta = Defaults.GetValueOrDefault(config.get("beta"), 2.0)
    val isDirected = Defaults.GetValueOrDefault(config.get("is_directed"), false)

    dataFormat match {
      // edge factored representation is mostly used for the
      // information integration work.
      case "edge_factored" => {
        BuildGraphFromEdgeFactoredData.Build(graph,
                                             config.get("graph_file"),
                                             config.get("seed_file"),
                                             maxSeedsPerClass,
                                             config.get("test_file"),           /* can be null */
                                             config.get("source_freq_file"),    /* can be null */
                                             config.get("target_filter_file"),  /* can be null */
                                             config.get("prune_threshold"),
                                             beta,
                                             isDirected)
			
        // gold labels for some or all of the nodes 
        if (config.containsKey("gold_labels_file"))
          graph.SetGoldLabels(config.get("gold_labels_file"))
      }
      case "node_factored" => {
        BuildGraphFromNodeFactoredData.Build(graph,
                                             config.get("graph_file"),
                                             config.get("seed_file"),
                                             maxSeedsPerClass,
                                             config.get("test_file"),
                                             beta,
                                             isDirected)
      }
      case _ => throw new RuntimeException("Data format " + dataFormat + " not recognized.")
    }
		
    // set Gaussian Kernel weights, if requested. In this case, we assume that existing
    // edge weights are distance squared i.e. || x_i - x_j ||^2  
    val setGaussianWeights = 
      Defaults.GetValueOrDefault(config.get("set_gaussian_kernel_weights"), false)

    if (setGaussianWeights) {
      MessagePrinter.Print("Going to set Gaussian Kernel weights ...");
      val sigmaFactor = Defaults.GetValueOrDie(config, "gauss_sigma_factor").toDouble
      graph.SetGaussianWeights(sigmaFactor)
    }
		
    // keep only top K neighbors: kNN, if requested
    if (config.containsKey("top_k_neighbors")) {
      val maxNeighbors = 
        Defaults.GetValueOrDefault(config.get("top_k_neighbors"), Integer.MAX_VALUE)
      graph.KeepTopKNeighbors(maxNeighbors)
    }
		
    // check whether random train and test splits are to be generated
    if (config.containsKey("train_fract")) {
      val trainFraction = Defaults.GetValueOrDie(config, "train_fract").toDouble
      CrossValidationGenerator.Split(graph, trainFraction)
      graph.SetSeedInjected()
    }
		
    MessagePrinter.Print("Seed injected: " + graph.IsSeedInjected)
    if (graph.IsSeedInjected) {
      // remove seed labels which are not present in any of the test nodes
      // graph.RemoveTrainOnlyLabels
		
      // check whether the seed information is consistent
      // graph.CheckAndStoreSeedLabelInformation(maxSeedsPerClass)
		
      // calculate random walk probabilities.
      // random walk probability computation depends on the seed label information,
      // and hence this can be done only after the seed labels have been injected.
      graph.CalculateRandomWalkProbabilities(beta)
    }

    // print out graph statistics
    MessagePrinter.Print(GraphStats.PrintStats(graph))
		
    // save graph in file, if requested
    if (config.containsKey("graph_output_file")) {
      // graph.WriteToFile(config.get("graph_output_file"))
      graph.WriteToFileWithAlphabet(config.get("graph_output_file"))
    }

    graph
  }

}
