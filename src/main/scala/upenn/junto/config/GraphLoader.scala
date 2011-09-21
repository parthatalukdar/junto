package upenn.junto.config

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

object GraphConfigLoader { 

  def apply (config: Hashtable[String, String]): Graph = {
		
    println("Going to build graph ...")

    // this is used mostly for the data received from Mark, i.e.
    // for information extraction machine learning work.
    val maxSeedsPerClass = 
      Defaults.GetValueOrDefault(config.get("max_seeds_per_class"), Integer.MAX_VALUE)
		
    val beta = Defaults.GetValueOrDefault(config.get("beta"), 2.0)
    val isDirected = Defaults.GetValueOrDefault(config.get("is_directed"), false)

    val edgeFilelist = Defaults.GetValueOrDie(config, "graph_file")
    val edges = (edgeFilelist split(",") map (EdgeFileReader(_)) toList) flatten

    val seedFilelist = Defaults.GetValueOrDie(config, "seed_file")
    val seeds = (seedFilelist split(",") map (LabelFileReader(_)) toList) flatten

    val testLabels = {
      if (config.containsKey("test_file")) LabelFileReader(config.get("test_file"))
      else List[Label]()
    }

    val setGaussianWeights = 
      Defaults.GetValueOrDefault(config.get("set_gaussian_kernel_weights"), false)
    val sigmaFactor = Defaults.GetValueOrDefault(config.get("gauss_sigma_factor"), 0.0)
		
    // keep only top K neighbors: kNN, if requested
    val maxNeighbors = Defaults.GetValueOrDefault(config.get("top_k_neighbors"), Integer.MAX_VALUE)

		
    val graph = GraphBuilder(edges, seeds, testLabels,
                             beta, maxNeighbors, maxSeedsPerClass,
                             setGaussianWeights, sigmaFactor,
                             config.get("prune_threshold"), isDirected)

    // gold labels for some or all of the nodes 
    if (config.containsKey("gold_labels_file"))
      graph.SetGoldLabels(config.get("gold_labels_file"))

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


object GraphBuilder {

  import io.Source
  import scala.collection.JavaConversions._
  import gnu.trove.TObjectIntHashMap

  // Create a graph using lots of defaults; no test labels provided
  def apply (edges: List[Edge], seeds: List[Label]): Graph = apply(edges, seeds, List[Label]())

  // Create a graph using lots of defaults
  def apply (edges: List[Edge], seeds: List[Label], testLabels: List[Label]): Graph = 
    apply(edges, seeds, testLabels, 2.0, Integer.MAX_VALUE, Integer.MAX_VALUE, false, 0.0, null, false)

  def apply (edges: List[Edge], seeds: List[Label], testLabels: List[Label], 
             beta: Double, maxNeighbors: Int, maxSeedsPerClass: Int, 
             setGaussianWeights: Boolean, sigmaFactor: Double,
             pruneThreshold: String, isDirected: Boolean) = {

    val graph = new Graph

    // Build the graph from the edges
    var cnt = 0
    for (edge <- edges) {
      cnt += 1
      if (cnt % 1000000 == 0) {
    	  println("Edges Processed: " + cnt);
      }

      // source -> target
      val dv = graph.AddVertex(edge.source, Constants.GetDummyLabel)
      dv.AddNeighbor(edge.target, edge.weight)
      
      // target -> source
      if (!isDirected) {
        val fv = graph.AddVertex(edge.target, Constants.GetDummyLabel)
        fv.AddNeighbor(edge.source, edge.weight)
      }
    }

    // Inject seed labels
    if (seeds.length > 0) {
      graph.SetSeedInjected

      val currSeedsPerClassCount = new TObjectIntHashMap[String]
      for (seed <- seeds) {

        if (!currSeedsPerClassCount.containsKey(seed.label))
          currSeedsPerClassCount.put(seed.label, 0)

        val vertex = graph._vertices.get(seed.vertex)
        if (vertex != null) {
          // update gold label of the current node
          vertex.SetGoldLabel(seed.label, seed.score)

          // add current label to the node's injected labels if not
          // already present
          if (currSeedsPerClassCount.get(seed.label) < maxSeedsPerClass 
              && !vertex.GetInjectedLabelScores.containsKey(seed.label)) {
            vertex.SetInjectedLabelScore(seed.label, seed.score)
            vertex.SetSeedNode
            currSeedsPerClassCount.increment(seed.label)
          }
        }
      }

      // calculate random walk probabilities.
      // random walk probability computation depends on the seed label information,
      // and hence this can be done only after the seed labels have been injected.
      graph.CalculateRandomWalkProbabilities(beta)
    }

    // Mark all test nodes, which will be used during evaluation.
    if (testLabels.length > 0) {
      for (node <- testLabels) {
        val vertex = graph._vertices.get(node.vertex)
        assert(vertex != null)
        vertex.SetGoldLabel(node.label, node.score)
        vertex.SetTestNode
      }			
    }

    // set Gaussian Kernel weights, if requested. In this case, we assume that existing
    // edge weights are distance squared i.e. || x_i - x_j ||^2  
    if (setGaussianWeights)
      graph.SetGaussianWeights(sigmaFactor)

    graph.KeepTopKNeighbors(maxNeighbors)
		
    if (pruneThreshold != null) 
      graph.PruneLowDegreeNodes(pruneThreshold.toInt)
		
    graph
  }
  
}
