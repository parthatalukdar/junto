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
    val graph = new Graph
		
    println("Going to build graph ...")

    // this is used mostly for the data received from Mark, i.e.
    // for information extraction machine learning work.
    val maxSeedsPerClass = 
      Defaults.GetValueOrDefault(config.get("max_seeds_per_class"), Integer.MAX_VALUE)
		
    val beta = Defaults.GetValueOrDefault(config.get("beta"), 2.0)
    val isDirected = Defaults.GetValueOrDefault(config.get("is_directed"), false)

    GraphBuilder(graph,
                 config.get("graph_file"),
                 config.get("seed_file"),
                 maxSeedsPerClass,
                 config.get("test_file"),           /* can be null */
                 config.get("source_freq_file"),    /* can be null */
                 config.get("prune_threshold"),
                 beta,
                 isDirected)

    // gold labels for some or all of the nodes 
    if (config.containsKey("gold_labels_file"))
      graph.SetGoldLabels(config.get("gold_labels_file"))
		
    // set Gaussian Kernel weights, if requested. In this case, we assume that existing
    // edge weights are distance squared i.e. || x_i - x_j ||^2  
    val setGaussianWeights = 
      Defaults.GetValueOrDefault(config.get("set_gaussian_kernel_weights"), false)

    if (setGaussianWeights) {
      MessagePrinter.Print("Going to set Gaussian Kernel weights ...")
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


object GraphBuilder {

  import io.Source
  import scala.collection.JavaConversions._
  import gnu.trove.TObjectIntHashMap

  def apply (graph: Graph, fileName: String, seedFile: String, maxSeedsPerClass: Int,
             testFile: String, srcFilterFile: String,
             pruneThreshold: String, beta: Double, isDirected: Boolean): Graph = {

    // build the graph itself
    buildMultilineInstance(graph, fileName, srcFilterFile, pruneThreshold, isDirected)

    // Inject seed labels
    if (seedFile != null) {
      injectSeedLabels(graph, seedFile, maxSeedsPerClass, (testFile == null))
      graph.SetSeedInjected
    }
		
    // Mark all test nodes which will be used
    // during evaluation.
    if (testFile != null)
      markTestNodes(graph, testFile)
		
    if (pruneThreshold != null) 
      graph.PruneLowDegreeNodes(Integer.parseInt(pruneThreshold))
		
    // calculate random walk probabilities
    graph.CalculateRandomWalkProbabilities(beta)
		
    graph
  }
  
  def buildMultilineInstance (graph: Graph, filelist: String, 
                              srcFilterFile: String, 
                              pruneThresholdStr: String, isDirected: Boolean) = {
    
    val pruneThreshold = pruneThresholdStr.toInt

    val edges = (filelist split(",") map (EdgeFileReader(_)) toList) flatten

    val srcValAlphabet = {
      if   (srcFilterFile != null) loadValAlphabetFile(srcFilterFile, pruneThreshold)
      else null
    }

    var totalSkipped = 0

    for (edge <- edges) {

      // if source frequency filter is to be applied and the current source
      // node is not present in the map, then skip current edge.
      if (srcFilterFile == null || srcValAlphabet.contains(edge.source)) {
	
	val srcNodeName = {
          if   (srcValAlphabet != null) Integer.toString(srcValAlphabet(edge.source))
          else edge.source
        }
	
        // source -> target
        val dv = graph.AddVertex(srcNodeName, Constants.GetDummyLabel)
        dv.AddNeighbor(edge.target, edge.weight)
	
        // target -> source
        if (!isDirected) {
          val fv = graph.AddVertex(edge.target, Constants.GetDummyLabel)
          fv.AddNeighbor(srcNodeName, edge.weight)
        }
      }
    }
  }

  def injectSeedLabels (graph: Graph, filelist: String, 
                        maxSeedsPerClass: Int, alsoMarkTest: Boolean) = {

    val seeds = (filelist split(",") map (SeedFileReader(_)) toList) flatten

    val currSeedsPerClassCount = new TObjectIntHashMap[String]

    for (seed <- seeds) {

      if (!currSeedsPerClassCount.containsKey(seed.label))
        currSeedsPerClassCount.put(seed.label, 0)

      val vertex = graph._vertices.get(seed.vertex)
      if (vertex != null) {
        // update gold label of the current node
        vertex.SetGoldLabel(seed.label, seed.score)

        if (currSeedsPerClassCount.get(seed.label) < maxSeedsPerClass) {
          // add current label to the node's injected labels if not
          // already present
          if (!vertex.GetInjectedLabelScores.containsKey(seed.label)) {
            vertex.SetInjectedLabelScore(seed.label, seed.score)
            vertex.SetSeedNode
            currSeedsPerClassCount.increment(seed.label)
          }
        } else if (alsoMarkTest) {
          vertex.SetTestNode
        }
      }
    }
  }


  def markTestNodes (graph: Graph, testFile: String) = {
    for (line <- Source fromFile(testFile) getLines) {
      val Array(nodename, seedlabel, scoreStr) = line.trim.split("\t")

      val vertex = graph._vertices.get(nodename)
      assert(vertex != null)
				
      vertex.SetGoldLabel(seedlabel, scoreStr.toDouble)
      vertex.SetTestNode()
    }			
  }

  def loadValAlphabetFile (filename: String, freqThreshold: Int) = {
    val retMap = new scala.collection.mutable.HashMap[String,Int]
    var lineNum = 0
    
    for (line <- Source fromFile(filename) getLines) {
      lineNum += 1
      val Array(valCount, valString) = line.trim split("\t")
      if (valCount.toInt > freqThreshold)
        retMap.put(valString, lineNum)
    }
    retMap
  }

}
