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
        EdgeFactoredGraphBuilder(graph,
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


object EdgeFactoredGraphBuilder {

  import io.Source
  import scala.collection.JavaConversions._
  import gnu.trove.TObjectIntHashMap

  def apply (graph: Graph, fileName: String, seedFile: String, maxSeedsPerClass: Int,
             testFile: String, srcFilterFile: String, trgFilterFile: String,
             pruneThreshold: String, beta: Double, isDirected: Boolean): Graph = {

    // build the graph itself
    buildMultilineInstance(graph, fileName, srcFilterFile, 
                           trgFilterFile, pruneThreshold,
                           isDirected)

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
    graph.CalculateRandomWalkProbabilities(beta);
		
    graph
  }
  
  def buildMultilineInstance (graph: Graph, filelist: String, 
                              srcFilterFile: String, trgFilterFile: String,
                              pruneThresholdStr: String, isDirected: Boolean) = {
    
    val pruneThreshold = pruneThresholdStr.toInt

    var totalSkipped = 0
    for (filename <- filelist split(";")) {
      MessagePrinter.Print("Loading from file: " + filename)

      var totalProcessed = 0
      
      val srcValAlphabet = {
        if   (srcFilterFile != null) loadValAlphabetFile(srcFilterFile, pruneThreshold)
        else null
      }

      val trgValFilter = {
        if   (trgFilterFile != null) loadFilterFile(trgFilterFile)
        else null
      }
      
      for (line <- Source fromFile(filename) getLines) {
        totalProcessed += 1

        if (totalProcessed % 100000 == 0)
          println("processed so far: " + totalProcessed)

        // doc feat val
        val Array(srcNodeNameStr, trgNodeName, weightStr) = line.trim split("\t")
	val weight = weightStr.toDouble
        
        // if source frequency filter is to be applied and the current source
        // node is not present in the map, then skip current edge.
        if (srcFilterFile != null && !srcValAlphabet.containsKey(srcNodeNameStr)) {

          totalSkipped += 1

        } else {
				
	  val srcNodeName = {
            if   (srcValAlphabet != null) Integer.toString(srcValAlphabet.get(srcNodeNameStr))
            else srcNodeNameStr
          }
					
          // doc -> feat
          val dv = graph.AddVertex(srcNodeName, Constants.GetDummyLabel)
          dv.AddNeighbor(trgNodeName, weight)
	  
          // feat -> doc
          if (!isDirected) {
            val fv = graph.AddVertex(trgNodeName, Constants.GetDummyLabel)
            fv.AddNeighbor(srcNodeName, weight)
          }
        }
      }
			
      println("Total skipped: " + totalSkipped)
    }
  }

  def injectSeedLabels (graph: Graph, seedFilelist: String, 
                        maxSeedsPerClass: Int, alsoMarkTest: Boolean) = {

    val currSeedsPerClassCount = new TObjectIntHashMap[String]

    for (filename <- seedFilelist split(",")) {
      for (line <- Source fromFile(filename) getLines) {

        // node_idx seed_label score
        val Array(nodename, seedlabel, scoreStr) = line.trim.split("\t")
	val score = scoreStr.toDouble

        if (!currSeedsPerClassCount.containsKey(seedlabel))
          currSeedsPerClassCount.put(seedlabel, 0)

        val vertex = graph._vertices.get(nodename)
        if (vertex != null) {
          // update gold label of the current node
          vertex.SetGoldLabel(seedlabel, score)

          if (currSeedsPerClassCount.get(seedlabel) < maxSeedsPerClass) {
            // add current label to the node's injected labels if not
            // already present
            if (!vertex.GetInjectedLabelScores.containsKey(seedlabel)) {
              vertex.SetInjectedLabelScore(seedlabel, score)
              vertex.SetSeedNode
              currSeedsPerClassCount.increment(seedlabel)
            }
          } else if (alsoMarkTest) {
            vertex.SetTestNode()
          }
        }
      }
    }

    var testNodeCnt = 0
    var seedNodeCnt = 0
    for (vname: String <- graph._vertices.keySet.toSet) {
      val vertex = graph._vertices.get(vname);
      if (vertex.IsTestNode) testNodeCnt += 1
      if (vertex.IsSeedNode) seedNodeCnt += 1
    }                              

    println("Total seeded nodes:: " + seedNodeCnt)
    if (alsoMarkTest)
      println("Total test nodes:: " + testNodeCnt)
  }


  def markTestNodes (graph: Graph, testFile: String) = {
    var cnt = 0
    for (line <- Source fromFile(testFile) getLines) {
      val Array(nodename, seedlabel, scoreStr) = line.trim.split("\t")

      val vertex = graph._vertices.get(nodename)
      assert(vertex != null)
				
      vertex.SetGoldLabel(seedlabel, scoreStr.toDouble)
      vertex.SetTestNode()
      cnt += 1
    }			
    println("Total " + cnt + " nodes marked as test nodes!")
  }

  def loadValAlphabetFile (filename: String, freqThreshold: Int): TObjectIntHashMap[String] = {
    val retMap = new TObjectIntHashMap[String]

    var lineNum = 0
    for (line <- Source fromFile(filename) getLines) {
      lineNum += 1
      val Array(valCount, valString) = line.trim split("\t")
      if (valCount.toInt > freqThreshold)
        retMap.put(valString, lineNum)
    }

    println("Total " + retMap.size + " loaded from " + filename);
    retMap
  }

  def loadFilterFile (filterFile: String) = {
    val retSet = new java.util.HashSet[String]
    for (line <- Source fromFile(filterFile) getLines)
      retSet.add(line)

    System.out.println("Total " + retSet.size + " loaded from " + filterFile)
    retSet
  }


}
