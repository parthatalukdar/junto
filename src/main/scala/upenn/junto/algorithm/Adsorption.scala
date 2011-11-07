package upenn.junto.algorithm

import upenn.junto.config.Flags
import upenn.junto.eval.GraphEval
import upenn.junto.graph._
import upenn.junto.util.CollectionUtil
import upenn.junto.util.Constants
import upenn.junto.util.MessagePrinter
import upenn.junto.util.ProbUtil

import java.util.ArrayList
import java.util.HashMap
import java.util.Iterator

import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.iterator.TObjectDoubleIterator

import scala.collection.JavaConversions._

class ModifiedAdsorption (keepTopKLabels: Int, mu1: Double, mu2: Double, mu3: Double)
extends Adsorption (keepTopKLabels, mu1, mu2, mu3) {

  def normalizeScores (vertex: Vertex, graph: Graph, scores: TObjectDoubleHashMap[String]) {
    ProbUtil.DivScores(scores, vertex.GetNormalizationConstant(graph, mu1, mu2, mu3))
  }

  // multiplier for MAD update: (p_v_cont * w_vu + p_u_cont * w_uv) where u is neighbor
  def getMultiplier (vName: String, vertex: Vertex, neighName: String, neighbor: Vertex) =
    (vertex.GetContinuationProbability * vertex.GetNeighborWeight(neighName) +
     neighbor.GetContinuationProbability * neighbor.GetNeighborWeight(vName))

}

class OriginalAdsorption (keepTopKLabels: Int, mu1: Double, mu2: Double, mu3: Double)
extends Adsorption (keepTopKLabels, mu1, mu2, mu3) {

  def normalizeScores (vertex: Vertex, graph: Graph, scores: TObjectDoubleHashMap[String]) {
    ProbUtil.Normalize(scores, keepTopKLabels)
  }

  // multiplier for Adsorption update: p_v_cont * w_uv (where u is neighbor)
  def getMultiplier (vName: String, vertex: Vertex, neighName: String, neighbor: Vertex) =
      vertex.GetContinuationProbability * neighbor.GetNeighborWeight(vName)

  override def normalizeIfNecessary (scores: TObjectDoubleHashMap[String]) { 
    ProbUtil.Normalize(scores) 
  }


}

abstract class Adsorption (keepTopKLabels: Int, mu1: Double, mu2: Double, mu3: Double)
extends LabelPropagationAlgorithm {

  // Normalization is needed only for the original Adsorption
  // algorithm.  After normalization, we have the weighted
  // neighborhood label distribution for the current node.
  def normalizeIfNecessary (scores: TObjectDoubleHashMap[String]) { }

  def normalizeScores (vertex: Vertex, graph: Graph, scores: TObjectDoubleHashMap[String]): Unit

  def getMultiplier (vName: String, vertex: Vertex, neighName: String, neighbor: Vertex): Double

  def run (g: Graph, maxIter: Int, useBipartiteOptimization: Boolean,
           verbose: Boolean, resultList: ArrayList[Map[String,Double]]) {
		
    // -- normalize edge weights
    // -- remove dummy label from injected or estimate labels.
    // -- if seed node, then initialize estimated labels with injected
    for (vName <- g._vertices.keySet.iterator) {
      val v: Vertex = g._vertices.get(vName)
			
      // add a self loop. This will be useful in contributing
      // currently estimated label scores to next round's computation.

      // remove dummy label: after normalization, some of the distributions
      // may not be valid probability distributions, but that is fine as the
      // algorithm doesn't require the scores to be normalized (to start with)
      v.SetInjectedLabelScore(Constants.GetDummyLabel, 0.0)

      if (v.IsSeedNode) {
        val injLabIter = v.GetInjectedLabelScores.iterator
        while (injLabIter.hasNext) {
          injLabIter.advance
          v.SetInjectedLabelScore(injLabIter.key.asInstanceOf[String], injLabIter.value)
        }
        v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](v.GetInjectedLabelScores))
      } else {
        // remove dummy label
        v.SetEstimatedLabelScore(Constants.GetDummyLabel, 0.0);				
      }
    }
		
    if (verbose) { 
      println("after_iteration " + 0 + 
              " objective: " + getObjective(g) +
              " precision: " + GraphEval.GetAccuracy(g) +
              " rmse: " + GraphEval.GetRMSE(g) +
              " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
              " mrr_test: " + GraphEval.GetAverageTestMRR(g))
    }

    print("Iteration:")
    for (iter <- 1 to maxIter) {
      print(" " + iter)
			
      val startTime = System.currentTimeMillis

      val newDist =  new HashMap[String, TObjectDoubleHashMap[String]]

      for (vName <- g._vertices.keySet.iterator) {
			
        val v: Vertex = g._vertices.get(vName)

        val vertexNewDist = new TObjectDoubleHashMap[String]
							
        // compute weighted neighborhood label distribution
        for (neighName <- v.GetNeighborNames) {
          val neigh: Vertex = g._vertices.get(neighName)
          val mult = getMultiplier(vName, v, neighName, neigh)

          if (verbose)
            println(v.GetName + " " + v.GetContinuationProbability + " " +
                    v.GetNeighborWeight(neighName) + " " +
                    neigh.GetContinuationProbability + " " + neigh.GetNeighborWeight(vName))

          if (mult <= 0) 
            MessagePrinter.PrintAndDie("Non-positive weighted edge:>>" +
                                       neigh.GetName + "-->" + v.GetName + "<<" + " " + mult)

          ProbUtil.AddScores(vertexNewDist, mult * mu2, neigh.GetEstimatedLabelScores)
        }
				
        if (verbose)
          println("Before norm: " + v.GetName + " " + ProbUtil.GetSum(vertexNewDist))

        normalizeIfNecessary(vertexNewDist)
								
        if (verbose) 
          println("After norm: " + v.GetName + " " + ProbUtil.GetSum(vertexNewDist))
				
        // add injection probability
        ProbUtil.AddScores(vertexNewDist,
                           v.GetInjectionProbability * mu1,
                           v.GetInjectedLabelScores)
	
        if (verbose)
          println(iter + " after_inj " + v.GetName() + " " +
                  ProbUtil.GetSum(vertexNewDist) + 
                  " " + CollectionUtil.Map2String(vertexNewDist) +
                  " mu1: " + mu1)

        // add dummy label distribution
        ProbUtil.AddScores(vertexNewDist,
                           v.GetTerminationProbability * mu3,
                           Constants.GetDummyLabelDist)
				
        if (verbose)
          println(iter + " after_dummy " + v.GetName() + " " +
                  ProbUtil.GetSum(vertexNewDist) + " " +
                  CollectionUtil.Map2String(vertexNewDist) +
                  " injected: " + CollectionUtil.Map2String(v.GetInjectedLabelScores))
				
        // keep only the top scoring k labels, this is particularly useful
        // when a large number of labels are involved.
        if (keepTopKLabels < Integer.MAX_VALUE) {
          ProbUtil.KeepTopScoringKeys(vertexNewDist, keepTopKLabels)
          if (vertexNewDist.size > keepTopKLabels)
            MessagePrinter.PrintAndDie("size mismatch: " +
                                       vertexNewDist.size + " " + keepTopKLabels)
        }
				
        // normalize in case of Adsorption
        normalizeScores(v, g, vertexNewDist)

        // Store the new distribution for later update
        newDist.put(vName, vertexNewDist)
      }			

      var deltaLabelDiff = 0.0
      var totalColumnUpdates = 0
      var totalEntityUpdates = 0
		
      // update all vertices with new estimated label scores
      for (vName <- g._vertices.keySet.iterator) {
        val v: Vertex = g._vertices.get(vName)
        val vertexNewDist = newDist.get(vName)
				
        if (!useBipartiteOptimization) {
          deltaLabelDiff += 
            ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0, 
                                                vertexNewDist, 1.0)
          v.SetEstimatedLabelScores(vertexNewDist)
        } else {
          // update column node labels on odd iterations
          if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
            totalColumnUpdates += 1
            deltaLabelDiff += 
              ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0,
                                                vertexNewDist, 1.0)
            v.SetEstimatedLabelScores(vertexNewDist)
          }
						
          // update entity labels on even iterations
          if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
            totalEntityUpdates += 1 
            deltaLabelDiff += 
              ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0,
                                                vertexNewDist, 1.0)
            v.SetEstimatedLabelScores(vertexNewDist)
          }
        }
      }
			
      val endTime = System.currentTimeMillis
			
      // clear map
      newDist.clear
			
      val totalNodes = g._vertices.size
      val deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes

      val res = Map(Constants.GetMRRString -> GraphEval.GetAverageTestMRR(g),
                    Constants.GetPrecisionString -> GraphEval.GetAccuracy(g))

      resultList.add(res)

      if (verbose)
        println("after_iteration " + iter +
                " objective: " + getObjective(g) +
                " accuracy: " + res(Constants.GetPrecisionString) +
                " rmse: " + GraphEval.GetRMSE(g) +
                " time: " + (endTime - startTime) +
                " label_diff_per_node: " + deltaLabelDiffPerNode +
                " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                " mrr_test: " + res(Constants.GetMRRString) +
                " column_updates: " + totalColumnUpdates +
                " entity_updates: " + totalEntityUpdates + "\n")
			
    }
    println()
		
  }
	
  def getObjective (g: Graph, v: Vertex): Double = {
    var obj = 0.0
		
    // difference with injected labels
    if (v.IsSeedNode)
      obj += (mu1 * v.GetInjectionProbability *
              ProbUtil.GetDifferenceNorm2Squarred(v.GetInjectedLabelScores, 1,
                                                  v.GetEstimatedLabelScores, 1))
		
    // difference with labels of neighbors
    for (neighbor <- v.GetNeighborNames)
      obj += (mu2 * v.GetNeighborWeight(neighbor) *
              ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1,
                                            g._vertices.get(neighbor).GetEstimatedLabelScores, 1))
	    
    // difference with dummy labels
    obj += mu3 * ProbUtil.GetDifferenceNorm2Squarred(
      Constants.GetDummyLabelDist, v.GetTerminationProbability, v.GetEstimatedLabelScores, 1
    )
    
    obj
  }

}
