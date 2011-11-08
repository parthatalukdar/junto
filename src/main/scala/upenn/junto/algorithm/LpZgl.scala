package upenn.junto.algorithm

import upenn.junto.config.Flags
import upenn.junto.eval.GraphEval
import upenn.junto.graph._
import upenn.junto.util.Constants
import upenn.junto.util.MessagePrinter
import upenn.junto.util.ProbUtil

import java.util.{ArrayList,HashMap,Iterator}

import gnu.trove.list.array.TDoubleArrayList
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.iterator.TObjectDoubleIterator

import scala.collection.JavaConversions._

/**
 * Implementation of the original label propagation algorithm:
 *
 * Xiaojin Zhu and Zoubin Ghahramani.
 * Learning from labeled and unlabeled data with label propagation.
 * Technical Report CMU-CALD-02-107, Carnegie Mellon University, 2002.
 * http://pages.cs.wisc.edu/~jerryzhu/pub/CMU-CALD-02-107.pdf
 *
 */
class LpZgl (g: Graph, mu2: Double, keepTopKLabels: Int) 
extends LabelPropagationAlgorithm(g) {

  def run (maxIter: Int, useBipartiteOptimization: Boolean, 
           verbose: Boolean, resultList: ArrayList[Map[String,Double]]) {
		
    var totalSeedNodes = 0
		
    // -- normalize edge weights
    // -- remove dummy label from injected or estimate labels.
    // -- if seed node, then initialize estimated labels with injected
    for (vName <- g.vertices.keySet.iterator) {
      val v: Vertex = g.vertices.get(vName)
			
      // remove dummy label: after normalization, some of the distributions
      // may not be valid probability distributions, but that is fine as the
      // algorithm doesn't require the scores to be normalized (to start with)
      v.SetInjectedLabelScore(Constants.GetDummyLabel, 0.0)
      ProbUtil.Normalize(v.injectedLabels)
			
      if (v.isSeedNode) {
        totalSeedNodes += 1
        v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](v.injectedLabels))
      } else {
        v.SetEstimatedLabelScore(Constants.GetDummyLabel, 0.0)
        ProbUtil.Normalize(v.estimatedLabels)
      }
    }
		
    if (totalSeedNodes <= 0)
      MessagePrinter.PrintAndDie("No seed nodes!! Total: " + totalSeedNodes)
		
    if (verbose)
      println("after_iteration " + 0 + 
              " objective: " + getGraphObjective +
              " accuracy: " + GraphEval.GetAccuracy(g) +
              " rmse: " + GraphEval.GetRMSE(g) +
              " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
              " mrr_test: " + GraphEval.GetAverageTestMRR(g))

    print("Iteration:");
    for (iter <- 1 to maxIter) {
      print(" " + iter)
			
      val startTime = System.currentTimeMillis

      val newDist = new HashMap[String, TObjectDoubleHashMap[String]]()

      for (vName <- g.vertices.keySet.iterator) {
        val v: Vertex = g.vertices.get(vName)
				
        // if the current node is a seed node, then there is no need
        // to estimate new labels.
        if (!v.isSeedNode) {
          // if not present already, create a new label score map
          // for the current hash map. otherwise, it is an error
          if (!newDist.containsKey(vName))
            newDist.put(vName, new TObjectDoubleHashMap[String])
          else
            MessagePrinter.PrintAndDie("Duplicate node found: " + vName)
					
          // compute weighted neighborhood label distribution
          for (neighName <- v.GetNeighborNames) {
            val neigh: Vertex = g.vertices.get(neighName)
            val mult = neigh.GetNeighborWeight(vName)
            if (mult <= 0)
              MessagePrinter.PrintAndDie("Zero weight edge: " +
                                         neigh.name + " --> " + v.name)
	
            ProbUtil.AddScores(newDist.get(vName),
                               mult * mu2,
                               neigh.estimatedLabels)
          }
	
          // normalize newly estimated label scores
          ProbUtil.Normalize(newDist.get(vName), keepTopKLabels)
        } else {
          newDist.put(v.name, new TObjectDoubleHashMap[String](v.estimatedLabels))
        }
      }

      var deltaLabelDiff = 0.0
			
      // update all vertices with new estimated label scores
      for (vName <- g.vertices.keySet.iterator) {
        val v: Vertex = g.vertices.get(vName)

        // normalize and retain only top scoring labels
        ProbUtil.Normalize(newDist.get(vName), keepTopKLabels)
				
        // if this is a seed node, then clam back the original
        // injected label distribution.
        if (v.isSeedNode) {
          v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](v.injectedLabels))
        } else {
          if (!useBipartiteOptimization) {
            deltaLabelDiff +=
              ProbUtil.GetDifferenceNorm2Squarred(v.estimatedLabels, 1.0,
                                                  newDist.get(vName), 1.0);
            v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
          } else {						
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.estimatedLabels, 1.0,
                                                    newDist.get(vName), 1.0)
              v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
            }
						
            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.estimatedLabels, 1.0,
                                                    newDist.get(vName), 1.0)
              v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
            }
          }
        }
      }
			
      val endTime = System.currentTimeMillis
			
      // clear map
      newDist.clear
			
      val totalNodes = g.vertices.size
      val deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes

      if (verbose) {
        val res = Map(Constants.GetMRRString -> GraphEval.GetAverageTestMRR(g),
                      Constants.GetPrecisionString -> GraphEval.GetAccuracy(g))

        resultList.add(res)

        println("\nafter_iteration " + iter +
                " objective: " + getGraphObjective +
                " accuracy: " + res(Constants.GetPrecisionString) +
                " rmse: " + GraphEval.GetRMSE(g) +
                " time: " + (endTime - startTime) +
                " label_diff_per_node: " + deltaLabelDiffPerNode +
                " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                " mrr_test: " + res(Constants.GetMRRString))
      }
			
    }
		
  }

  def getObjective (v: Vertex): Double = {

    var obj = 0.0
		
    // difference with injected labels
    if (v.isSeedNode)
      obj += ProbUtil.GetDifferenceNorm2Squarred(v.injectedLabels, 1,
                                                 v.estimatedLabels, 1)
    // difference with labels of neighbors
    for (neighbor <- v.GetNeighborNames)
      obj += (v.GetNeighborWeight(neighbor) *
              ProbUtil.GetDifferenceNorm2Squarred(
                v.estimatedLabels, 1,
                g.vertices.get(neighbor).estimatedLabels, 1))
	    
    obj
  }
}
