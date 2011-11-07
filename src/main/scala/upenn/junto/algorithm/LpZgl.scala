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
class LpZgl (mu2: Double, keepTopKLabels: Int) extends LabelPropagationAlgorithm {

  def run (g: Graph, maxIter: Int, useBipartiteOptimization: Boolean, 
           verbose: Boolean, resultList: ArrayList[Map[String,Double]]) {
		
    var totalSeedNodes = 0
		
    // -- normalize edge weights
    // -- remove dummy label from injected or estimate labels.
    // -- if seed node, then initialize estimated labels with injected
    for (vName <- g._vertices.keySet.iterator) {
      val v: Vertex = g._vertices.get(vName)
			
      // remove dummy label: after normalization, some of the distributions
      // may not be valid probability distributions, but that is fine as the
      // algorithm doesn't require the scores to be normalized (to start with)
      v.SetInjectedLabelScore(Constants.GetDummyLabel, 0.0)
      ProbUtil.Normalize(v.GetInjectedLabelScores)
			
      if (v.IsSeedNode) {
        totalSeedNodes += 1
        v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](v.GetInjectedLabelScores))
      } else {
        v.SetEstimatedLabelScore(Constants.GetDummyLabel, 0.0)
        ProbUtil.Normalize(v.GetEstimatedLabelScores)
      }
    }
		
    if (totalSeedNodes <= 0)
      MessagePrinter.PrintAndDie("No seed nodes!! Total: " + totalSeedNodes)
		
    if (verbose)
      println("after_iteration " + 0 + 
              " objective: " + getObjective(g) +
              " accuracy: " + GraphEval.GetAccuracy(g) +
              " rmse: " + GraphEval.GetRMSE(g) +
              " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
              " mrr_test: " + GraphEval.GetAverageTestMRR(g))

    print("Iteration:");
    for (iter <- 1 to maxIter) {
      print(" " + iter)
			
      val startTime = System.currentTimeMillis

      val newDist = new HashMap[String, TObjectDoubleHashMap[String]]()

      for (vName <- g._vertices.keySet.iterator) {
        val v: Vertex = g._vertices.get(vName)
				
        // if the current node is a seed node, then there is no need
        // to estimate new labels.
        if (!v.IsSeedNode) {
          // if not present already, create a new label score map
          // for the current hash map. otherwise, it is an error
          if (!newDist.containsKey(vName))
            newDist.put(vName, new TObjectDoubleHashMap[String]())
          else
            MessagePrinter.PrintAndDie("Duplicate node found: " + vName)
					
          // compute weighted neighborhood label distribution
          for (neighName <- v.GetNeighborNames) {
            val neigh: Vertex = g._vertices.get(neighName)
            val mult = neigh.GetNeighborWeight(vName)
            if (mult <= 0)
              MessagePrinter.PrintAndDie("Zero weight edge: " +
                                         neigh.GetName() + " --> " + v.GetName)
	
            ProbUtil.AddScores(newDist.get(vName),
                               mult * mu2,
                               neigh.GetEstimatedLabelScores)
          }
	
          // normalize newly estimated label scores
          ProbUtil.Normalize(newDist.get(vName), keepTopKLabels)
        } else {
          newDist.put(v.GetName, new TObjectDoubleHashMap[String](v.GetEstimatedLabelScores))
        }
      }

      var deltaLabelDiff = 0.0
			
      // update all vertices with new estimated label scores
      for (vName <- g._vertices.keySet.iterator) {
        val v: Vertex = g._vertices.get(vName)

        // normalize and retain only top scoring labels
        ProbUtil.Normalize(newDist.get(vName), keepTopKLabels)
				
        // if this is a seed node, then clam back the original
        // injected label distribution.
        if (v.IsSeedNode) {
          v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](v.GetInjectedLabelScores))
        } else {
          if (!useBipartiteOptimization) {
            deltaLabelDiff +=
              ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0,
                                                  newDist.get(vName), 1.0);
            v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
          } else {						
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0,
                                                    newDist.get(vName), 1.0)
              v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
            }
						
            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores, 1.0,
                                                    newDist.get(vName), 1.0)
              v.SetEstimatedLabelScores(new TObjectDoubleHashMap[String](newDist.get(vName)))
            }
          }
        }
      }
			
      val endTime = System.currentTimeMillis
			
      // clear map
      newDist.clear
			
      val totalNodes = g._vertices.size
      val deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes

      if (verbose) {
        val res = Map(Constants.GetMRRString -> GraphEval.GetAverageTestMRR(g),
                      Constants.GetPrecisionString -> GraphEval.GetAccuracy(g))

        resultList.add(res)

        println("\nafter_iteration " + iter +
                " objective: " + getObjective(g) +
                " accuracy: " + res(Constants.GetPrecisionString) +
                " rmse: " + GraphEval.GetRMSE(g) +
                " time: " + (endTime - startTime) +
                " label_diff_per_node: " + deltaLabelDiffPerNode +
                " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                " mrr_test: " + res(Constants.GetMRRString))
      }
			
    }
		
  }

  def getObjective (g: Graph, v: Vertex): Double = {

    var obj = 0.0
		
    // difference with injected labels
    if (v.IsSeedNode)
      obj += ProbUtil.GetDifferenceNorm2Squarred(v.GetInjectedLabelScores, 1,
                                                 v.GetEstimatedLabelScores, 1)
    // difference with labels of neighbors
    for (neighbor <- v.GetNeighborNames)
      obj += (v.GetNeighborWeight(neighbor) *
              ProbUtil.GetDifferenceNorm2Squarred(
                v.GetEstimatedLabelScores, 1,
                g._vertices.get(neighbor).GetEstimatedLabelScores, 1))
	    
    obj
  }
}
