package upenn.junto.algorithm;

import upenn.junto.config.Flags;
import upenn.junto.eval.GraphEval;
import upenn.junto.graph.*;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;
import upenn.junto.util.ProbUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;

/**
 * Implementation of the original label propagation algorithm:
 *
 * Xiaojin Zhu and Zoubin Ghahramani.
 * Learning from labeled and unlabeled data with label propagation.
 * Technical Report CMU-CALD-02-107, Carnegie Mellon University, 2002.
 * http://pages.cs.wisc.edu/~jerryzhu/pub/CMU-CALD-02-107.pdf
 *
 */
public class LP_ZGL {

  public static void Run(Graph g, int maxIter, double mu2, 
                         int keepTopKLabels, boolean useBipartitieOptimization, boolean verbose,
                         ArrayList resultList) {
		
    int totalSeedNodes = 0;
		
    // -- normalize edge weights
    // -- remove dummy label from injected or estimate labels.
    // -- if seed node, then initialize estimated labels with injected
    Iterator<String> viter1 = g._vertices.keySet().iterator();
    while (viter1.hasNext()) {
      String vName = viter1.next();
      Vertex v = g._vertices.get(vName);
			
      // add a self loop. This will be useful in contributing
      // currently estimated label scores to next round's computation.
      // v.AddNeighbor(vName, 1.0);

      // v.NormalizeTransitionProbability();
			
      // remove dummy label: after normalization, some of the distributions
      // may not be valid probability distributions, but that is fine as the
      // algorithm doesn't require the scores to be normalized (to start with)
      v.SetInjectedLabelScore(Constants.GetDummyLabel(), 0.0);
      ProbUtil.Normalize(v.GetInjectedLabelScores());
			
      if (v.IsSeedNode()) {
        ++totalSeedNodes;
        v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(v.GetInjectedLabelScores()));
      } else {
        v.SetEstimatedLabelScore(Constants.GetDummyLabel(), 0.0);
        ProbUtil.Normalize(v.GetEstimatedLabelScores());
      }
    }
		
    if (totalSeedNodes <= 0) {
      MessagePrinter.PrintAndDie("No seed nodes!! Total: " + totalSeedNodes);
    }
		
    if (verbose) { 
      System.out.println("after_iteration " + 0 + 
                         " objective: " + GetLPObjective(g) +
                         " accuracy: " + GraphEval.GetAccuracy(g) +
                         " rmse: " + GraphEval.GetRMSE(g) +
                         " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                         " mrr_test: " + GraphEval.GetAverageTestMRR(g));
    }

    for (int iter = 1; iter <= maxIter; ++iter) {
      System.out.println("Iteration: " + iter);
			
      long startTime = System.currentTimeMillis();

      HashMap<String, TObjectDoubleHashMap> newDist =
        new HashMap<String, TObjectDoubleHashMap>();
      Iterator<String> viter = g._vertices.keySet().iterator();
			
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex v = g._vertices.get(vName);
				
        // if the current node is a seed node, then there is no need
        // to estimate new labels.
        if (!v.IsSeedNode()) {
          // if not present already, create a new label score map
          // for the current hash map. otherwise, it is an error
          if (!newDist.containsKey(vName)) {
            newDist.put(vName, new TObjectDoubleHashMap());
          } else {
            MessagePrinter.PrintAndDie("Duplicate node found: " + vName);
          }
					
          // compute weighted neighborhood label distribution
          Object[] neighNames = v.GetNeighborNames();
					
          for (int ni = 0; ni < neighNames.length; ++ni) {
            String neighName = (String) neighNames[ni];
            Vertex neigh = g._vertices.get(neighName);
	
            double mult = neigh.GetNeighborWeight(vName);
            if (mult <= 0) {
              MessagePrinter.PrintAndDie("Zero weight edge: " +
                                         neigh.GetName() + " --> " + v.GetName());
            }
	
            ProbUtil.AddScores(newDist.get(vName),
                            mult * mu2,
                            neigh.GetEstimatedLabelScores());
          }
	
          // normalize newly estimated label scores
          ProbUtil.Normalize(newDist.get(vName), keepTopKLabels);
        } else {
          newDist.put(v.GetName(), new TObjectDoubleHashMap<String>(v.GetEstimatedLabelScores()));
        }
      }

      double deltaLabelDiff = 0;
			
      // update all vertices with new estimated label scores
      viter = g._vertices.keySet().iterator();
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex v = g._vertices.get(vName);

        // deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(), 1.0,
        //  												   newDist.get(vName), 1.0);
				
        // normalize and retain only top scoring labels
        ProbUtil.Normalize(newDist.get(vName), keepTopKLabels);
				
        // if this is a seed node, then clam back the original
        // injected label distribution.
        if (v.IsSeedNode()) {
          v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(v.GetInjectedLabelScores()));
        } else {
          if (!useBipartitieOptimization) {
            deltaLabelDiff +=
              ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(), 1.0,
                                               newDist.get(vName), 1.0);
            v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(newDist.get(vName)));
          } else {						
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(), 1.0,
                                                 newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(newDist.get(vName)));
            }
						
            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              deltaLabelDiff +=
                ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(), 1.0,
                                                 newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(newDist.get(vName)));
            }
          }
        }
      }
			
      long endTime = System.currentTimeMillis();
			
      // clear map
      newDist.clear();
			
      int totalNodes = g._vertices.size();
      double deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes;

      if (verbose) {
        TObjectDoubleHashMap res = new TObjectDoubleHashMap();
        res.put(Constants.GetMRRString(), GraphEval.GetAverageTestMRR(g));
        res.put(Constants.GetPrecisionString(), GraphEval.GetAccuracy(g));
        resultList.add(res);

        System.out.println("after_iteration " + iter +
                           " objective: " + GetLPObjective(g) +
                           " accuracy: " + res.get(Constants.GetPrecisionString()) +
                           " rmse: " + GraphEval.GetRMSE(g) +
                           " time: " + (endTime - startTime) +
                           " label_diff_per_node: " + deltaLabelDiffPerNode +
                           " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                           " mrr_test: " + res.get(Constants.GetMRRString()));
      }
			
      // String outputFile = "estimates.iter_" + iter + ".log";  
      // g.SaveEstimatedScores(outputFile);
			
      if (false && deltaLabelDiffPerNode <= Constants.GetStoppingThreshold()) {
        if (useBipartitieOptimization) {
          if (iter > 1 && iter % 2 == 1) {
            MessagePrinter.Print("Convergence reached!!");
            break;
          }
        } else {
          MessagePrinter.Print("Convergence reached!!");
          break;
        }
      }
    }
		
    if (resultList.size() > 0) {
      TObjectDoubleHashMap res =
        (TObjectDoubleHashMap) resultList.get(resultList.size() - 1);
      MessagePrinter.Print(Constants.GetPrecisionString() + " " +
                           res.get(Constants.GetPrecisionString()));
      MessagePrinter.Print(Constants.GetMRRString() + " " +
                           res.get(Constants.GetMRRString()));
    }
  }
	
  public static double GetLPObjective(Graph g) {
    double obj = 0;
    Iterator<String> viter = g._vertices.keySet().iterator();
    while (viter.hasNext()) {
      String vName = viter.next();
      Vertex v = g._vertices.get(vName);
      obj += GetLPObjective(g, v);
    }
    return (obj);
  }
	
  public static double GetLPObjective(Graph g, Vertex v) {
    double obj = 0;
		
    // difference with injected labels
    if (v.IsSeedNode()) {
      obj += ProbUtil.GetDifferenceNorm2Squarred(
                                              v.GetInjectedLabelScores(), 1,
                                              v.GetEstimatedLabelScores(), 1);
    }
		
    // difference with labels of neighbors
    Object[] neighborNames = v.GetNeighborNames();
    for (int i = 0; i < neighborNames.length; ++i) {
      obj += v.GetNeighborWeight((String) neighborNames[i]) *
        ProbUtil.GetDifferenceNorm2Squarred(
                                         v.GetEstimatedLabelScores(), 1,
                                         g._vertices.get((String) neighborNames[i]).GetEstimatedLabelScores(), 1);
    }
		
    return (obj);
  }
}
