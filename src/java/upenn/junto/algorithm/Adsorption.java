package upenn.junto.algorithm;

import upenn.junto.config.Flags;
import upenn.junto.eval.GraphEval;
import upenn.junto.graph.*;
import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;
import upenn.junto.util.ProbUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;

public class Adsorption {

  // This method runs Adsorption when mode = original, and it runs
  // Modified Adsorption (MAD) when mode = modified.
  public static void Run(Graph g, int maxIter, String mode,
                         double mu1, double mu2, double mu3,
                         int keepTopKLabels, boolean useBipartitieOptimization,
                         boolean verbose, ArrayList resultList) {
		
    // Class prior normalization
    // g.ClassPriorNormalization();
		
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
      // ProbUtil.Normalize(v.GetInjectedLabelScores());
			
      if (v.IsSeedNode()) {
        TObjectDoubleIterator injLabIter = v.GetInjectedLabelScores().iterator();
        while (injLabIter.hasNext()) {
          injLabIter.advance();
          v.SetInjectedLabelScore((String) injLabIter.key(), injLabIter.value());
        }
        v.SetEstimatedLabelScores(v.GetInjectedLabelScores().clone());
      } else {
        // remove dummy label
        v.SetEstimatedLabelScore(Constants.GetDummyLabel(), 0.0);				
        // g.SetEstimatedScoresToUniformLabelPrior(v);				
        // ProbUtil.Normalize(v.GetEstimatedLabelScores());
      }
    }
		
    if (verbose) { 
      System.out.println("after_iteration " + 0 + 
                         " objective: " + GetObjective(g, mu1, mu2, mu3) +
                         " precision: " + GraphEval.GetAccuracy(g) +
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

          double mult = -1;
          if (Flags.IsOriginalMode(mode)) {
            // multiplier for Adsorption update
            // p_v_cont * w_uv (where u is neighbor)
            mult = v.GetContinuationProbability() * neigh.GetNeighborWeight(vName);
          } else if (Flags.IsModifiedMode(mode)) {
            // multiplier for MAD update
            // (p_v_cont * w_vu + p_u_cont * w_uv) where u is neighbor
            mult = (v.GetContinuationProbability() * v.GetNeighborWeight(neighName) +
                    neigh.GetContinuationProbability() * neigh.GetNeighborWeight(vName));
						
            if (verbose) {
              System.out.println(v.GetName() + " " + v.GetContinuationProbability() + " " +
                                 v.GetNeighborWeight(neighName) + " " +
                                 neigh.GetContinuationProbability() + " " + neigh.GetNeighborWeight(vName));
            }
          } else {
            MessagePrinter.PrintAndDie("Invalid mode: " + mode);
          }
          if (mult <= 0) {
            MessagePrinter.PrintAndDie("Non-positive weighted edge:>>" +
                                       neigh.GetName() + "-->" + v.GetName() + "<<" + " " + mult);
          }

          ProbUtil.AddScores(newDist.get(vName),
                          mult * mu2,
                          neigh.GetEstimatedLabelScores());
        }
				
        if (verbose) {
          System.out.println("Before norm: " + v.GetName() + " " +
                             ProbUtil.GetSum(newDist.get(vName)));
        }
				
        // normalization is needed only for the original Adsorption algorithm
        if (mode.equals("original")) {
          // after normalization, we have the weighted
          // neighborhood label distribution for the current node
          ProbUtil.Normalize(newDist.get(vName));
        }
				
        if (verbose) {
          System.out.println("After norm: " + v.GetName() + " " +
                             ProbUtil.GetSum(newDist.get(vName)));
        }
				
        // add injection probability
        ProbUtil.AddScores(newDist.get(vName),
                        v.GetInjectionProbability() * mu1,
                        v.GetInjectedLabelScores());
				
        if (verbose) {
          System.out.println(iter + " after_inj " + v.GetName() + " " +
                             ProbUtil.GetSum(newDist.get(vName)) + 
                             " " + CollectionUtil.Map2String(newDist.get(vName)) +
                             " mu1: " + mu1);
        }

        // add dummy label distribution
        ProbUtil.AddScores(newDist.get(vName),
                        v.GetTerminationProbability() * mu3,
                        Constants.GetDummyLabelDist());
        // ProbUtil.AddScores(newDist.get(vName),
        // v.GetTerminationProbability() * mu3,
        // labels);
				
        if (verbose) {
          System.out.println(iter + " after_dummy " + v.GetName() + " " +
                             ProbUtil.GetSum(newDist.get(vName)) + " " +
                             CollectionUtil.Map2String(newDist.get(vName)) +
                             " injected: " + CollectionUtil.Map2String(v.GetInjectedLabelScores()));
        }
				
        // keep only the top scoring k labels, this is particularly useful
        // when a large number of labels are involved.
        if (keepTopKLabels < Integer.MAX_VALUE) {
          ProbUtil.KeepTopScoringKeys(newDist.get(vName), keepTopKLabels);
          if (newDist.get(vName).size() > keepTopKLabels) {
            MessagePrinter.PrintAndDie("size mismatch: " +
                                       newDist.get(vName).size() + " " + keepTopKLabels);
          }
        }
				
        // normalize in case of Adsorption
        if (Flags.IsModifiedMode(mode)) {
          ProbUtil.DivScores(newDist.get(vName), v.GetNormalizationConstant(g, mu1, mu2, mu3));
        } else {
          ProbUtil.Normalize(newDist.get(vName), keepTopKLabels);
        }
      }			
			
      double deltaLabelDiff = 0;
			
      int totalColumnUpdates = 0;
      int totalEntityUpdates = 0;
		
      // update all vertices with new estimated label scores
      viter = g._vertices.keySet().iterator();
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex v = g._vertices.get(vName);
				
        if (false && v.IsSeedNode()) {
          MessagePrinter.PrintAndDie("Should have never reached here!");
          v.SetEstimatedLabelScores(v.GetInjectedLabelScores().clone());
        } else {
          if (!useBipartitieOptimization) {
            deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
                                                               v.GetEstimatedLabelScores(), 1.0, newDist.get(vName), 1.0);
            v.SetEstimatedLabelScores(newDist.get(vName).clone());
          } else {
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              ++totalColumnUpdates;
              deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
                                                                 v.GetEstimatedLabelScores(), 1.0, newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(newDist.get(vName).clone());
            }
						
            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              ++totalEntityUpdates;
              deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
                                                                 v.GetEstimatedLabelScores(), 1.0, newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(newDist.get(vName).clone());
            }
          }
        }
      }
			
      long endTime = System.currentTimeMillis();
			
      // clear map
      newDist.clear();
			
      int totalNodes = g._vertices.size();
      double deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes;

      TObjectDoubleHashMap res = new TObjectDoubleHashMap();
      res.put(Constants.GetMRRString(), GraphEval.GetAverageTestMRR(g));
      res.put(Constants.GetPrecisionString(), GraphEval.GetAccuracy(g));
      resultList.add(res);
      if (verbose) {
        System.out.println("after_iteration " + iter +
                           " objective: " + GetObjective(g, mu1, mu2, mu3) +
                           " accuracy: " + res.get(Constants.GetPrecisionString()) +
                           " rmse: " + GraphEval.GetRMSE(g) +
                           " time: " + (endTime - startTime) +
                           " label_diff_per_node: " + deltaLabelDiffPerNode +
                           " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                           " mrr_test: " + res.get(Constants.GetMRRString()) +
                           " column_updates: " + totalColumnUpdates +
                           " entity_updates: " + totalEntityUpdates + "\n");
      }
			
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
	
  private static double GetObjective(Graph g, double mu1, double mu2, double mu3) {
    double obj = 0;
    Iterator<String> viter = g._vertices.keySet().iterator();
    while (viter.hasNext()) {
      String vName = viter.next();
      Vertex v = g._vertices.get(vName);
      obj += GetObjective(g, v, mu1, mu2, mu3);
    }
    return (obj);
  }
	
  private static double GetObjective(Graph g, Vertex v, double mu1, double mu2, double mu3) {
    double obj = 0;
		
    // difference with injected labels
    if (v.IsSeedNode()) {
      obj += mu1 * v.GetInjectionProbability() *
        ProbUtil.GetDifferenceNorm2Squarred(
                                         v.GetInjectedLabelScores(), 1,
                                         v.GetEstimatedLabelScores(), 1);
    }
		
    // difference with labels of neighbors
    Object[] neighborNames = v.GetNeighborNames();
    for (int i = 0; i < neighborNames.length; ++i) {
      obj += mu2 * v.GetNeighborWeight((String) neighborNames[i]) *
        ProbUtil.GetDifferenceNorm2Squarred(
                                         v.GetEstimatedLabelScores(), 1,
                                         g._vertices.get((String) neighborNames[i]).GetEstimatedLabelScores(), 1);
    }
		
    // difference with dummy labels
    obj += mu3 * ProbUtil.GetDifferenceNorm2Squarred(
                                                  Constants.GetDummyLabelDist(), v.GetTerminationProbability(),
                                                  v.GetEstimatedLabelScores(), 1);
		
    return (obj);
  }
}
