package upenn.junto.algorithm;

import upenn.junto.config.Flags;
import upenn.junto.eval.GraphEval;
import upenn.junto.graph.*;
import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;
import upenn.junto.util.ProbUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

public class QC {

  private static ArrayList<String> _allLabels;

  public static void Run(Graph g, int maxIter, double mu1,
                         double mu2, double mu3, boolean normalize, int keepTopKLabels,
                         boolean useBipartitieOptimization, boolean verbose, ArrayList resultList) {

    TObjectDoubleHashMap seedAmpliFactor = null;
    _allLabels = new ArrayList<String>();

    // -- normalize edge weights
    // -- if seed node, then initialize estimated labels with injected
    Iterator<String> viter1 = g._vertices.keySet().iterator();
    while (viter1.hasNext()) {
      String vName = viter1.next();
      Vertex v = g._vertices.get(vName);

      // v.NormalizeTransitionProbability();
      // ProbUtil.Normalize(v.GetInjectedLabelScores());

      if (v.IsSeedNode()) {
        TObjectDoubleIterator injLabIter = v.GetInjectedLabelScores()
          .iterator();
        while (injLabIter.hasNext()) {
          injLabIter.advance();
          v.SetInjectedLabelScore((String) injLabIter.key(),
                                  injLabIter.value());
          _allLabels.add((String) injLabIter.key());
        }
        v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(v.GetInjectedLabelScores()));
      } else {
        // v.SetEstimatedLabelScore(Constants.GetDummyLabel(), 0.0);
        // v.SetEstimatedLabelScores(Utils.GetUniformPrior(_allLabels));

        // ProbUtil.Normalize(v.GetEstimatedLabelScores());
      }
    }
		
    TObjectDoubleHashMap uniformPrior = ProbUtil.GetUniformPrior(_allLabels);

    for (int iter = 1; iter <= maxIter; ++iter) {
      System.out.println("Iteration: " + iter);

      long startTime = System.currentTimeMillis();

      HashMap<String, TObjectDoubleHashMap> newDist =
        new HashMap<String, TObjectDoubleHashMap>();
      Iterator<String> viter = g._vertices.keySet().iterator();

      boolean printLog = false;

      //			int processedCnt = 0;
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex v = g._vertices.get(vName);
				
        //				++processedCnt;
        //				if (processedCnt % 100000 == 0) {
        //					MessagePrinter.Print("Processed " + processedCnt);
        //				}

        // if not present already, create a new label score map
        // for the current hash map. otherwise, it is an error
        if (!newDist.containsKey(vName)) {
          newDist.put(vName, new TObjectDoubleHashMap());
        } else {
          MessagePrinter
            .PrintAndDie("Duplicate node found: " + vName);
        }

        // compute weighted neighborhood label distribution
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          String neighName = (String) neighNames[ni];
          Vertex neigh = g._vertices.get(neighName);

          double mult = -1;
          // w_uv (where u is neighbor)
          mult = neigh.GetNeighborWeight(vName);
          if (mult <= 0) {
            MessagePrinter.PrintAndDie("Zero weight edge: "
                                       + neigh.GetName() + " --> " + v.GetName());
          }

          ProbUtil.AddScores(newDist.get(vName), mult * mu2, neigh
                          .GetEstimatedLabelScores());
        }

        if (printLog) {
          // System.out.println("Before norm: " + v.GetName() + " " +
          // ProbUtil.GetSum(newDist.get(vName)));
        }

        // add injection probability
        ProbUtil.AddScores(newDist.get(vName), mu1,
                        v.GetInjectedLabelScores());

        if (printLog) {
          // System.out.println(iter + " after_inj " + v.GetName() + "
          // " +
          // ProbUtil.GetSum(newDist.get(vName)) +
          // " " + CollectionUtil.Map2String(newDist.get(vName)) +
          // " mu1: " + mu1);
        }

        // add uniform prior
        if (mu3 != 0) {
          ProbUtil.AddScores(newDist.get(vName), mu3,
                          uniformPrior);
        }

        if (printLog) {
          System.out.println(iter + " after_dummy " + v.GetName()
                             + " " + ProbUtil.GetSum(newDist.get(vName)) + " "
                             + CollectionUtil.Map2String(newDist.get(vName))
                             + " injected: "
                             + CollectionUtil.Map2String(v.GetInjectedLabelScores()));
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

        // normalize
        if (!normalize) {
          ProbUtil.DivScores(newDist.get(vName), 
                          v.GetNormalizationConstant2(g, mu1, mu2, mu3));
        } else {
          ProbUtil.Normalize(newDist.get(vName), keepTopKLabels);
        }
      }

      double deltaLabelDiff = 0;
      int totalColumnUpdates = 0;
      int totalEntityUpdates = 0;
			
      //			processedCnt = 0;

      // update all vertices with new estimated label scores
      viter = g._vertices.keySet().iterator();
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex v = g._vertices.get(vName);
				
        //				++processedCnt;
        //				if (processedCnt % 100000 == 0) {
        //					MessagePrinter.Print("Processed_2 " + processedCnt);
        //				}

        if (false && v.IsSeedNode()) {
          MessagePrinter
            .PrintAndDie("Should have never reached here!");
          v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(v.GetInjectedLabelScores()));
        } else {
          if (!useBipartitieOptimization) {
            deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(v
                                                               .GetEstimatedLabelScores(), 1.0, newDist
                                                               .get(vName), 1.0);
            v.SetEstimatedLabelScores(new TObjectDoubleHashMap<String>(newDist.get(vName)));
          } else {
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              ++totalColumnUpdates;
              deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
                                                                 v.GetEstimatedLabelScores(), 1.0,
                                                                 newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(
                new TObjectDoubleHashMap<String>(newDist.get(vName)));
            }

            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              ++totalEntityUpdates;
              deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
                                                                 v.GetEstimatedLabelScores(), 1.0,
                                                                 newDist.get(vName), 1.0);
              g._vertices.get(vName).SetEstimatedLabelScores(
                new TObjectDoubleHashMap<String>(newDist.get(vName)));
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
                           " objective: " + GetObjective(g, mu1, mu2, mu3) +
                           " accuracy: " + res.get(Constants.GetPrecisionString()) +
                           " rmse: " + GraphEval.GetRMSE(g) +
                           " time: " + (endTime - startTime) +
                           " label_diff_per_node: " + deltaLabelDiffPerNode +
                           " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
                           " mrr_test: " + res.get(Constants.GetMRRString()) +
                           " column_updates: "	+ totalColumnUpdates +
                           " entity_updates: " + totalEntityUpdates + "\n");
      }

      if (false && iter >= 5
          && deltaLabelDiffPerNode <= Constants.GetStoppingThreshold()) {
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

  private static double GetObjective(Graph g, double mu1, double mu2,
                                     double mu3) {
    double obj = 0;
    Iterator<String> viter = g._vertices.keySet().iterator();
    while (viter.hasNext()) {
      String vName = viter.next();
      Vertex v = g._vertices.get(vName);
      obj += GetObjective(g, v, mu1, mu2, mu3);
    }
    return (obj);
  }

  private static double GetObjective(Graph g, Vertex v, double mu1,
                                     double mu2, double mu3) {
    double obj = 0;

    // difference with injected labels
    if (v.IsSeedNode()) {
      obj += mu1
        * ProbUtil.GetDifferenceNorm2Squarred(v.GetInjectedLabelScores(), 1, 
                                              v.GetEstimatedLabelScores(), 1);
    }

    // difference with labels of neighbors
    Object[] neighborNames = v.GetNeighborNames();
    for (int i = 0; i < neighborNames.length; ++i) {
      obj += mu2
        * v.GetNeighborWeight((String) neighborNames[i])
        * ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(), 1, 
               g._vertices.get((String) neighborNames[i]).GetEstimatedLabelScores(), 1);
    }

    // difference with dummy labels
    if (mu3 != 0) {
      obj += mu3
        * ProbUtil.GetDifferenceNorm2Squarred(ProbUtil.GetUniformPrior(_allLabels), 1, 
                                              v.GetEstimatedLabelScores(), 1);
    }

    return (obj);
  }
}
