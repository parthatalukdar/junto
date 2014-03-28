package upenn.junto.algorithm.mad_sketch;

import upenn.junto.config.Flags;
// import upenn.junto.eval.GraphEval;
import upenn.junto.graph.*;
// import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;
import upenn.junto.util.ProbUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;
import gnu.trove.TObjectFloatIterator;

public class MADSketch {

  // This method runs Adsorption when mode = original, and it runs
  // Modified Adsorption (MAD) when mode = modified.
  public static void Run(Graph2 g, int maxIter, String mode,
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
      Vertex2 v = g._vertices.get(vName);
			
      // add a self loop. This will be useful in contributing
      // currently estimated label scores to next round's computation.
      // v.AddNeighbor(vName, 1.0);

      // v.NormalizeTransitionProbability();
			
      // remove dummy label: after normalization, some of the distributions
      // may not be valid probability distributions, but that is fine as the
      // algorithm doesn't require the scores to be normalized (to start with)
      
      //v.SetInjectedLabelScore(Constants.GetDummyLabel(), 0.0);
      v.SetInjectedLabelScore(Constants.GetDummyLabel(), (float) 0.0);
      
      // ProbUtil.Normalize(v.GetInjectedLabelScores());
			
      if (v.IsSeedNode()) {
//        TObjectFloatIterator injLabIter =
//        		g._labelManager.getLabelScores2(v.GetInjectedLabelScores()).iterator();
//        while (injLabIter.hasNext()) {
//          injLabIter.advance();
//          v.SetInjectedLabelScore((String) injLabIter.key(), injLabIter.value());
//        }
        v.SetEstimatedLabelScores(g._labelManager.clone(v.GetInjectedLabelScores()));
      } else {
        // remove dummy label
    	// TODO(ppt): check whether to make this permanent
        // v.SetEstimatedLabelScore(Constants.GetDummyLabel(), (float) 0.0);				
        
    	  // g.SetEstimatedScoresToUniformLabelPrior(v);				
        // ProbUtil.Normalize(v.GetEstimatedLabelScores());
      }
    }
		
//    if (verbose) { 
//      System.out.println("after_iteration " + 0 + 
//                         " objective: " + GetObjective(g, mu1, mu2, mu3) +
//                         " precision: " + GraphEval.GetAccuracy(g) +
//                         " rmse: " + GraphEval.GetRMSE(g) +
//                         " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
//                         " mrr_test: " + GraphEval.GetAverageTestMRR(g));	
//    }
    
    long timeInLastIteration = 0;
		
    for (int iter = 1; iter <= maxIter; ++iter) {
      System.gc();
      System.out.println("\nIteration: " + iter +
	  			 " memory_used: " + (Runtime.getRuntime().totalMemory() - 
	  					 					Runtime.getRuntime().freeMemory()) +
	  			 " time_in_lat_iter(msec): " + timeInLastIteration);
			
      long startTime = System.currentTimeMillis();

      // new (veretx, labelScores) map
      // HashMap<String, TObjectDoubleHashMap> newDist =
      //  new HashMap<String, TObjectDoubleHashMap>();
      HashMap<String,CountMinSketchLabel> newDist =
    		  new HashMap<String,CountMinSketchLabel>();
      
      Iterator<String> viter = g._vertices.keySet().iterator();
			
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex2 v = g._vertices.get(vName);
				
        // if not present already, create a new label score map
        // for the current hash map. otherwise, it is an error
        if (!newDist.containsKey(vName)) {
          // newDist.put(vName, new TObjectDoubleHashMap());
          // newDist.put(vName, new LabelScores<T>());
          newDist.put(vName, g._labelManager.getEmptyLabelDist());
        } else {
          MessagePrinter.PrintAndDie("Duplicate node found: " + vName);
        }
				
        // compute weighted neighborhood label distribution
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          String neighName = (String) neighNames[ni];
          Vertex2 neigh = g._vertices.get(neighName);

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
              System.out.println(v.GetName() + " " +
            		  			 v.GetContinuationProbability() + " " +
                                 v.GetNeighborWeight(neighName) + " " +
                                 neigh.GetContinuationProbability() + " " +
                                 neigh.GetNeighborWeight(vName));
            }
          } else {
            MessagePrinter.PrintAndDie("Invalid mode: " + mode);
          }
          if (mult <= 0) {
            MessagePrinter.PrintAndDie("Non-positive weighted edge:>>" +
                                       neigh.GetName() + "-->" + v.GetName() + "<<" + " " + mult);
          }
          
          // ProbUtil.AddScores(newDist.get(vName),
          CountMinSketchLabelManager.add(newDist.get(vName), (float) 1.0,                          	  
                          	  		neigh.GetEstimatedLabelScores(), (float) (mult * mu2));
        }
				
//        if (verbose) {
//          System.out.println("Before norm: " + v.GetName() + " " +
//                             // ProbUtil.GetSum(newDist.get(vName)));
//                             newDist.get(vName).getSum());
//        }
				
        // normalization is needed only for the original Adsorption algorithm
//        if (mode.equals("original")) {
//          // after normalization, we have the weighted
//          // neighborhood label distribution for the current node
//          // ProbUtil.Normalize(newDist.get(vName));
//          newDist.get(vName).normalize();
//        }
				
//        if (verbose) {
//          System.out.println("After norm: " + v.GetName() + " " +
//                             // ProbUtil.GetSum(newDist.get(vName)));
//                             newDist.get(vName).getSum());
//        }
        
        if (verbose) {
            System.out.println(iter + " after_neigh " + v.GetName() + " " +
                               // ProbUtil.GetSum(newDist.get(vName)) + 
            //                  newDist.get(vName).getSum() +
            //                   " " + CollectionUtil2.Map2String(newDist.get(vName)) +
                               " " + CollectionUtil2.Map2String(g._labelManager.getLabelScores(newDist.get(vName))) +
                               " mu1: " + mu1);
        }
				
        // add injection probability
        // ProbUtil.AddScores(newDist.get(vName),
        CountMinSketchLabelManager.add(newDist.get(vName), (float) 1.0,
                        v.GetInjectedLabelScores(), (float) (v.GetInjectionProbability() * mu1));
				
        if (verbose) {
          System.out.println(iter + " after_inj " + v.GetName() + " " +
                             // ProbUtil.GetSum(newDist.get(vName)) + 
          //                  newDist.get(vName).getSum() +
          //                   " " + CollectionUtil2.Map2String(newDist.get(vName)) +
                             " " + CollectionUtil2.Map2String(g._labelManager.getLabelScores(newDist.get(vName))) +
                             " mu1: " + mu1);
        }

        // add dummy label distribution
        // ProbUtil.AddScores(newDist.get(vName),
        CountMinSketchLabelManager.add(newDist.get(vName), (float) 1.0,
                        g._labelManager.GetDummyLabelDist(), (float) (v.GetTerminationProbability() * mu3));
                        // ()Constants.GetDummyLabelDist());
        // ProbUtil.AddScores(newDist.get(vName),
        // v.GetTerminationProbability() * mu3,
        // labels);
				
        if (verbose) {
          System.out.println(iter + " after_dummy " + v.GetName() + " " +
                             // ProbUtil.GetSum(newDist.get(vName)) + " " +
          //                   newDist.get(vName).getSum() + " " +
          //                   CollectionUtil2.Map2String(newDist.get(vName)) +
          					CollectionUtil2.Map2String(g._labelManager.getLabelScores(newDist.get(vName))) +
                            " injected: " + CollectionUtil2.Map2String(g._labelManager.getLabelScores(v.GetInjectedLabelScores())));
        }
				
//        // keep only the top scoring k labels, this is particularly useful
//        // when a large number of labels are involved.
//        if (keepTopKLabels < Integer.MAX_VALUE) {
//          // ProbUtil.KeepTopScoringKeys(newDist.get(vName), keepTopKLabels);
//        	newDist.get(vName).keepTopScoringKeys(keepTopKLabels);
//          if (newDist.get(vName).size() > keepTopKLabels) {
//            MessagePrinter.PrintAndDie("size mismatch: " +
//                                       newDist.get(vName).size() + " " + keepTopKLabels);
//          }
//        }
				
        // normalize in case of Adsorption
        if (Flags.IsModifiedMode(mode)) {
          // ProbUtil.DivScores(newDist.get(vName), v.GetNormalizationConstant(g, mu1, mu2, mu3));
        	
        	double divisor = v.GetNormalizationConstant(g, mu1, mu2, mu3);
        	if (verbose) {
        		System.out.println("BEFORE DIV: " + vName + " "  + divisor + " " + 
        				CollectionUtil2.Map2String(g._labelManager.getLabelScores(newDist.get(vName))));
        	}
        	CountMinSketchLabelManager.divScores(newDist.get(vName), divisor);
        	if (verbose) {
        		System.out.println("AFTER DIV:" + vName + " " + divisor + " " +
        				CollectionUtil2.Map2String(g._labelManager.getLabelScores(newDist.get(vName))));
        	}
        } else {
          // ProbUtil.Normalize(newDist.get(vName), keepTopKLabels);
//          newDist.get(vName).normalize(keepTopKLabels);
        }
      }			
			
//      double deltaLabelDiff = 0;
			
      int totalColumnUpdates = 0;
      int totalEntityUpdates = 0;
		
      // update all vertices with new estimated label scores
      viter = g._vertices.keySet().iterator();
      while (viter.hasNext()) {
        String vName = viter.next();
        Vertex2 v = g._vertices.get(vName);
				
//        if (false && v.IsSeedNode()) {
//          MessagePrinter.PrintAndDie("Should have never reached here!");
//          v.SetEstimatedLabelScores(v.GetInjectedLabelScores().clone());
//        } else {
          if (!useBipartitieOptimization) {
//            // deltaLabelDiff += ProbUtil.GetDifferenceNorm2Squarred(
//            //                                                   v.GetEstimatedLabelScores(), 1.0, newDist.get(vName), 1.0);
//        	  deltaLabelDiff +=
//        			  v.GetEstimatedLabelScores().getSquarredDifference(1.0, newDist.get(vName), 1.0);
//        	  
//            v.SetEstimatedLabelScores(newDist.get(vName).clone());

        	  // System.out.println("SETTING_EST: " + newDist.size() + " " + vName + " " + (newDist.get(vName) == null));
        	  v.SetEstimatedLabelScores(
        			  CountMinSketchLabelManager.clone(newDist.get(vName)));
          } else {
            // update column node labels on odd iterations
            if (Flags.IsColumnNode(vName) && (iter % 2 == 0)) {
              ++totalColumnUpdates;
//              deltaLabelDiff += 
//            		  // ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(),
//            		  v.GetEstimatedLabelScores().getSquarredDifference(1.0, newDist.get(vName), 1.0);
              v.SetEstimatedLabelScores(
            		  CountMinSketchLabelManager.clone(newDist.get(vName)));
            }
						
            // update entity labels on even iterations
            if (!Flags.IsColumnNode(vName) && (iter % 2 == 1)) {
              ++totalEntityUpdates;
//              deltaLabelDiff += // ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(),
//            		  v.GetEstimatedLabelScores().getSquarredDifference(1.0, newDist.get(vName), 1.0);
              v.SetEstimatedLabelScores(
            		  CountMinSketchLabelManager.clone(newDist.get(vName)));
            }
         }
       }
      //}
			
      long endTime = System.currentTimeMillis();
      timeInLastIteration = endTime - startTime;
			
      // clear map
      newDist.clear();
			
      int totalNodes = g._vertices.size();
//      double deltaLabelDiffPerNode = (1.0 * deltaLabelDiff) / totalNodes;

      TObjectDoubleHashMap res = new TObjectDoubleHashMap();
      res.put(Constants.GetMRRString(), Graph2.GetAverageTestMRR(g));
      res.put(Constants.GetPrecisionString(), Graph2.GetAccuracy(g));
      resultList.add(res);
//      if (verbose) {
//        System.out.println("after_iteration " + iter +
//                           " objective: " + GetObjective(g, mu1, mu2, mu3) +
//                           " accuracy: " + res.get(Constants.GetPrecisionString()) +
//                           " rmse: " + GraphEval.GetRMSE(g) +
//                           " time: " + (endTime - startTime) +
//                           " label_diff_per_node: " + deltaLabelDiffPerNode +
//                           " mrr_train: " + GraphEval.GetAverageTrainMRR(g) +
//                           " mrr_test: " + res.get(Constants.GetMRRString()) +
//                           " column_updates: " + totalColumnUpdates +
//                           " entity_updates: " + totalEntityUpdates + "\n");
//      }
			
//      if (false && deltaLabelDiffPerNode <= Constants.GetStoppingThreshold()) {
//        if (useBipartitieOptimization) {
//          if (iter > 1 && iter % 2 == 1) {
//            MessagePrinter.Print("Convergence reached!!");
//            break;
//          }
//        } else {
//          MessagePrinter.Print("Convergence reached!!");
//          break;
//        }
//      }
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
	
//  private static <T> double GetObjective(Graph2 g, double mu1, double mu2, double mu3) {
//    double obj = 0;
//    Iterator<String> viter = g._vertices.keySet().iterator();
//    while (viter.hasNext()) {
//      String vName = viter.next();
//      Vertex2 v = g._vertices.get(vName);
//      obj += GetObjective(g, v, mu1, mu2, mu3);
//    }
//    return (obj);
//  }
	
//  private static <T> double GetObjective(Graph2 g, Vertex2 v, double mu1, double mu2, double mu3) {
//    double obj = 0;
//		
//    // difference with injected labels
//    if (v.IsSeedNode()) {
//      obj += mu1 * v.GetInjectionProbability() *
//        // ProbUtil.GetDifferenceNorm2Squarred(v.GetInjectedLabelScores(),
//    	v.GetInjectedLabelScores().getSquarredDifference(
//                                         1,
//                                         v.GetEstimatedLabelScores(),
//                                         1);
//    }
//		
//    // difference with labels of neighbors
//    Object[] neighborNames = v.GetNeighborNames();
//    for (int i = 0; i < neighborNames.length; ++i) {
//      obj += mu2 * v.GetNeighborWeight((String) neighborNames[i]) *
//        // ProbUtil.GetDifferenceNorm2Squarred(v.GetEstimatedLabelScores(),
//    	v.GetInjectedLabelScores().getSquarredDifference(                                 
//                                         1,
//                                         g._vertices.get((String) neighborNames[i]).GetEstimatedLabelScores(),
//                                         1);
//    }
//		
//    // difference with dummy labels
////    obj += mu3 * ProbUtil.GetDifferenceNorm2Squarred(
////                                                  Constants.GetDummyLabelDist(),
////                                                  v.GetTerminationProbability(),
////                                                  v.GetEstimatedLabelScores(), 1);
//    obj += mu3 * v.GetEstimatedLabelScores().getSquarredDifference(
//    		1, 
//            g._labelManager.GetDummyLabelDist(),
//            v.GetTerminationProbability());
//		
//    return (obj);
//  }
}
