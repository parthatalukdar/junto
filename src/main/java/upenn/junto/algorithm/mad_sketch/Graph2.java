package upenn.junto.algorithm.mad_sketch;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;
import gnu.trove.TObjectFloatIterator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Random;

import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;

// import upenn.junto.label.*;
import upenn.junto.util.ObjectDoublePair;
import upenn.junto.util.RyanAlphabet;
// import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.Defaults;
import upenn.junto.util.MessagePrinter;

public class Graph2 {
  public HashMap<String,Vertex2> _vertices;
  public TObjectDoubleHashMap<String> _labels;
  public CountMinSketchLabelManager _labelManager;
  private boolean _isSeedInjected = false;
	
  private static String kDelim_ = "\t"; 
	
  public Graph2 (Hashtable config) {
    _vertices = new HashMap<String,Vertex2>();
    _labels = new TObjectDoubleHashMap<String>();
    
    int depth = Integer.parseInt(Defaults.GetValueOrDie(config, "sketch_depth"));
    int width = Integer.parseInt(Defaults.GetValueOrDie(config, "sketch_width"));
    
    _labelManager = new CountMinSketchLabelManager(depth, width);
  }
	
//  public Vertex2 AddVertex2(String name, String label) {
//    return (this.AddVertex2(name, label, 1.0));
//  }

  public Vertex2 AddVertex(String name, String label, float weight) {
    Vertex2 v = _vertices.get(name);
		
    // Add if the Vertex2 is already not preset. Or, if the Vertex2
    // is present but it doesn't have a valid label assigned, then
    // update its label
    if (v == null) {
      _vertices.put(name, new Vertex2(name,
    		  						label,
    		  						weight,
    		  						_labelManager));
    } else {
      v.SetGoldLabel(label, weight);
    }
    return _vertices.get(name);
  }
	
  public Vertex2 GetVertex2(String name) {
    if (!_vertices.containsKey(name)) {
      MessagePrinter.PrintAndDie("Node " + name + " doesn't exist!");
    }
    return _vertices.get(name);
  }
	
  // remove nodes if they are not connected to a certain number
  // of neighbors.
  public void PruneLowDegreeNodes(int minNeighborCount) {
    Iterator<String> vIter = _vertices.keySet().iterator();
    int totalPruned = 0;
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      // if a node has lower than certain number of neighbors, then
      // that node can be filtered out, also the node should be a
      // feature node.
      if (/*v.GetName().startsWith(Constants.GetFeatPrefix()) &&*/
          v.GetNeighborNames().length <= minNeighborCount) {
        // make the node isolated
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          v.RemoveNeighbor((String) neighNames[ni]);
          _vertices.get(neighNames[ni]).RemoveNeighbor(v.GetName());					
        }
				
        // now remove the Vertex2 from the Vertex2 list
        vIter.remove();
        ++totalPruned;
      }
    }
		
    System.out.println("Total nodes pruned: " + totalPruned);
  }
	
  // remove feature nodes if they are not connected to a certain number
  // of neighbors.
  public void PruneLowDegreeFeatNodes(int minNeighborCount) {
    Iterator<String> vIter = _vertices.keySet().iterator();
    int totalPruned = 0;
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      // if a node has lower than certain number of neighbors, then
      // that node can be filtered out, also the node should be a
      // feature node.
      if (v.GetName().startsWith(Constants.GetFeatPrefix()) &&
          v.GetNeighborNames().length <= minNeighborCount) {
        // make the node isolated
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          v.RemoveNeighbor((String) neighNames[ni]);
          _vertices.get(neighNames[ni]).RemoveNeighbor(v.GetName());					
        }
				
        // now remove the Vertex2 from the Vertex2 list
        vIter.remove();
        ++totalPruned;
      }
    }
		
    System.out.println("Total nodes pruned: " + totalPruned);
  }
	
  public void PruneHighDegreeNodes(int maxNeighborCount) {
    Iterator<String> vIter = _vertices.keySet().iterator();
    int totalPruned = 0;
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      // if a node has higher than certain number of neighbors, then
      // that node can be filtered out, also the node should be a
      // feature node.
      if (/*v.GetName().startsWith(Constants.GetFeatPrefix()) &&*/
          v.GetNeighborNames().length >= maxNeighborCount) {
        // make the node isolated
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          v.RemoveNeighbor((String) neighNames[ni]);
          _vertices.get(neighNames[ni]).RemoveNeighbor(v.GetName());					
        }
				
        // now remove the Vertex2 from the Vertex2 list
        vIter.remove();
        ++totalPruned;
      }
    }
		
    System.out.println("Total nodes pruned: " + totalPruned);
  }
	
  public void PruneHighDegreeFeatNodes(int maxNeighborCount) {
    Iterator<String> vIter = _vertices.keySet().iterator();
    int totalPruned = 0;
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      // if a feature node has higher than certain number of neighbors, then
      // that node can be filtered out, also the node should be a
      // feature node.
      if (v.GetName().startsWith(Constants.GetFeatPrefix()) &&
          v.GetNeighborNames().length >= maxNeighborCount) {
        // make the node isolated
        Object[] neighNames = v.GetNeighborNames();
        for (int ni = 0; ni < neighNames.length; ++ni) {
          v.RemoveNeighbor((String) neighNames[ni]);
          _vertices.get(neighNames[ni]).RemoveNeighbor(v.GetName());					
        }
				
        // now remove the Vertex2 from the Vertex2 list
        vIter.remove();
        ++totalPruned;
      }
    }
		
    System.out.println("Total nodes pruned: " + totalPruned);
  }
	
  // keep only K highest scoring neighbors
  public void KeepTopKNeighbors(int K) {
    int totalEdges = 0;
    Iterator<String> vIter = _vertices.keySet().iterator();
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      Object[] neighNames = v.GetNeighborNames();
      TObjectDoubleHashMap neighWeights = new TObjectDoubleHashMap();
      int totalNeighbors = neighNames.length;
      for (int ni = 0; ni < totalNeighbors; ++ni) {
        neighWeights.put(neighNames[ni], v.GetNeighborWeight((String) neighNames[ni]));
      }
      // now sort the neighbors
      ArrayList<ObjectDoublePair> sortedNeighList = CollectionUtil2.ReverseSortMap(neighWeights);
			
      // Since the array is reverse sorted, the highest scoring
      // neighbors are listed first, which we want to retain. Hence,
      // remove everything after after the top-K neighbors.
      totalNeighbors = sortedNeighList.size();
      for (int sni = K; sni < totalNeighbors; ++sni) {
        v.RemoveNeighbor((String) sortedNeighList.get(sni).GetLabel());
      }
			
      totalEdges += v.GetNeighborNames().length;
    }
		
    // now make all the directed edges undirected.
    vIter = _vertices.keySet().iterator();
    while (vIter.hasNext()) {
      Vertex2 v = _vertices.get(vIter.next());
			
      Object[] neighNames = v.GetNeighborNames();
      int totalNeighbors = neighNames.length;
      for (int ni = 0; ni < totalNeighbors; ++ni) {
        Vertex2 neigh = _vertices.get(neighNames[ni]);
				
        // if the reverse edge is not present, then add it
        if (neigh.GetNeighborWeight(v.GetName()) == 0) {
          neigh.AddNeighbor(v.GetName(),
                            v.GetNeighborWeight((String) neighNames[ni]));
          ++totalEdges;
        }
      }
    }
		
    System.out.println("Total edges: " + totalEdges);
  }
	
  public void CheckAndStoreSeedLabelInformation(int maxLabelsPerClass) {
    TObjectDoubleHashMap<String> testLabels = new TObjectDoubleHashMap<String>();
    TObjectDoubleHashMap<String> seedLabels = new TObjectDoubleHashMap<String>();

    for (String vName : this._vertices.keySet()) {
      Vertex2 v = this._vertices.get(vName);
      if (v.IsTestNode()) {
        TObjectDoubleIterator labelIter =
        		this._labelManager.getLabelScores(v.GetGoldLabel()).iterator();
        while (labelIter.hasNext()) {
          labelIter.advance();
          testLabels.adjustOrPutValue((String) labelIter.key(), 1, 1);
        }
      }
			
      if (v.IsSeedNode()) {
        Object[] injLabels= this._labelManager.getLabelScores(v.GetInjectedLabelScores()).keys();
        for (int li = 0; li < injLabels.length; ++li) {
          seedLabels.adjustOrPutValue((String) injLabels[li], 1, 1);
        }
      }
    }
		
    // store the labels currently seeded into the graph
    this._labels = seedLabels;
		
    MessagePrinter.Print("Total Test Labels: " + testLabels.size() +
                         " " + CollectionUtil2.Map2String(testLabels));				
    MessagePrinter.Print("Total Seed Labels: " + seedLabels.size() +
                         " " + CollectionUtil2.Map2String(seedLabels));
		
    //		// now check that count of train labels are exactly same as the
    //		// required count, if not then throw an error
    //		TObjectDoubleIterator seedLabelIterator = seedLabels.iterator();
    //		while (seedLabelIterator.hasNext()) {
    //			seedLabelIterator.advance();
    //			if (seedLabelIterator.value() < maxLabelsPerClass) {
    //				MessagePrinter.PrintAndDie("ERROR: Seed Label " + seedLabelIterator.key() +
    //						" count: " + seedLabelIterator.value() +
    //						" required: " + maxLabelsPerClass);
    //			}
    //		}
  }
	
//  public void RemoveTrainOnlyLabels() {
//    TObjectDoubleHashMap<String> testLabels = new TObjectDoubleHashMap<String>();
//    for (String vName : this._vertices.keySet()) {
//      Vertex2 v = this._vertices.get(vName);
//      if (v.IsTestNode()) {
//        TObjectDoubleIterator labelIter = v.GetGoldLabel().getLabels().iterator();
//        while (labelIter.hasNext()) {
//          labelIter.advance();
//          testLabels.adjustOrPutValue((String) labelIter.key(), 1, 1);
//        }
//      }
//    }
//    // MessagePrinter.Print("Total Test Labels: " + testLabels.size() +
//    // " " + CollectionUtil2.Map2String(testLabels));
//		
//    HashSet<String> trainOnlyLabels = new HashSet<String>();
//    TObjectDoubleHashMap<String> trainLabels = new TObjectDoubleHashMap<String>();
//		
//    // now iterate over the nodes and remove any seed label that is not
//    // present in any of the test nodes.
//    for (String vName : this._vertices.keySet()) {
//      Vertex2 v = this._vertices.get(vName);
//      if (v.IsSeedNode()) {
//        Object[] injLabels= v.GetInjectedLabelScores().getLabels().keys();
//        for (int li = 0; li < injLabels.length; ++li) {
//          if (!testLabels.contains(injLabels[li])) {
//            v.RemoveInjectedLabel((String) injLabels[li]);
//            if (v.GetInjectedLabelScores().size() == 0) {
//              v.ResetSeedNode();
//            }
//						
//            if (!trainOnlyLabels.contains((String) injLabels[li])) {
//              trainOnlyLabels.add((String) injLabels[li]);
//              MessagePrinter.Print("Removing train only label: " + injLabels[li]);
//            }
//          } else {
//            trainLabels.adjustOrPutValue((String) injLabels[li], 1, 1);
//          }
//        }
//      }
//    }
//		
//    // now iterate over the nodes again and remove any test only labels i.e.
//    // label which is present in one of the test and in none of the train nodes
//    for (String vName : this._vertices.keySet()) {
//      Vertex2 v = this._vertices.get(vName);
//      if (v.IsTestNode()) {
//        Object[] goldLabels= v.GetGoldLabel().getLabels().keys();
//        for (int li = 0; li < goldLabels.length; ++li) {
//          if (!trainLabels.contains(goldLabels[li])) {
//            v.RemoveGoldLabel((T) goldLabels[li]);
//            if (v.GetGoldLabel().size() == 0) {
//              v.ResetTestNode();
//            }						
//          }
//        }
//      }
//    }
//		
//    //		MessagePrinter.Print("Total Seed Labels: " + trainLabels.size() +
//    //				" " + CollectionUtil2.Map2String(trainLabels));
//  }
	
  public void SaveEstimatedScores(String outputFile) {
    try {
      double doc_mrr_sum = 0;
      int correct_doc_cnt = 0;
      int total_doc_cnt = 0;

      BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
      Iterator<String> vIter = _vertices.keySet().iterator();			
      while (vIter.hasNext()) {
        String vName = vIter.next();
        Vertex2 v = _vertices.get(vName);
				
        if (v.IsTestNode()) {
          double mrr = v.GetMRR();
          ++total_doc_cnt;
          doc_mrr_sum += mrr;
          if (mrr == 1) {
            ++correct_doc_cnt;
          }
        }
        bw.write(v.GetName() + kDelim_ +
                // CollectionUtil2.Map2String(v.GetGoldLabel(), _labelManager) + kDelim_ +
                // v.GetInjectedLabelScoresPretty(this._labelManager) + kDelim_ +
                // v.GetEstimatedLabelScoresPretty(this._labelManager) + kDelim_ +
        		CollectionUtil2.Map2String(this._labelManager.getLabelScores(v.GetGoldLabel())) + kDelim_ +
        		CollectionUtil2.Map2String(this._labelManager.getLabelScores(v.GetInjectedLabelScores())) + kDelim_ +
        		CollectionUtil2.Map2String(this._labelManager.getLabelScores(v.GetEstimatedLabelScores())) + kDelim_ +
                 v.IsTestNode() + kDelim_ +
                 v.GetMRR() + "\n");
      }
      bw.close();
			
      // print summary result
      // assert (total_doc_cnt > 0);
      if (total_doc_cnt > 0) {
        System.out.println("PRECISION " + (1.0 * correct_doc_cnt) / total_doc_cnt +
                           " (" + correct_doc_cnt + " correct out of " + total_doc_cnt + ")");
        System.out.println("MRR " + (1.0 * doc_mrr_sum) / total_doc_cnt);
      } else {
        System.out.println("Total test instances evaluated: " + total_doc_cnt);
      }
			
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
	
  //	public void WriteToFile(String outputFile) {
  //		try {
  //			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
  //			Iterator<String> vIter = _vertices.keySet().iterator();			
  //			while (vIter.hasNext()) {
  //				String vName = vIter.next();
  //				Vertex2 v = _vertices.get(vName);
  //				Object[] neighNames = v.GetNeighborNames();
  //				String neighStr = "";
  //				int totalNeighbors = neighNames.length;
  //				for (int ni = 0; ni < totalNeighbors; ++ni) {
  //					Vertex2 n = _vertices.get(neighNames[ni]);
  //					
  //					neighStr += neighNames[ni] + " " +
  //									v.GetNeighborWeight((String) neighNames[ni]);
  //					if (ni < totalNeighbors - 1) {
  //						neighStr += " ";
  //					}
  //				}
  //				
  //				String rwProbStr =
  //					CollectionUtil2._kInjProb + " " + v.GetInjectionProbability() + " " +
  //					CollectionUtil2._kContProb + " " + v.GetContinuationProbability() + " " +					
  //					CollectionUtil2._kTermProb + " " + v.GetTerminationProbability();
  //				
  //				// output format
  //				// id gold_label injected_labels estimated_labels neighbors rw_probabilities
  //				bw.write(v.GetName() + kDelim_ +
  //						CollectionUtil2.Map2String(v.GetGoldLabel(), _labelAlphabet) + kDelim_ +
  //						CollectionUtil2.Map2String(v.GetInjectedLabelScores(), _labelAlphabet) + kDelim_ +
  //						CollectionUtil2.Map2String(v.GetEstimatedLabelScores(), _labelAlphabet) + kDelim_ +
  //						neighStr + kDelim_ +
  //						rwProbStr + "\n");
  //			}
  //			bw.close();
  //		} catch (IOException ioe) {
  //			ioe.printStackTrace();
  //		}
  //	}
	
  public DefaultDirectedWeightedGraph<Vertex2,DefaultWeightedEdge> GetJGraphTGraph() {
    DefaultDirectedWeightedGraph<Vertex2,DefaultWeightedEdge> g2 =
      new DefaultDirectedWeightedGraph<Vertex2, DefaultWeightedEdge>(DefaultWeightedEdge.class);
    Set<String> vertices = this._vertices.keySet();
    for (String vName : vertices) {
      Vertex2 v = this._vertices.get(vName);
      g2.addVertex(v);
			
      for (Object nName : this._vertices.get(vName).GetNeighbors().keys()) {
        Vertex2 nv = this._vertices.get(nName);
        g2.addVertex(nv);
				
        DefaultWeightedEdge e = g2.addEdge(v, nv);
        double ew = v.GetNeighborWeight((String) nName);
        if (e != null) {
          g2.setEdgeWeight(e, ew);
        }
      }
    }
		
    return (g2);
  }
	
  public void WriteToFileWithAlphabet(String graphOutputFile) {
    try {
      BufferedWriter graphWriter =
        new BufferedWriter(new FileWriter(graphOutputFile));
      BufferedWriter seedWriter =
        new BufferedWriter(new FileWriter(graphOutputFile + ".seed_vertices"));

      RyanAlphabet alpha = new RyanAlphabet();
      // we would like to avoid 0 as the index
      alpha.lookupIndex("", true);
			
      Iterator<String> vIter = _vertices.keySet().iterator();			
      while (vIter.hasNext()) {
        String vName = vIter.next();
        int vIdx = alpha.lookupIndex(vName, true);
				
        // seed node writer
        if (_vertices.get(vName).IsSeedNode()) {
          seedWriter.write(vIdx + "\t" + vIdx + "\t" + 1.0 + "\n");
        }

        Vertex2 v = _vertices.get(vName);
        Object[] neighNames = v.GetNeighborNames();
        int totalNeighbors = neighNames.length;
        for (int ni = 0; ni < totalNeighbors; ++ni) {
          int nIdx = alpha.lookupIndex(neighNames[ni], true);
					
          // edge writer
          graphWriter.write(vIdx + "\t" +
                            nIdx + "\t" +
                            v.GetNeighborWeight((String) neighNames[ni]) +
                            "\n");
        }				
      }
      graphWriter.close();
      seedWriter.close();
			
      // write out the alphabet file
      alpha.dump(graphOutputFile + ".alpha");
			
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public void SetGaussianWeights(double sigmaFactor) {
    // get the average edge weight of the graph
    double avgEdgeWeight = GetAverageEdgeWeightSqrt();
		
    for (String vName : this._vertices.keySet()) {
      Vertex2 v = this._vertices.get(vName);
      TObjectDoubleIterator nIter = v.GetNeighbors().iterator();
      while (nIter.hasNext()) {
        nIter.advance();
        String nName = (String) nIter.key();

        // we assume that the currently set distances are distance squares
        double currWeight = v.GetNeighborWeight(nName);
        double sigmaSquarred = Math.pow(sigmaFactor * avgEdgeWeight, 2);
        double newWeight = Math.exp((-1.0 * currWeight) / (2 * sigmaSquarred));
        //				MessagePrinter.Print("Weights: " + currWeight + " avg: " + avgEdgeWeight +
        //						" " + sigmaSquarred + " " + newWeight);
				
        v.SetNeighborWeight(nName, newWeight);
      }	
    }
  }
	
  private double GetAverageEdgeWeightSqrt() {
    int totalEdges = 0;
    double totalDistance = 0;

    for (String vName : this._vertices.keySet()) {
      Vertex2 v = this._vertices.get(vName);
      TObjectDoubleIterator nIter = v.GetNeighbors().iterator();
      while (nIter.hasNext()) {
        nIter.advance();
        ++totalEdges;

        // we assume that the currently set distances are distance squares,
        // so we need to take sqrt before averaging.
        totalDistance += Math.sqrt(v.GetNeighborWeight((String) nIter.key()));
      }	
    }

    return ((1.0 * totalDistance) / totalEdges);
  }
	
  // This method makes sure that total seed injection scores for each
  // label is the same. This is useful when the classes are unbalanced.
  // Class prior normalization (CPN) makes sure that the less frequent classes
  // are injected with higher scores compared to the more frequent classes.
  //
  // CPN is different from Class Mass Normalization (CMN) as CMN is used post
  // classification, while CPN is used before the classification process is
  // started.
  public void ClassPriorNormalization() {
    TObjectDoubleHashMap labels = new TObjectDoubleHashMap();
		
    // compute class weight sum
    TObjectDoubleHashMap classSeedSum = new TObjectDoubleHashMap();
    Iterator<String> viter0 = this._vertices.keySet().iterator();
    while (viter0.hasNext()) {
      String vName = viter0.next();
      Vertex2 v = _vertices.get(vName);
      if (v.IsSeedNode()) {
        TObjectDoubleIterator injLabIter =
        		this._labelManager.getLabelScores(v.GetInjectedLabelScores()).iterator();
        while (injLabIter.hasNext()) {
          injLabIter.advance();
          double currVal = classSeedSum.containsKey(injLabIter.key()) ?
            classSeedSum.get(injLabIter.key()) : 0;
          classSeedSum.put(injLabIter.key(), currVal + injLabIter.value());
					
          // add the label to the list of labels
          if (!labels.containsKey(injLabIter.key())) {
            labels.put(injLabIter.key(), 1.0);
          }
        }
      }
    }
		
    double[] seedWeightSums = classSeedSum.getValues();
    double maxSum = -1;
    for (int wsi = 0; wsi < seedWeightSums.length; ++wsi) {
      if (seedWeightSums[wsi] > maxSum) {
        maxSum = seedWeightSums[wsi];
      }
    }
		
    TObjectDoubleHashMap seedAmpliFactor = new TObjectDoubleHashMap();
    TObjectDoubleIterator wIter = classSeedSum.iterator();
    while (wIter.hasNext()) {
      wIter.advance();
      seedAmpliFactor.put(wIter.key(), maxSum / wIter.value());
      System.out.println("Label: " + wIter.key() +
                         " ampli_factor: " + seedAmpliFactor.get(wIter.key()));
    }
		
    // now multiply injected scores with amplification factors
    Iterator<String> viter = this._vertices.keySet().iterator();
    while (viter.hasNext()) {
      String vName = viter.next();
      Vertex2 v = _vertices.get(vName);
      if (v.IsSeedNode()) {				
        TObjectDoubleIterator injLabIter =
        		this._labelManager.getLabelScores(v.GetInjectedLabelScores()).iterator(); 
        while (injLabIter.hasNext()) {
          injLabIter.advance();
					
          double currVal = injLabIter.value();
          injLabIter.setValue(currVal * seedAmpliFactor.get(injLabIter.key()));
        }
      }
    }
  }

  // initialize estimated scores with uniform scores for all labels
  public void SetEstimatedScoresToUniformLabelPrior(Vertex2 v) {
    Random r = new Random(1000);

    TObjectDoubleIterator lIter = _labels.iterator();
    double perLabScore = 1.0 / _labels.size();

    while (lIter.hasNext()) {
      lIter.advance();
      // v.SetEstimatedLabelScore((String) lIter.key(), perLabScore);
			
      // (sparse) random initialization
      if (r.nextBoolean()) {
        v.SetEstimatedLabelScore((String) lIter.key(), r.nextFloat());
      }
    }
  }
	
  // load the gold labels for some or all of the nodes from the file
  public void SetGoldLabels(String goldLabelsFile) {
    try {
      BufferedReader gbr = new BufferedReader(new FileReader(goldLabelsFile));
      String line;
      while ((line = gbr.readLine()) != null) {
        String[] fields = line.split("\t");
        assert (fields.length == 3);
				
        Vertex2 v = this._vertices.get(fields[0]);
        if (v != null) {
          // int li = labelAlphabet_.lookupIndex(fields[1], true);
          // v.SetGoldLabel(Integer.toString(li), Double.parseDouble(fields[2]));
          v.SetGoldLabel(fields[1], Float.parseFloat(fields[2]));
          
          // System.out.println("GOLD_LABEL: " + v.GetName() + " " +
          //		fields[1] + " " +
          //		this._labelManager.lookupStringLabel(fields[1], false));
        }
      }
      gbr.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
	
  // calculate the random walk probabilities i.e. injection, continuation and
  // termination probabilities for each node
  public void CalculateRandomWalkProbabilities(double beta) {
    Iterator<String> viter = this._vertices.keySet().iterator();
    int totalZeroEntropyNeighborhoodNodes = 0;
    while (viter.hasNext()) {
      String vName = viter.next();
      boolean isZeroEntropy = this.GetVertex2(vName).CalculateRWProbabilities(beta);
      if (isZeroEntropy) {
        ++totalZeroEntropyNeighborhoodNodes;
      }
    }
    MessagePrinter.Print("ZERO ENTROPY NEIGHBORHOOD Heuristic adjustment used for " +
                         totalZeroEntropyNeighborhoodNodes + " nodes!");
  }
	
  public void SetSeedInjected() {
    this._isSeedInjected = true;
  }
	
  public boolean IsSeedInjected() {
    return (this._isSeedInjected);
  }

  public static double GetAccuracy(Graph2 g) {
   double doc_mrr_sum = 0;
   int correct_doc_cnt = 0;
   int total_doc_cnt = 0;

   Iterator<String> vIter = g._vertices.keySet().iterator();			
   while (vIter.hasNext()) {
     String vName = vIter.next();
     Vertex2 v = g._vertices.get(vName);
				
     if (v.IsTestNode()) {
       double mrr = v.GetMRR();
       ++total_doc_cnt;
       doc_mrr_sum += mrr;
       if (mrr == 1) {
         ++correct_doc_cnt;
       }
     }
   }

   return ((1.0 * correct_doc_cnt) / total_doc_cnt);
 }

 public static double GetAverageTestMRR(Graph2 g) {
   double doc_mrr_sum = 0;
   int total_doc_cnt = 0;

   Iterator<String> vIter = g._vertices.keySet().iterator();
   while (vIter.hasNext()) {
    String vName = vIter.next();
    Vertex2 v = g._vertices.get(vName);

    if (v.IsTestNode()) {
     double mrr = v.GetMRR();
     ++total_doc_cnt;
     doc_mrr_sum += mrr;
    }
   }

   // System.out.println("MRR Computation: " + doc_mrr_sum + " " +
   // total_doc_cnt);
   return ((1.0 * doc_mrr_sum) / total_doc_cnt);
 }
}
