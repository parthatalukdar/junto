package upenn.junto.graph;

import upenn.junto.type.ObjectDoublePair;
import upenn.junto.util.Constants;
import upenn.junto.util.CollectionUtil;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class CrossValidationGenerator {
  // seed used to initialize the random number generator
  static long _kDeterministicSeed = 100;
	
  public static void Split(Graph g, double trainFract) {		
    Random r = new Random(_kDeterministicSeed); 
    // Random r = new Random(); 
		
    TObjectDoubleHashMap instanceVertices = new TObjectDoubleHashMap();
    Iterator vIter = g._vertices.keySet().iterator();
    while (vIter.hasNext()) {
      Vertex v = g._vertices.get(vIter.next());
			
      // nodes without feature prefix and those with at least one
      // gold labels are considered valid instances
      if (!v.GetName().startsWith(Constants.GetFeatPrefix()) &&
          v.GetGoldLabel().size() > 0) {
        instanceVertices.put(v, r.nextDouble());
      }
    }
		
    ArrayList<ObjectDoublePair> sortedRandomInstances = 
      CollectionUtil.ReverseSortMap(instanceVertices); 

    int totalInstances = sortedRandomInstances.size();
    double totalTrainInstances = Math.ceil(totalInstances * trainFract); 
    for (int vi = 0; vi < totalInstances; ++vi) {
      Vertex v = (Vertex) sortedRandomInstances.get(vi).GetLabel();
			
      // mark train and test nodes
      if (vi < totalTrainInstances) {
        v.SetSeedNode();
				
        // we expect that the gold labels for the node has already been
        // set, we only need to copy them as injected labels
        TObjectDoubleIterator goldLabIter = v.GetGoldLabel().iterator();
        while (goldLabIter.hasNext()) {
          goldLabIter.advance();
          v.SetInjectedLabelScore((String) goldLabIter.key(), goldLabIter.value());
        }
      } else {
        v.SetTestNode();
      }
    }
		
    //		// for sanity check, count the number of train and test nodes
    //		int totalTrainNodes = 0;
    //		int totalTestNodes = 0;
    //		for (int vi = 0; vi < totalInstances; ++vi) {
    //			Vertex v = (Vertex) sortedRandomInstances.get(vi).GetLabel();
    //			if (v.IsSeedNode()) {
    //				++totalTrainNodes;
    //			}
    //			if (v.IsTestNode()) {
    //				++totalTestNodes;
    //			}
    //		}
    //		MessagePrinter.Print("Total train nodes: " + totalTrainNodes);
    //		MessagePrinter.Print("Total test nodes: " + totalTestNodes);
  }
}
