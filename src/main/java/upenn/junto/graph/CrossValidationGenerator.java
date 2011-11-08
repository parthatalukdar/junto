package upenn.junto.graph;

import upenn.junto.util.ObjectDoublePair;
import upenn.junto.util.Constants;
import upenn.junto.util.CollectionUtil;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;

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
    Iterator vIter = g.vertices().keySet().iterator();
    while (vIter.hasNext()) {
      Vertex v = g.vertices().get(vIter.next());
			
      // nodes without feature prefix and those with at least one
      // gold labels are considered valid instances
      if (!v.name().startsWith(Constants.GetFeatPrefix()) &&
          v.goldLabels().size() > 0) {
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
        v.setIsSeedNode(true);
				
        // we expect that the gold labels for the node has already been
        // set, we only need to copy them as injected labels
        TObjectDoubleIterator goldLabIter = v.goldLabels().iterator();
        while (goldLabIter.hasNext()) {
          goldLabIter.advance();
          v.SetInjectedLabelScore((String) goldLabIter.key(), goldLabIter.value());
        }
      } else {
        v.setIsTestNode(true);
      }
    }
		
    //		// for sanity check, count the number of train and test nodes
    //		int totalTrainNodes = 0;
    //		int totalTestNodes = 0;
    //		for (int vi = 0; vi < totalInstances; ++vi) {
    //			Vertex v = (Vertex) sortedRandomInstances.get(vi).GetLabel();
    //			if (v.isSeedNode()) {
    //				++totalTrainNodes;
    //			}
    //			if (v.isTestNode()) {
    //				++totalTestNodes;
    //			}
    //		}
    //		MessagePrinter.Print("Total train nodes: " + totalTrainNodes);
    //		MessagePrinter.Print("Total test nodes: " + totalTestNodes);
  }
}
