package upenn.junto.graph;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import upenn.junto.type.RyanFeatureVector;
import upenn.junto.util.MessagePrinter;

public class BuildGraphFromNodeFactoredData {

  public static Graph Build(Graph g,
                            String graphFile,
                            String seedFile,
                            int maxSeedsPerClass,
                            String testFile,
                            double beta,
                            boolean isDirected) {
    // build the graph itself
    g = LoadGraph(g, graphFile, seedFile, isDirected);
		
    //		// keep only top K neighbors: kNN
    //		if (keepTopKNeighbors != Integer.MAX_VALUE) {
    //			g.KeepTopKNeighbors(keepTopKNeighbors);
    //		}

    // Inject seed labels
    g = InjectSeedLabels(g, seedFile, maxSeedsPerClass);
		
    // Mark all test nodes which will be used
    // during evaluation.
    if (testFile != null) {
      g = MarkTestNodes(g, testFile);
    }
		
    // calculate random walk probabilities for each node.
    Iterator<String> viter = g._vertices.keySet().iterator();
    while (viter.hasNext()) {
      String vName = viter.next();
      g.GetVertex(vName).CalculateRWProbabilities(beta);
    }
		
    return (g);
  }
	
  public static Graph LoadGraph(Graph g, String graphFile,
                                String seedFile, boolean isDirected) {
    try {
      // create a graph with vertices, edges will be added later.
      BufferedReader bfr = new BufferedReader(new FileReader(seedFile));
      String line;
      while ((line = bfr.readLine()) != null) {				
        // id label score
        String[] fields = line.split("\t");

        // create a new node
        Vertex v = g.AddVertex(fields[0], fields[1],
                               Double.parseDouble(fields[2]));
      }
      bfr.close();

      int totalZeroWeightEdgesSkipped = 0;
			
      // now add the edges and set their weights
      BufferedReader br = new BufferedReader(new FileReader(graphFile));
      while ((line = br.readLine()) != null) {
        // source,node_1,sim_1,...,node_n,sim_n
        String[] fields = line.split(",");
        //				if (fields.length != 3) {
        //					MessagePrinter.Print("Skipping line: " + line);
        //					continue;
        //				}

        String srcNodeName = fields[0];
        int totalEntries = fields.length;
        int totalCurrentNeighbors = 0;
        for (int ni = 1; ni < totalEntries; ni += 2) {
          String trgNodeName = fields[ni];
          int edgeWeight = Integer.parseInt(fields[ni + 1]);
					
          if (edgeWeight <= 0) {
            ++totalZeroWeightEdgesSkipped;
            continue;
          }
          ++totalCurrentNeighbors;
          //					if (totalCurrentNeighbors > keepTopKNeighbors) {
          //						break;
          //					}
					
          Vertex sv = g.AddVertex(srcNodeName, "");
          Vertex tv = g.AddVertex(trgNodeName, "");
					
          // source <-- target
          sv.AddNeighbor(trgNodeName, edgeWeight);
					
          // add the reverse edge
          if (!isDirected) {
            tv.AddNeighbor(srcNodeName, edgeWeight);
          }
        }								
      }
      br.close();

      MessagePrinter.Print("Total edges with zero weight skipped: " +
                           totalZeroWeightEdgesSkipped);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }
	
  private static Graph InjectSeedLabels(Graph g, String seedFile, int maxSeedsPerClass) {
    try {
      TObjectIntHashMap seedsPerClassCount = new TObjectIntHashMap();
      BufferedReader br = new BufferedReader(new FileReader(seedFile));
      String line;
      int totalSeeds = 0;
      while ((line = br.readLine()) != null) {
        line.trim();
        // node_idx seed_label
        String[] fields = line.split("\t");
        assert(fields.length <= 3);
				
        // consider only positive seed label scores.
        if (fields.length == 3 &&
            Double.parseDouble(fields[2]) <= 0) {
          continue;
        }
				
        if (!seedsPerClassCount.containsKey(fields[1])) {
          seedsPerClassCount.put(fields[1], 0);
        }

        // a vertex is either a seed node or a test node
        Vertex v = g._vertices.get(fields[0]);
        if (seedsPerClassCount.get(fields[1]) < maxSeedsPerClass) {
          double injVal = 1.0;
          if (fields.length == 3) {
            injVal = Double.parseDouble(fields[2]);
          }
          v.SetInjectedLabelScore(fields[1], injVal);
          v.SetSeedNode();
          v.ResetTestNode();
					
          seedsPerClassCount.increment(fields[1]);
          ++totalSeeds;
        } else {
          v.SetTestNode();
        }
      }
      br.close();
			
      System.out.println("Total seed nodes: " + totalSeeds);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }
	
  private static Graph MarkTestNodes(Graph g, String testFile) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(testFile));
      String line;
      int cnt = 0;
      while ((line = br.readLine()) != null) {
        // node_idx label
        String[] fields = line.split("\t");
        if (fields.length != 3) {
          MessagePrinter.PrintAndDie("Invalid test entry: " + line);
        }

        Vertex v = g._vertices.get(fields[0]);
        if (v == null) {
          MessagePrinter.Print("Unknown test node: " + fields[0]);
          continue;
        }
				
        if (v.IsSeedNode()) {
          MessagePrinter.PrintAndDie("Same node marked as seed and test: " + v.GetName());
        }
				
        v.SetGoldLabel(fields[1], Double.parseDouble(fields[2]));
        v.SetTestNode();
        ++cnt;
      }			
      br.close();
			
      System.out.println("Total " + cnt + " nodes marked as test nodes!");
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }
}
