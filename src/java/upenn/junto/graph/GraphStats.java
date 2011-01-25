package upenn.junto.graph;

import gnu.trove.TObjectIntHashMap;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import upenn.junto.util.MessagePrinter;

import org.jgrapht.GraphPath;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.alg.KShortestPaths;

public class GraphStats {
  // Number of K-shortest paths generated. 
  private static int _kPrime = -1;

  public static void PrintStats(Graph g, String graphStatsFile) {
    try {
      BufferedWriter swr = new BufferedWriter(new FileWriter(graphStatsFile));
      swr.write(PrintStats(g));
      swr.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
	
  public static String PrintStats(Graph g) {
    int totalSeedNodes = 0;
    int totalTestNodes = 0;
    int totalSeedAndTestNodes = 0;
    int totalEdges = 0;
    int totalVertices = 0;
    int maxDegree = Integer.MIN_VALUE;
    int minDegree = Integer.MAX_VALUE;

    for (String vName : g._vertices.keySet()) {
      Vertex v = g._vertices.get(vName);
      ++totalVertices;
			
      int degree = v.GetNeighborNames().length;
      if (degree > maxDegree) { maxDegree = degree; } 
      if (degree < minDegree) { minDegree = degree; } 

      totalEdges += v.GetNeighbors().size();
			
      if (v.IsSeedNode()) { ++totalSeedNodes; }
      if (v.IsTestNode()) { ++totalTestNodes; }
      if (v.IsSeedNode() && v.IsTestNode()) { ++totalSeedAndTestNodes; }
    }
		
    //		DefaultDirectedWeightedGraph<Vertex,DefaultWeightedEdge> g2 = g.GetJGraphTGraph();

    String retStr = "Total seed vertices: " + totalSeedNodes + "\n";
    retStr += "Total test vertices: " + totalTestNodes + "\n";
    retStr += "Total seed vertices which are also test vertices: " + totalSeedAndTestNodes + "\n";
    retStr += "Total vertices: " + totalVertices + "\n";
    retStr += "Total edges: " + totalEdges + "\n";
    retStr += "Average degree: " + (1.0 * totalEdges) / totalVertices + "\n";
    retStr += "Min degree: " + minDegree + "\n";
    retStr += "Max degree: " + maxDegree + "\n";
    // retStr += GetDiameter(g2);
    //		retStr += "Strongly connected: " +
    //				(IsStronglyConnected(g) ? "true" : "false") + "\n";

    return (retStr);
  }
	
  //	private static boolean IsStronglyConnected(Graph g) {
  //		DefaultDirectedWeightedGraph<Vertex,DefaultWeightedEdge> g2 =
  //			g.GetJGraphTGraph();		
  //		StrongConnectivityInspector<Vertex,DefaultWeightedEdge> sci =
  //			new StrongConnectivityInspector<Vertex,DefaultWeightedEdge>(g2);
  //		return (sci.isStronglyConnected());
  //	}
	
  private static String GetDiameter(
                                    DefaultDirectedWeightedGraph<Vertex,DefaultWeightedEdge> g) {
    String retDiaReport = "";
		
    //		HashMap<Vertex,KShortestPaths<Vertex,DefaultWeightedEdge>> kShortestPathMap =
    //			new HashMap<Vertex,KShortestPaths<Vertex,DefaultWeightedEdge>>();
		
    boolean isConnected = true;
    int diameter = -1;
		
    int totalProcessed = 0;
		
    Iterator<Vertex> vIter = g.vertexSet().iterator();
    while (vIter.hasNext()) {
      Vertex v = vIter.next();
      if (!v.IsSeedNode()) {
        continue;
      }
			
      ++totalProcessed;
      if (totalProcessed % 1000 == 0) {
        MessagePrinter.Print("Processed: " + totalProcessed + " curr_dia: " + diameter);
      }
			
      KShortestPaths<Vertex,DefaultWeightedEdge> ksp = new KShortestPaths(g, v, 1);
      // kShortestPathMap.put(v, new KShortestPaths(g, v, _kPrime));
			
      Iterator<Vertex> vIter2 = g.vertexSet().iterator();
      while (vIter2.hasNext()) {
        Vertex nv = vIter2.next();
				
        // skip self comparison
        if (v.equals(nv)) { continue; }

        List<GraphPath<Vertex,DefaultWeightedEdge>> paths = ksp.getPaths(nv);
				
        if (paths == null) { isConnected = false; }
        else  if (paths.get(0).getEdgeList().size() > diameter) {
          diameter = paths.get(0).getEdgeList().size();
        }
      }
    }
		
    retDiaReport += "Connected(from_seed_nodes): " + (isConnected ? "true" : "false") + "\n";
    retDiaReport += "Diameter(from_seed_nodes): " + diameter + "\n";
		
    return (retDiaReport);
  }
  
	
  public static void main(String[] args) {
    Hashtable config = ConfigReader.read_config(args);

    // load the graph
    Graph g = GraphLoader.LoadGraph(config);
    MessagePrinter.Print(PrintStats(g));
  }
}
