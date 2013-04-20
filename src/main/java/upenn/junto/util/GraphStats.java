package upenn.junto.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;

import upenn.junto.config.ConfigReader;
import upenn.junto.config.GraphConfigLoader;
import upenn.junto.graph.Graph;
import upenn.junto.graph.Vertex;

public class GraphStats {
	private static Logger logger = LogManager.getLogger(GraphStats.class);
    
  // Number of K-shortest paths generated. 
  private static int _kPrime = -1;

  public static void PrintStats(Graph g, String graphStatsFile) {
    try {
      BufferedWriter swr = new BufferedWriter(new FileWriter(graphStatsFile));
      swr.write(PrintStats(g));
      swr.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
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

    for (String vName : g.vertices().keySet()) {
      Vertex v = g.vertices().get(vName);
      ++totalVertices;
			
      int degree = v.GetNeighborNames().length;
      if (degree > maxDegree) { maxDegree = degree; } 
      if (degree < minDegree) { minDegree = degree; } 

      totalEdges += v.neighbors().size();
			
      if (v.isSeedNode()) { ++totalSeedNodes; }
      if (v.isTestNode()) { ++totalTestNodes; }
      if (v.isSeedNode() && v.isTestNode()) { ++totalSeedAndTestNodes; }
    }
		
    String retStr = "Total seed vertices: " + totalSeedNodes + "\n";
    retStr += "Total test vertices: " + totalTestNodes + "\n";
    retStr += "Total seed vertices which are also test vertices: " + totalSeedAndTestNodes + "\n";
    retStr += "Total vertices: " + totalVertices + "\n";
    retStr += "Total edges: " + totalEdges + "\n";
    retStr += "Average degree: " + (1.0 * totalEdges) / totalVertices + "\n";
    retStr += "Min degree: " + minDegree + "\n";
    retStr += "Max degree: " + maxDegree + "\n";

    return (retStr);
  }
	
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
      if (!v.isSeedNode()) {
        continue;
      }
			
      ++totalProcessed;
      if (totalProcessed % 1000 == 0) {
        logger.info("Processed: " + totalProcessed + " curr_dia: " + diameter);
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
    Graph g = GraphConfigLoader.apply(config);
    MessagePrinter.Print(PrintStats(g));
  }
}
