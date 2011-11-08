package upenn.junto.eval;

import upenn.junto.graph.Graph;
import upenn.junto.graph.Vertex;

import java.util.Iterator;

public class GraphEval {

  public static double GetAccuracy(Graph g) {
    double doc_mrr_sum = 0;
    int correct_doc_cnt = 0;
    int total_doc_cnt = 0;

    Iterator<String> vIter = g.vertices().keySet().iterator();			
    while (vIter.hasNext()) {
      String vName = vIter.next();
      Vertex v = g.vertices().get(vName);
			
      if (v.isTestNode()) {
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
	
  public static double GetAverageTestMRR(Graph g) {
    double doc_mrr_sum = 0;
    int total_doc_cnt = 0;

    Iterator<String> vIter = g.vertices().keySet().iterator();			
    while (vIter.hasNext()) {
      String vName = vIter.next();
      Vertex v = g.vertices().get(vName);
			
      if (v.isTestNode()) {
        double mrr = v.GetMRR();
        ++total_doc_cnt;
        doc_mrr_sum += mrr;
      }
    }

    // System.out.println("MRR Computation: " + doc_mrr_sum + " " + total_doc_cnt);
    return ((1.0 * doc_mrr_sum) / total_doc_cnt);
  }
	
  public static double GetAverageTrainMRR(Graph g) {
    double doc_mrr_sum = 0;
    int total_doc_cnt = 0;

    Iterator<String> vIter = g.vertices().keySet().iterator();			
    while (vIter.hasNext()) {
      String vName = vIter.next();
      Vertex v = g.vertices().get(vName);
			
      if (v.isSeedNode()) {
        double mrr = v.GetMRR();
        ++total_doc_cnt;
        doc_mrr_sum += mrr;
      }
    }

    // System.out.println("MRR Computation: " + doc_mrr_sum + " " + total_doc_cnt);
    return ((1.0 * doc_mrr_sum) / total_doc_cnt);
  }
	
  public static double GetRMSE(Graph g) {
    double totalMSE = 0;
    int totalCount = 0;

    Iterator<String> vIter = g.vertices().keySet().iterator();			
    while (vIter.hasNext()) {
      String vName = vIter.next();
      Vertex v = g.vertices().get(vName);
			
      if (v.isTestNode()) {
        totalMSE += v.GetMSE();
        ++totalCount;
      }
    }

    return (Math.sqrt((1.0 * totalMSE) / totalCount));
  }
  
}
