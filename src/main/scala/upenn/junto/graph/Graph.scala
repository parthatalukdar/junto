package upenn.junto.graph

import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.iterator.TObjectDoubleIterator
import java.util.{ArrayList,HashMap,Iterator}
import upenn.junto.util._
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.log4j.Logging

class Graph extends Logging {

  val vertices = new HashMap[String, Vertex]
  val labels = new TObjectDoubleHashMap[String]
  var isSeedInjected = false

  def AddVertex (name: String, label: String): Vertex = AddVertex(name, label, 1.0)

  def AddVertex (name: String, label: String, weight: Double): Vertex = {
    val v = vertices.get(name)
		
    // Add if the vertex is already not preset. Or, if the vertex
    // is present but it doesn't have a valid label assigned, then
    // update its label
    if (v == null)
      vertices.put(name, Vertex(name, label, weight))
    else
      v.setGoldLabel(label, weight)

    vertices.get(name)
  }
	
  def GetVertex (name: String) = {
    if (!vertices.containsKey(name)) 
      new RuntimeException("Node " + name + " doesn't exist!")
    vertices.get(name)
  }
	
  // remove nodes if they are not connected to a certain number
  // of neighbors.
  def PruneLowDegreeNodes (minNeighborCount: Int) {
    var totalPruned = 0

    val vIter: Iterator[String] = vertices.keySet.iterator
    while (vIter.hasNext) {
      val v = vertices.get(vIter.next)

      // if a node has lower than certain number of neighbors, then
      // that node can be filtered out, also the node should be a
      // feature node.
      if (v.neighbors.keySet.size <= minNeighborCount) {
        // make the node isolated
        for (neighName <- v.GetNeighborNames) {
          v.neighbors.remove(neighName)
          vertices.get(neighName).neighbors.remove(v.name)
        }
				
        // now remove the vertex from the vertex list
        vIter.remove
        totalPruned += 1
      }
    }
		
    logger.info("Total nodes pruned: " + totalPruned)
  }
	
  // keep only K highest scoring neighbors
  def KeepTopKNeighbors (kValue: Int) {
    var totalEdges = 0
    for (vName <- vertices.keySet) {
      val v = vertices.get(vName)
			
      val neighWeights: TObjectDoubleHashMap[String] = new TObjectDoubleHashMap[String]
      for (neighName <-  v.GetNeighborNames)
        neighWeights.put(neighName, v.GetNeighborWeight(neighName))

      // now sort the neighbors
      val sortedNeighList: ArrayList[ObjectDoublePair] = 
        CollectionUtil.ReverseSortMap(neighWeights)
			
      // Since the array is reverse sorted, the highest scoring
      // neighbors are listed first, which we want to retain. Hence,
      // remove everything after after the top-K neighbors.
      for (neighLabelScore <- sortedNeighList.drop(kValue))
        v.neighbors.remove(neighLabelScore.GetLabel)

      totalEdges += v.neighbors.keySet.size
    }
		
    // now make all the directed edges undirected.
    for (vName <- vertices.keySet) {
      val v = vertices.get(vName)

      for (neighName <-  v.GetNeighborNames) {
        val neigh = vertices.get(neighName)
				
        // if the reverse edge is not present, then add it
        if (neigh.GetNeighborWeight(v.name) == 0.0) {
          neigh.setNeighbor(v.name, v.GetNeighborWeight(neighName))
          totalEdges += 1
        }
      }
    }
		
    logger.info("Total edges: " + totalEdges)
  }
	

  def SetGaussianWeights (sigmaFactor: Double) {
    // get the average edge weight of the graph
    val avgEdgeWeight = GetAverageEdgeWeightSqrt
		
    for (vName <- vertices.keySet) {
      val v = vertices.get(vName)

      val nIter: TObjectDoubleIterator[String] = v.neighbors.iterator
      while (nIter.hasNext) {
        nIter.advance
        val nName = nIter.key

        // we assume that the currently set distances are distance squares
        val currWeight = nIter.value
        val sigmaSquarred = math.pow(sigmaFactor * avgEdgeWeight, 2)
        val newWeight = math.exp((-1.0 * currWeight) / (2 * sigmaSquarred))
        v.neighbors.put(nName, newWeight)
      }
    }
  }
	
  private def GetAverageEdgeWeightSqrt: Double = {
    var totalEdges = 0
    var totalDistance = 0.0

    for (vName <- vertices.keySet) {
      val v = vertices.get(vName)
      val nIter: TObjectDoubleIterator[String] = v.neighbors.iterator
      while (nIter.hasNext) {
        nIter.advance
        totalEdges += 1

        // we assume that the currently set distances are distance squares,
        // so we need to take sqrt before averaging.
        totalDistance += math.sqrt(v.GetNeighborWeight(nIter.key))
      }	
    }

    totalDistance/totalEdges
  }
	
  // load the gold labels for some or all of the nodes from the file
  def SetGoldLabels (goldLabelsFile: String) {
    for (line <- io.Source.fromFile(goldLabelsFile).getLines) {
      val fields = line.split("\t")
      assert (fields.length == 3)
				
      logger.info(line)
      val v = vertices.get(fields(0))
      if (v != null)
        v.setGoldLabel(fields(1), fields(2).toDouble)
    }
  }
	
  // calculate the random walk probabilities i.e. injection, continuation and
  // termination probabilities for each node
  def CalculateRandomWalkProbabilities (beta: Double) {
    var totalZeroEntropyNeighborhoodNodes = 0

    for (vName <- vertices.keySet) {
      val isZeroEntropy = GetVertex(vName).CalculateRWProbabilities(beta)
      if (isZeroEntropy)
        totalZeroEntropyNeighborhoodNodes += 1
    }
    logger.info("ZERO ENTROPY NEIGHBORHOOD Heuristic adjustment used for " +
                         totalZeroEntropyNeighborhoodNodes + " nodes!")
  }
	
}

object GraphIo extends Logging {
  import java.io.{BufferedWriter,FileWriter}
  
  val kDelim_ = "\t" 

  def saveEstimatedScores (graph: Graph, outputFile: String) {
    var doc_mrr_sum = 0.0
    var correct_doc_cnt = 0
    var total_doc_cnt = 0

    val bw = new BufferedWriter(new FileWriter(outputFile))

    for (vName <- graph.vertices.keySet) {
      val v = graph.vertices.get(vName)
      if (v.isTestNode) {
        val mrr = v.GetMRR
        total_doc_cnt += 1
        doc_mrr_sum += mrr
        if (mrr == 1.0)
          correct_doc_cnt += 1
      }

      bw.write(v.name + kDelim_ +
               CollectionUtil.Map2String(v.goldLabels) + kDelim_ +
               Vertex.getPrettyPrintMap(v.injectedLabels) + kDelim_ +
               Vertex.getPrettyPrintMap(v.estimatedLabels) + kDelim_ +
               v.isTestNode + kDelim_ +
               v.GetMRR + "\n")
    }
    bw.close
    
    // print summary result
    // assert (total_doc_cnt > 0)
    if (total_doc_cnt > 0) {
      logger.info("PRECISION " + correct_doc_cnt.toDouble / total_doc_cnt +
                         " (" + correct_doc_cnt + " correct out of " + total_doc_cnt + ")")
      logger.info("MRR " + doc_mrr_sum.toDouble / total_doc_cnt)
    } else {
      logger.info("Total test instances evaluated: " + total_doc_cnt)
    }
    
  }

}
