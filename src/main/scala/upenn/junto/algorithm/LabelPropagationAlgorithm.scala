package upenn.junto.algorithm

import java.util.ArrayList
import gnu.trove.map.hash.TObjectDoubleHashMap
import upenn.junto.graph.{Graph, Vertex}
import scala.collection.JavaConversions._

abstract class LabelPropagationAlgorithm (graph: Graph) {

  def run (maxIter: Int, useBipartiteOptimization: Boolean,
           verbose: Boolean, resultList: ArrayList[Map[String,Double]])

  def getObjective (vertex: Vertex): Double

  def getGraphObjective: Double =
    graph.vertices.keySet.iterator.foldLeft(0.0)(
      (obj, vName) => obj + getObjective(graph.vertices.get(vName))
    )


}
