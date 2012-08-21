package upenn.junto.graph

import akka.actor._
import scala.collection.JavaConversions._

import gnu.trove.map.hash.TObjectDoubleHashMap

import upenn.junto.algorithm._
import upenn.junto.util.Constants


/**
 * An implementation of MAD using Akka actors. Still preliminary -- has some things
 * to add in yet, and round out, but the core algorithm is working properly. There
 * are many things that can surely be improved, especially to reducing the amount
 * of blocking employed here.
 *
 * @author Jason Baldridge
 */
object MadGraphRunner {

  sealed trait MadMessage
  case object NextStep extends MadMessage
  case object Stop extends MadMessage
  case class SetNeighbors(neighbors: Map[ActorRef, Double]) extends MadMessage
  case object PushLabels extends MadMessage
  case class DoneUpdating(delta: Double, mrr: Double) extends MadMessage
  case class Advance(worker: ActorRef) extends MadMessage

  case class NeighborInfo(
    neighbor: ActorRef, 
    nPcontinue: Double, 
    nWeightOfRecipent: Double,
    labelDist: Map[String, Double]
  )

  /**
   * Entry point into actor-based MAD: create the actors for the graph and its
   * vertices and start the work.
   */
  def apply (graph: Graph, mu1: Double, mu2: Double, mu3: Double, maxIters: Int) {
    val system = ActorSystem("MadRunner")
    val clock = system.actorOf(Props(new Clock(maxIters)), name="clock")
    val madGraph = system.actorOf(Props(new MadGraph(clock, graph, mu1, mu2, mu3)), name="graph")
    madGraph ! NextStep
  }

  /**
   * An actor that controls the entire graph. Stores all the vertex actors
   * and dispatches work for them to do, and communicates with the Clock to
   * keep each iteration of work separate.
   */
  class MadGraph (clock: ActorRef, graph: Graph, mu1: Double, mu2: Double, mu3: Double) 
  extends Actor {

    val normalizationConstants = 
      MadHelper.computeNormalizationConstants(graph, mu1, mu2, mu3)

    AdsorptionHelper.prepareGraph(graph)

    val namesToActorVertices: Map[String, ActorRef] = 
      graph
        .vertices
        .values
        .toIndexedSeq
        .zipWithIndex
        .map { case(v, index) => {
          v.SetInjectedLabelScore(Constants.GetDummyLabel, 0.0)
          val vertexActorRef = context.actorOf(
            Props(new MadVertex(self, v.name, v.pinject, v.pcontinue, v.pabandon,
                          mu1, mu2, mu3, v.neighbors.size,
                          normalizationConstants.get(v.name),
                          TroveToScalaMap(v.injectedLabels), 
                          TroveToScalaMap(v.estimatedLabels),
                          v.isTestNode, 
                          TroveToScalaMap(v.goldLabels))), name = "vertex"+index)
          (v.name, vertexActorRef)
        }}
        .toMap

    namesToActorVertices.foreach {
      case(vName, vertexActorRef) => {
        val neighborsAsStrings = graph.vertices.get(vName).neighbors
        val neighborsAsActorRefs = neighborsAsStrings.keySet.map {
          neighName => (namesToActorVertices(neighName) -> neighborsAsStrings.get(neighName))
        } toMap

        vertexActorRef ! SetNeighbors(neighborsAsActorRefs)
      }
    }
    
    val vertices = namesToActorVertices.values.toIndexedSeq
    val numTestNodes = graph.vertices.values.count(_.isTestNode).toDouble

    var numBusyVertices: Int = _
    var totalDeltaLabelDiff = 0.0
    var correctNodeCount = 0.0

    def receive = {

      // Broadcast to all the vertices that they should push their
      // labels to their neighbors
      case NextStep =>
        numBusyVertices = vertices.length
        vertices.foreach(vertex => vertex ! PushLabels)

      // Receive a message from a vertex about the results of its
      // updating its previous iteration estimated label distribution
      // to the new one.
      case DoneUpdating(delta, mrr) => {
        numBusyVertices -= 1
        totalDeltaLabelDiff += delta
        correctNodeCount += (if (mrr==1.0) 1.0 else 0.0)

        if (numBusyVertices == 0) {
          println("Delta: " + totalDeltaLabelDiff)
          println("Acc: " + correctNodeCount/numTestNodes)
          totalDeltaLabelDiff = 0.0
          correctNodeCount = 0.0
          clock ! Advance(self)
        }
      }

      // Tell all the vertices to stop and then stop itself.
      case Stop => 
        vertices.foreach(vertex => vertex ! PoisonPill)
        context.system.shutdown

    }

  }


  /**
   * An actor for a vertex. Knows how to send its fellow vertices the information
   * they need to perform MAD updates, and -- of course -- what to do with the
   * information it receives from fellow vertices.
   */
  class MadVertex (
    controller: ActorRef,
    name: String, pinject: Double, pcontinue: Double, pabandon: Double,
    mu1: Double, mu2: Double, mu3: Double, numNeighbors: Int,
    miiNormalization: Double,
    injectedLabels: Map[String, Double],
    var estimatedLabels: Map[String, Double],
    isTestNode: Boolean,
    goldLabels: Map[String, Double]
  ) extends Actor {

    import upenn.junto.util._
    import java.util.ArrayList

    var neighbors: Map[ActorRef, Double] = _
    val newLabelDist = new collection.mutable.HashMap[String, Double]

    var numNeighborMessagesReceived = 0

    def receive = {

      // Set the neighbors of this node, as a map from ActorRefs for
      // the neighbors to their weights.
      case SetNeighbors (neighborActorRefs) => 
        neighbors = neighborActorRefs

      // Push the label distribution and relevant other factors so
      // that one's neighbors can compute their updates.
      case PushLabels =>
        for (neighRef <- neighbors.keySet)
          neighRef ! NeighborInfo(self, pcontinue, neighbors(neighRef), estimatedLabels)

      // Receive a message from a neighbor and perform the relevant computation.
      case NeighborInfo(neighbor: ActorRef, nPcontinue, nWeightOfRecipient, nLabelDist) => {
        val mult = pcontinue * neighbors(neighbor) + nPcontinue * nWeightOfRecipient
        DistUtil.addScores(newLabelDist, mult*mu2, nLabelDist)

	numNeighborMessagesReceived += 1

	// When all neighbors have reported their distributions, wrap
	// up the update and report back to the controller.
	if (numNeighborMessagesReceived == numNeighbors) {
	  val (deltaLabelDiff, mrr) = updateEstimatedLabels
          numNeighborMessagesReceived = 0
	  controller ! DoneUpdating(deltaLabelDiff, mrr)	  
	}
      }
        
    }

    // Once all labels have been pushed, finalizes the update, which
    // involves vertex-internal computations with the injected
    // labels and the prior distribution (the dummy distribution).
    def updateEstimatedLabels = {

      // Add in the injected label contribution
      DistUtil.addScores(newLabelDist, pinject*mu1, injectedLabels)

      // Add in the dummy label contribution
      DistUtil.addScores(newLabelDist, pabandon*mu3, DistUtil.DummyLabelDist)

      // Normalize by M_ii
      newLabelDist.foreach { 
        case(k,v) => newLabelDist += (k -> v/miiNormalization)
      }

      // Calculate the delta from the previous estimated label distribution
      val deltaLabelDiff = 
        DistUtil.getDifferenceNorm2Squared(estimatedLabels, 1.0, newLabelDist, 1.0)

      // Swap in the new distribution and clear newLabelDist for the next round
      estimatedLabels = newLabelDist.toMap
      newLabelDist.clear


      val mrr =
        if (isTestNode) {
          val sortedMap: List[(String,Double)] = 
            estimatedLabels.toList.sortBy(_._2).reverse.filter(_._1 != Constants.GetDummyLabel)

          val goldRank = sortedMap.indexWhere(pair => goldLabels.containsKey(pair._1))

          if (goldRank > -1) 1.0/(goldRank + 1.0) else 0.0
        } else {
          0.0
        }
      (deltaLabelDiff,mrr)
    }


  }

  /**
   * A time keeper that makes sure that each iteration is distinct from the next, by
   * ensuring that the graph controller (MadGraph actor) doesn't broadcast the next
   * label pushing event until all vertices are done updating.
   */
  class Clock (maxSteps: Int) extends Actor {
    var currentStep = 0

    def receive = {
      case Advance(worker) =>
        currentStep += 1
        println("Step: " + currentStep)
        if (currentStep == maxSteps) {
          worker ! Stop
          context.stop(self)
        } else {
          worker ! NextStep
        }
    }

  }

}

/**
 * Converts a Trove map to a Scala map.
 */
object TroveToScalaMap {
  def apply[T] (tmap: TObjectDoubleHashMap[T]): Map[T,Double] =
    tmap.keySet.map(key => (key -> tmap.get(key))).toMap
}

/**
 * Utilities for working with probability distributions stored
 * as Map[String, Double] objects.
 */
object DistUtil {

  val DummyLabelDist = Map(Constants.GetDummyLabel -> 1.0)

  def addScores (accumulator: collection.mutable.Map[String, Double],
                 multiplier: Double,
                 addDist: Map[String,Double]) {
    addDist.mapValues(_*multiplier).foreach {
      case(k,v) => 
        val updatedValue = v + accumulator.getOrElse(k, 0.0)
        accumulator += (k -> updatedValue)
    } 
  }


  def getDifferenceNorm2Squared(
    m1: Map[String, Double],
    m1Mult: Double, 
    m2: collection.mutable.Map[String, Double], 
    m2Mult: Double
  ) = {
    val differences = (m1.keys ++ m2.keys).map(k => m1.getOrElse(k, 0.0) - m2.getOrElse(k, 0.0))
    math.sqrt(differences.map(x => x*x).sum)
  }

}
