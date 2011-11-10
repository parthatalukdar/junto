package upenn.junto.graph

import akka.actor.{Actor, ActorRef, PoisonPill}
import Actor._
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
  case class SetNeighbors(neighbors: TObjectDoubleHashMap[ActorRef]) extends MadMessage
  case object PushLabels extends MadMessage
  case object DonePushing extends MadMessage
  case object UpdateEstimatedLabels extends MadMessage
  case class DoneUpdating(delta: Double, mrr: Double) extends MadMessage
  case class Advance(worker: ActorRef) extends MadMessage

  case class NeighborInfo(
    neighbor: ActorRef, 
    nPcontinue: Double, 
    nWeightOfRecipent: Double,
    labelDist: TObjectDoubleHashMap[String]
  )

  /**
   * Entry point into actor-based MAD: create the actors for the graph and its
   * vertices and start the work.
   */
  def apply (graph: Graph, mu1: Double, mu2: Double, mu3: Double, maxIters: Int) {
    val clock = actorOf(new Clock(maxIters)).start()
    val madGraph = actorOf(new MadGraph(clock, graph, mu1, mu2, mu3)).start()
    madGraph !! NextStep
    println("Done!")
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
      graph.vertices.values.toIndexedSeq.map {
        v: Vertex => {
          v.SetInjectedLabelScore(Constants.GetDummyLabel, 0.0)
          val vertexActorRef = actorOf(new MadVertex(self, v.name, v.pinject, v.pcontinue, v.pabandon,
                                                     mu1, mu2, mu3, v.neighbors.size,
                                                     normalizationConstants.get(v.name),
                                                     v.injectedLabels, v.estimatedLabels,
                                                     v.isTestNode, v.goldLabels))
          vertexActorRef.start()
          (v.name, vertexActorRef)
        }
      }.toMap

    namesToActorVertices.foreach {
      case(vName, vertexActorRef) => {
        val neighborsAsStrings = graph.vertices.get(vName).neighbors
        val neighborsAsActorRefs = new TObjectDoubleHashMap[ActorRef]
        for (neighName <- neighborsAsStrings.keySet)
          neighborsAsActorRefs.put(namesToActorVertices(neighName), 
                                   neighborsAsStrings.get(neighName))

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
          totalDeltaLabelDiff = 0
          correctNodeCount = 0
          clock ! Advance(self)
        }
      }

      // Tell all the vertices to stop and then stop itself.
      case Stop => 
        vertices.foreach(vertex => vertex ! PoisonPill)
        self.stop()

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
    injectedLabels: TObjectDoubleHashMap[String],
    var estimatedLabels: TObjectDoubleHashMap[String],
    isTestNode: Boolean,
    goldLabels: TObjectDoubleHashMap[String]
  ) extends Actor {

    import upenn.junto.util._
    import java.util.ArrayList

    var neighbors: TObjectDoubleHashMap[ActorRef] = _
    var newLabelDist = new TObjectDoubleHashMap[String]

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
          neighRef ! NeighborInfo(self, pcontinue, neighbors.get(neighRef), estimatedLabels)

      // Receive a message from a neighbor and perform the relevant computation.
      case NeighborInfo(neighbor: ActorRef, nPcontinue, nWeightOfRecipient, nLabelDist) => {
        val mult = pcontinue * neighbors.get(neighbor) + nPcontinue * nWeightOfRecipient
        ProbUtil.AddScores(newLabelDist, mult*mu2, nLabelDist)
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
      ProbUtil.AddScores(newLabelDist, pinject*mu1, injectedLabels)

      // Add in the dummy label contribution
      ProbUtil.AddScores(newLabelDist, pabandon*mu3, Constants.GetDummyLabelDist)

      // Normalize by M_ii
      ProbUtil.DivScores(newLabelDist, miiNormalization)

      // Calculate the delta from the previous estimated label distribution
      val deltaLabelDiff = 
        ProbUtil.GetDifferenceNorm2Squarred(estimatedLabels, 1.0, newLabelDist, 1.0)

      // Swap in the new distribution and clear newLabelDist for the next round
      estimatedLabels = new TObjectDoubleHashMap[String](newLabelDist)
      newLabelDist.clear

      val mrr =
        if (isTestNode) {
          val sortedMap: List[ObjectDoublePair] = 
            CollectionUtil.ReverseSortMap(estimatedLabels).toList.filter(_.GetLabel != Constants.GetDummyLabel)
          val goldRank = sortedMap.indexWhere(pair => goldLabels.containsKey(pair.GetLabel))
          if (goldRank > -1) 1.0/(goldRank + 1.0)
          else 0.0
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
          self.stop()
        } else {
          worker ! NextStep
        }
    }

  }

}


