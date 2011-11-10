package upenn.junto.graph

import java.util.ArrayList

import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.iterator.TObjectDoubleIterator

import upenn.junto.util.ObjectDoublePair
import upenn.junto.util.RyanAlphabet
import upenn.junto.util.RyanFeatureVector
import upenn.junto.util.Constants
import upenn.junto.util.CollectionUtil
import upenn.junto.util.ProbUtil

import scala.collection.JavaConversions._

object Vertex {

  def apply (name: String): Vertex = apply(name, Constants.GetDummyLabel, 0.0)

  def apply (name: String, label: String): Vertex = apply(name, label, 1.0)

  def apply (name: String, label: String, weight: Double): Vertex = {
    val vertex = new Vertex(name)
    vertex.setGoldLabel(label, weight)
    vertex
  }

  def getPrettyPrintMap (m: TObjectDoubleHashMap[String]): String = getPrettyPrintMap(m, null)

  def getPrettyPrintMap (m: TObjectDoubleHashMap[String], la: RyanAlphabet): String = {		
    val sortedMap: ArrayList[ObjectDoublePair] = CollectionUtil.ReverseSortMap(m)
    var op = ""
    sortedMap.foreach { 
      labelScorePair => 
        var label = labelScorePair.GetLabel.asInstanceOf[String]

        if (la != null) {
          val li = CollectionUtil.String2Integer(label)
          if (li != null)
            label = la.lookupObject(li.intValue).asInstanceOf[String]
        }
        op += " " + label + " " + labelScorePair.GetScore
    }
  
    op.trim
  }


}

class Vertex (val name: String) {

  // probability with which the injected probability
  // should be used.
  var pinject: Double = -1.0   
	
  // probability with which the random walk should be
  // continued to a neighboring vertex. This leads to
  // weighted average label distribution of all neighbors.
  var pcontinue: Double = -1.0 

  // probability with which the random walk is terminated
  // and the dummy label emitted.
  var pabandon: Double = -1.0  

  // set to true when scores on transition going out
  // of the vertex is normalized and is made a probability
  // distribution.
  var isTransitionNormalized = false
	
  // labels & scores which are injected in the node
  // as prior knowledge. Only positive scores are
  // allowed.
  val injectedLabels = new TObjectDoubleHashMap[String]
	
  // labels & their scores estimated by the algorithm.
  // only positive scores are allowed.
  var estimatedLabels = new TObjectDoubleHashMap[String]

  // initialize the estimated labels with dummy label
  estimatedLabels.put(Constants.GetDummyLabel, 1.0)

  // neighbors of the vertex along with edge/association
  // weight
  val neighbors = new TObjectDoubleHashMap[String]
	
  // gold labels (if any of the vertex) optional
  val goldLabels = new TObjectDoubleHashMap[String]

  // set to true if the node is injected with seed labels
  var isSeedNode = false
	
  // set to true if the node is to be included during evaluation
  // by default: false
  var isTestNode = false
	
  // feature representation of the vertex
  //val features = new RyanFeatureVector(-1, -1, null)

  def setGoldLabel (goldLabel: String, weight: Double) {
    if (goldLabel != Constants.GetDummyLabel && goldLabel.length > 0) {
      if (weight == 0.0)
        goldLabels.remove(goldLabel)
      else
        goldLabels.put(goldLabel, weight)
    }
  }
	
  def setNeighbor (neighbor: String, weight: Double) = neighbors.put(neighbor, weight)
	
  def GetNeighborWeight (neighbor: String): Double = 
    if (neighbors.containsKey(neighbor)) neighbors.get(neighbor) else 0.0
	
  def GetNeighborNames = neighbors.keys(new Array[String](0))
	
  def GetInjectedLabelScore (label: String) =
    if (injectedLabels.containsKey(label)) injectedLabels.get(label) else 0.0
	
  def SetInjectedLabelScore (label: String, weight: Double) {
    if (weight != 0.0) {
      injectedLabels.put(label, weight)
      isSeedNode = true
    } else {
      injectedLabels.remove(label)
    }
  }
	
  def RemoveInjectedLabel (label: String) {
    injectedLabels.remove(label)
    isSeedNode = injectedLabels.size != 0
  }
	
  def GetEstimatedLabelScore (label: String) =
    if (estimatedLabels.containsKey(label)) estimatedLabels.get(label) else 0.0
	
  def SetEstimatedLabelScore (label: String, weight: Double) {
    if (weight != 0) 
      estimatedLabels.put(label, weight)
    else 
      estimatedLabels.remove(label)
  }
	
  def SetEstimatedLabelScores (updatedScores: TObjectDoubleHashMap[String]) {
    estimatedLabels.clear
    estimatedLabels = updatedScores
  }
	
  def UpdateEstimatedLabel (label: String, weight: Double) {
    estimatedLabels.adjustOrPutValue(label, weight, weight)
  }
	
  // Calculate random walk based probabilities.
  // For details, see Sec 3 of Talukdar et al, EMNLP 08
  //
  // The method returns true of the node has zero entropy neighborhood.
  def CalculateRWProbabilities (beta: Double): Boolean = {
    val neighborClone = new TObjectDoubleHashMap[String](neighbors)
    ProbUtil.Normalize(neighborClone)
		
    val ent = GetNeighborhoodEntropy(neighborClone)
    var cv = math.log(beta) / math.log(beta + ent)
		
    var isZeroEntropy = false

    var jv = 0.0
    if (injectedLabels.size >= 1) {
      jv = (1 - cv) * Math.sqrt(ent)
			
      // Entropy can be 0 when the seed node is connected to only
      // one other node. This can make the injection probability 0,
      // which is readjusted to 1
      if (jv == 0) {
        isZeroEntropy = true
        jv = 0.99
        cv = 0.01
      }
    }
    val zv = Math.max(cv + jv, 1.0)
		
    pcontinue = cv / zv
    pinject = jv / zv
    pabandon = Math.max(0, 1 - pcontinue - pinject)
		
    isZeroEntropy
  }
	
  def NormalizeTransitionProbability {
    ProbUtil.Normalize(neighbors)
    isTransitionNormalized = true
  }

  def GetNeighborhoodEntropy (map: TObjectDoubleHashMap[String]) = {
    var entropy = 0.0
    val ni = map.iterator
    while (ni.hasNext) {
      ni.advance
      entropy += -1 * ni.value() * Math.log(ni.value()) / Math.log(2)
    }
    entropy
  }
	
  // returns the sum of weights of all edges going out
  // from the node.
  def GetOutEdgeWeightSum: Double = {
    var sum = 0.0
    val ni = neighbors.iterator
    while (ni.hasNext) {
      ni.advance
      sum += ni.value
    }
    sum
  }
	
	
  def GetMRR: Double = {
    val sortedMap: List[ObjectDoublePair] = 
      CollectionUtil.ReverseSortMap(estimatedLabels).toList.filter(_.GetLabel != Constants.GetDummyLabel)
    val goldRank = sortedMap.indexWhere(pair => goldLabels.containsKey(pair.GetLabel))
    if (goldRank > -1) 1.0/(goldRank + 1.0)
    else 0.0
  }

  
  def GetMSE: Double = {		
    // a new copy of the estimated labels, minus the dummy label
    val estimatedLabelsCopy = new TObjectDoubleHashMap[String]

    val iter: TObjectDoubleIterator[String] = estimatedLabels.iterator
    while (iter.hasNext) {
      iter.advance
      if (!iter.key.equals(Constants.GetDummyLabel))
        estimatedLabelsCopy.put(iter.key, iter.value)
      // Check: using "put" rather than "adjustValue" because the
      // latter doesn't make sense here.
    }
		
    // normalize the estimated label scores.
    ProbUtil.Normalize(estimatedLabelsCopy)

    // now compute mean squared error
    var mse = 0.0
    val goldLabIter: TObjectDoubleIterator[String] = goldLabels.iterator
    while (goldLabIter.hasNext) {
      goldLabIter.advance
      if (estimatedLabelsCopy.containsKey(goldLabIter.key)) {
        val diff = goldLabIter.value - estimatedLabelsCopy.get(goldLabIter.key)
        mse += diff * diff
				
        // remove the label from estimated labels so that finally
        // only non-gold labels remain.
        estimatedLabelsCopy.remove(goldLabIter.key())
      } else {
        mse += goldLabIter.value * goldLabIter.value
      }
    }
		
    // now add the error for all the estimated labels which are non-gold
    val estLabelIter: TObjectDoubleIterator[String] = estimatedLabelsCopy.iterator
    while (estLabelIter.hasNext) {
      estLabelIter.advance
      mse += estLabelIter.value * estLabelIter.value
    }
    mse
  }

  def setIsTestNode (newValue: Boolean = true) = isTestNode = newValue
  def setIsSeedNode (newValue: Boolean = true) = isSeedNode = newValue
	
  // returns a representation of the node in the following format, with
  // fields separated by a delimited which is passed as an argument
  // Output Format:
  // id gold_label injected_labels estimated_labels neighbors rw_probabilities
  def toString (delim: String) = {
    val rwProbStr = (Constants._kInjProb + " " + pinject + " " 
                     + Constants._kContProb + " " + pcontinue + " "
                     + Constants._kTermProb + " " + pabandon)
      
    (name + delim +
     CollectionUtil.Map2String(goldLabels) + delim +
     CollectionUtil.Map2String(injectedLabels) + delim +
     CollectionUtil.Map2String(estimatedLabels) + delim +
     "Neighbors: " + CollectionUtil.Map2String(neighbors) + delim +
     rwProbStr)
  }

}
