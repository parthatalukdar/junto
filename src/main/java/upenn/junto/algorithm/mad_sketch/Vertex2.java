package upenn.junto.algorithm.mad_sketch;

import java.util.ArrayList;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;

// import upenn.junto.label.CountMinSketchLabel;
// import upenn.junto.label.CountMinSketchLabelManager;
// import upenn.junto.label.LabelScores;
// import upenn.junto.label.SketchLabelScores;
import upenn.junto.util.ObjectDoublePair;
import upenn.junto.util.RyanAlphabet;
import upenn.junto.util.RyanFeatureVector;
import upenn.junto.util.Constants;
// import upenn.junto.util.CollectionUtil;
import upenn.junto.util.MessagePrinter;
// import upenn.junto.util.ProbUtil;


public class Vertex2 {
  // vertex name
  private String name_;
	
  // continuation probability
  private double contProb_;
	
  // injection probability
  private double injectProb_;
	
  // termination probability
  private double abndProb_;
	
  // set to true when scores on transition going out
  // of the vertex is normalized and is made a probability
  // distribution.
  private boolean isTransitionNormalized = false;
	
  // labels & scores which are injected in the node
  // as prior knowledge. Only positive scores are
  // allowed.
  private CountMinSketchLabel injected_labels_;
  // private Object injected_labels_;
	
  // labels & their scores estimated by the algorithm.
  // only positive scores are allowed.
  private CountMinSketchLabel estimated_labels_;
	
  // neighbors of the vertex along with edge/association
  // weight
  private TObjectDoubleHashMap neighbors_;
	
  // gold label (if any of the vertex); optional
  private CountMinSketchLabel goldLabel_;
	
  // set to true if the node is injected with seed labels
  private boolean isSeedNode_;
	
  // set to true if the node is to be included during evaluation
  // by default: false
  private boolean isTestNode_;
	
  // feature representation of the vertex
  private RyanFeatureVector features_;
  
  // private CountMinSketchLabel dummyLabel_;
  
  CountMinSketchLabelManager labelManager;
	
  public Vertex2(String name, String label, float weight,
		  						CountMinSketchLabelManager cmslm) {
	this.labelManager = cmslm;
    Initialize(name, label, weight);
  }

//  private Vertex2(String name, CountMinSketchLabel dummyLabel, CountMinSketchLabel label) {
//    Initialize(name, dummyLabel, label, 1.0);
//  }
//	
//  public Vertex2(String name, CountMinSketchLabel dummyLabel, CountMinSketchLabel label, double weight) {
//    Initialize(name, dummyLabel, label, weight);
//  }
	
  private void Initialize(String name, String label, float weight) {
    this.name_ = name;
    this.contProb_ = -1;
    this.injectProb_ = -1;
    this.abndProb_ = -1;
    // this.isTransitionNormalized = false;

    // this.injected_labels_ = new CountMinSketchLabel(labelManager.cms.depth, labelManager.cms.width);
    this.estimated_labels_ = new CountMinSketchLabel(labelManager.cms.depth, labelManager.cms.width);
    // this.goldLabel_ = new CountMinSketchLabel(labelManager.cms.depth, labelManager.cms.width);
    
    if (label != null && !label.equals(Constants.GetDummyLabel())) {
        SetGoldLabel(label, weight);
    }
    
    this.neighbors_ = new TObjectDoubleHashMap();
    this.isSeedNode_ = false;
    this.isTestNode_ = false;
    this.features_ = new RyanFeatureVector(-1, -1, null);

    // initialize the estimated labels with dummy label
    labelManager.add(estimated_labels_, (float) 1.0, label, (float) 1.0);
  }
	
  public String GetName() {
    return this.name_;
  }
	
  public void SetGoldLabel(String gl, float weight) {
    assert(gl != null);

    if (gl.equals(Constants.GetDummyLabel()) || weight == 0) { return; }
    
    if (this.goldLabel_ == null) {
    	this.goldLabel_ = new CountMinSketchLabel(labelManager.cms.depth, labelManager.cms.width);
    }
    if (labelManager.getScore(goldLabel_, gl) < weight) {
    	labelManager.add(goldLabel_, (float) 1.0, gl, weight);
    }
  }
	
//  public void RemoveGoldLabel(T gl) {
//    assert(gl != null);
//    this.goldLabel_.remove(gl);
//  }

  public CountMinSketchLabel GetGoldLabel() {
    return (this.goldLabel_);
  }
	
  public void AddNeighbor(String n, double w) {
    neighbors_.put(n, w);
  }
	
  public void RemoveNeighbor(String n) {
    if (neighbors_.containsKey(n)) {
      neighbors_.remove(n);
    }
  }
	
  public double GetNeighborWeight(String n) {
    return neighbors_.containsKey(n) ?
      neighbors_.get(n) : 0;
  }
	
  public void SetNeighborWeight(String n, double w) {
    assert (w > 0) : w;
    neighbors_.put(n, w);
  }
	
  public Object[] GetNeighborNames() {
    return neighbors_.keys();
  }
	
  public TObjectDoubleHashMap GetNeighbors() {
    return neighbors_;
  }

  public double GetInjectedLabelScore(String l) {
    return labelManager.getScore(injected_labels_, l);
  }
	
  // public TObjectDoubleHashMap GetInjectedLabelScores() {
   // return this.injected_labels_.getLabels();
  //}
  
  public CountMinSketchLabel GetInjectedLabelScores() {
	    return this.injected_labels_;
  }
	
//  public String GetInjectedLabelScoresPretty(RyanAlphabet la) {
//    return (this.GetPrettyPrintMap(this.injected_labels_.getLabels(), la));
//  }
	
  public void SetInjectedLabelScore(String l, float w) {
	if (w == 0) { return; }
	
	if (this.injected_labels_ == null) {
		this.injected_labels_ = new CountMinSketchLabel(labelManager.cms.depth, labelManager.cms.width);
	}
	// System.out.println("INJECTING: " + GetName() + " " + l + " " + w);
	
	labelManager.add(injected_labels_, (float) 1.0, l, w);
  }
	
//  public void RemoveInjectedLabel(String l) {
//    injected_labels_.remove(l);
//    if (injected_labels_.size() == 0) {
//      ResetSeedNode();
//    }
//  }
	
  public double GetEstimatedLabelScore(String l) {
	return this.labelManager.getScore(this.estimated_labels_, l);
  }
	
//  public String GetEstimatedLabelScoresPretty(RyanAlphabet la) {
//    return (this.GetPrettyPrintMap(this.estimated_labels_.getLabels(), la));
//  }
	
  // public TObjectDoubleHashMap GetEstimatedLabelScores() {
  //  return this.estimated_labels_.getLabels();
  //}
  
  public CountMinSketchLabel GetEstimatedLabelScores() {
	    return this.estimated_labels_;
  }
	
  public void SetEstimatedLabelScore(String l, float w) {
    if (w != 0) {
      labelManager.add(estimated_labels_, (float) 1.0, l, w);
    } else {
    	MessagePrinter.PrintAndDie("Label removal in Vertex2.SetEstimatedLabelScore() is not implemented");
      // estimated_labels_.remove(l);
    }
  }
	
  public void SetEstimatedLabelScores(CountMinSketchLabel m) {
    // CountMinSketchLabelManager.clear(estimated_labels_);
    estimated_labels_ = m;
  }
	
  public static String GetPrettyPrintMap(TObjectDoubleHashMap m, RyanAlphabet la) {		
    ArrayList<ObjectDoublePair> sortedMap = CollectionUtil2.ReverseSortMap(m);
    String op = "";
    for (int lspi = 0; lspi < sortedMap.size(); ++lspi) {
      String label = (String) sortedMap.get(lspi).GetLabel();
      if (la != null) {
        Integer li = CollectionUtil2.String2Integer(label);
        if (li != null) {
          label = (String) la.lookupObject(li.intValue());
        }
      }
      op += " " + label + " " +
        sortedMap.get(lspi).GetScore();
    }

    return (op.trim());
  }

  public void UpdateEstimatedLabel(String l, float w) {
	  labelManager.add(estimated_labels_, (float) 1.0, l, w);
//    if (!estimated_labels_.containsKey(l)) {
//      estimated_labels_.put(l, w);
//    } else {
//      estimated_labels_.put(l, estimated_labels_.get(l) + w);
//    }
  }
	
  // calculate random walk based probabilities
  // For details, see Sec 3 of Talukdar et al, EMNLP 08
  //
  // the method returns true of the node has zero entropy neighborhood
  public boolean CalculateRWProbabilities(double beta) {
    // TODO(partha): temporarily commented for working with WebKB data
    // on 03/26/2009. need to decide whether to make it permanent. 
    //		if (!isTransitionNormalized) {
    //			NormalizeTransitionProbability();
    //		}
    TObjectDoubleHashMap neighborClone = this.neighbors_.clone();
    Normalize(neighborClone);
		
    double ent = GetNeighborhoodEntropy(neighborClone);
    double cv = Math.log(beta) / Math.log(beta + ent);
		
    boolean isZeroEntropy = false;

    double jv = 0;
    // if (injected_labels_.size() >= 1) {
    if (injected_labels_ != null &&
    		!CountMinSketchLabelManager.isEmpty(injected_labels_)) {
      jv = (1 - cv) * Math.sqrt(ent);
			
      // Entropy can be 0 when the seed node is connected to only
      // one other node. This can make the injection probability 0,
      // which is readjusted to 1
      if (jv == 0) {
        isZeroEntropy = true;
        jv = 0.99;
        cv = 0.01;
        // MessagePrinter.Print("ZERO ENTROPY NEIGHBORHOOD ... DECIDE WHAT TO DO!");
        //				MessagePrinter.Print("ZERO ENTROPY NEIGHBORHOOD for " + this.GetName() +
        //										" ... Heuristic adjustment used!");
      }
    }
    double zv = Math.max(cv + jv, 1);
		
    contProb_ = cv / zv;
    injectProb_ = jv / zv;
    abndProb_ = Math.max(0, 1 - contProb_ - injectProb_);
		
    //		// TODO(partha): temporary, no random walk probability
    //		abndProb_ = 0;
    //		contProb_ = 1;
    //		injectProb_ = 0;
    //		if (injected_labels_.size() >= 1) {
    //			injectProb_ = 1;
    //			contProb_ = 0;
    //		}

    //		System.out.println(this.name_ + "\t" + contProb_ + "\t" +
    //						   injectProb_ + "\t" + abndProb_ + "\t" +
    //						   injected_labels_.size() +
    //						   "\tentropy: " + ent +
    //						   "\tseed_size: " + injected_labels_.size());
		
    return (isZeroEntropy);
  }
	
  public void NormalizeTransitionProbability() {
    Normalize(this.neighbors_);
    isTransitionNormalized = true;
  }

  private double GetNeighborhoodEntropy(TObjectDoubleHashMap map) {
    double entropy = 0;
    //		TObjectDoubleIterator ni = neighbors_.iterator();
    TObjectDoubleIterator ni = map.iterator();
    while (ni.hasNext()) {
      ni.advance();
      entropy += -1 * ni.value() *
        Math.log(ni.value()) / Math.log(2);
    }
    return (entropy);
  }
	
  // returns the sum of weights of all edges going out
  // from the node.
  public double GetOutEdgeWeightSum() {
    double sum = 0;
    TObjectDoubleIterator ni = neighbors_.iterator();
    while (ni.hasNext()) {
      ni.advance();
      sum += ni.value();
    }
    return (sum);
  }
	
  // probability with which the injected probability
  // should be used.
  public double GetInjectionProbability() {
    return (injectProb_);
  }
	
  // probability with which the random walk should be
  // continued to a neighboring vertex. This leads to
  // weighted average label distribution of all neighbors.
  public double GetContinuationProbability() {
    return (contProb_);
  }
	
  // probability with which the random walk is terminated
  // and the dummy label emitted.
  public double GetTerminationProbability() {
    return (abndProb_);
  }
	
  //	public double GetNormalizationConstantOld(double mu1, double mu2, double mu3) {
  //		double mii = 0;
  //		double totalNeighWeight = 0;
  //		TObjectDoubleIterator nIter = neighbors_.iterator();
  //		while (nIter.hasNext()) {
  //			nIter.advance();
  //			totalNeighWeight += nIter.value();
  //		}
  //		
  //		// mu1 x p^{inj} + mu2 x p^{cont} x \sum_j W_{ij} + mu3
  //		mii = mu1 * this.GetInjectionProbability() +
  //			    mu2 * this.GetContinuationProbability() * totalNeighWeight +
  //			    mu3;
  //
  //		return (mii);
  //	}
	
  public double GetNormalizationConstant(Graph2 g, double mu1, double mu2, double mu3) {
    double mii = 0;
    double totalNeighWeight = 0;
    TObjectDoubleIterator nIter = neighbors_.iterator();
    while (nIter.hasNext()) {
      nIter.advance();
      totalNeighWeight += this.GetContinuationProbability() * nIter.value();
			
      Vertex2 neigh = g._vertices.get(nIter.key());
      totalNeighWeight += neigh.GetContinuationProbability() *
        neigh.GetNeighborWeight(this.GetName());
    }
		
    // mu1 x p^{inj} + 0.5 * mu2 x \sum_j (p_{i}^{cont} W_{ij} + p_{j}^{cont} W_{ji}) + mu3
    mii = mu1 * this.GetInjectionProbability() +
      /*0.5 **/ mu2 * totalNeighWeight +
      mu3;

    return (mii);
  }
	
  public double GetLCLPNormalizationConstant(Graph2 g, double mu1, double mu2, double mu3) {
    // sum_{u} W_uv^{2}, where u is the neighbor 
    double totalNeighWeightSq = 0;
    TObjectDoubleIterator nIter = neighbors_.iterator();
    while (nIter.hasNext()) {
      nIter.advance();
      Vertex2 neigh = g._vertices.get(nIter.key());
      totalNeighWeightSq += neigh.GetNeighborWeight(this.GetName()) *
        neigh.GetNeighborWeight(this.GetName());
    }
		
    double denom = mu1 * (IsSeedNode() ? 1 : 0) +
      mu2 * (1 + totalNeighWeightSq) +
      mu3;
    //		System.out.println("norm denom: " + GetName() + " " + IsSeedNode() + " " + denom);
		
    // mu1 x S_{vv} + mu2 * (1 + \sum_{u} W_{vu}^{2}) + mu3
    return (denom);
  }
	
  // This is used in case of LGC
  public double GetNormalizationConstant2(Graph2 g, double mu1, double mu2, double mu3) {
    double mii = 0;
    double totalNeighWeight = 0;
    TObjectDoubleIterator nIter = neighbors_.iterator();
    while (nIter.hasNext()) {
      nIter.advance();
      totalNeighWeight += nIter.value();			
    }
		
    // mu1 x p^{inj} + 0.5 * mu2 x \sum_j (p_{i}^{cont} W_{ij} + p_{j}^{cont} W_{ji}) + mu3
    mii = mu1 * (IsSeedNode() ? 1 : 0) + /*0.5 **/ mu2 * totalNeighWeight + mu3;

    return (mii);
  }
	
  public boolean IsFeatNode() {
    boolean retVal = false;
    if (this.GetName().startsWith(Constants.GetFeatPrefix())) {
      retVal = true;
    }
    return retVal;
  }
	
  public void SetSeedNode() {
    this.isSeedNode_ = true;
  }
	
  public void ResetSeedNode() {
    this.isSeedNode_ = false;
  }
	
  public boolean IsSeedNode() {
    return this.isSeedNode_;
  }
	
  public void SetTestNode() {
    this.isTestNode_ = true;
  }
	
  public void ResetTestNode() {
    this.isTestNode_ = false;
  }
	
  public boolean IsTestNode() {
    return this.isTestNode_;
  }
	
  public void AddFeatureVal(int idx, double val) {
    this.features_.add(idx, val);
  }
	
  public RyanFeatureVector GetFeatureVector() {
    return this.features_;
  }
	
  public double GetMRR() {
    ArrayList<ObjectDoublePair> sortedMap =
    		CollectionUtil2.ReverseSortMap(labelManager.getLabelScores(estimated_labels_));

    double mrr = 0;
    int goldRank = 0;
    for (int lspi = 0; lspi < sortedMap.size(); ++lspi) {
      if (sortedMap.get(lspi).GetLabel().equals(Constants.GetDummyLabel())) {
        continue;				
      }
      ++goldRank;

      //			if (sortedMap.get(lspi).GetLabel().equals(GetGoldLabel())) {
      //				mrr = 1.0 / goldRank;
      //				break;
      //			}
      // if (this.goldLabel_.containsKey(sortedMap.get(lspi).GetLabel())) {
      if (goldLabel_ != null &&
    		  labelManager.contains(goldLabel_, (String) sortedMap.get(lspi).GetLabel())) {
        mrr = 1.0 / goldRank;
        break;
      }
    }
    return (mrr);
  }
	
//  public double GetMSE() {		
//    // a new copy of the estimated labels, minus the dummy label
//    TObjectDoubleHashMap estimatedLabelsCopy = new TObjectDoubleHashMap();
//    TObjectDoubleIterator iter = this.estimated_labels_.getLabels().iterator();
//    while (iter.hasNext()) {
//      iter.advance();
//      if (iter.key().equals(Constants.GetDummyLabel())) {
//        continue;
//      }
//      estimatedLabelsCopy.adjustValue(iter.key(), iter.value());
//    }
//		
//    // normalize the estimated label scores.
//    ProbUtil.Normalize(estimatedLabelsCopy);
//
//    // now compute mean squared error
//    double mse = 0;
//    TObjectDoubleIterator goldLabIter = this.goldLabel_.getLabels().iterator();
//    while (goldLabIter.hasNext()) {
//      goldLabIter.advance();
//      if (estimatedLabelsCopy.containsKey(goldLabIter.key())) {
//        double diff = goldLabIter.value() - estimatedLabelsCopy.get(goldLabIter.key());
//        mse += diff * diff;
//				
//        // remove the label from estimated labels so that finally
//        // only non-gold labels remain.
//        estimatedLabelsCopy.remove(goldLabIter.key());
//      } else {
//        mse += goldLabIter.value() * goldLabIter.value();
//      }
//    }
//		
//    // now add the error for all the estimated labels which are non-gold
//    TObjectDoubleIterator estLabelIter = estimatedLabelsCopy.iterator();
//    while (estLabelIter.hasNext()) {
//      estLabelIter.advance();
//      mse += estLabelIter.value() * estLabelIter.value();
//    }
//		
//    return (mse);
//  }
	
  // returns a representation of the node in the following format, with
  // fields separated by a delimited which is passed as an argument
  // Output Format:
  // id gold_label injected_labels estimated_labels neighbors rw_probabilities
  public String toString(String delim) {
    String rwProbStr =
      Constants._kInjProb + " " + GetInjectionProbability() + " " +
      Constants._kContProb + " " + GetContinuationProbability() + " " +	
      Constants._kTermProb + " " + GetTerminationProbability();

    return(this.GetName() + delim +
           CollectionUtil2.Map2String(labelManager.getLabelScores(this.goldLabel_)) + delim +
           CollectionUtil2.Map2String(labelManager.getLabelScores(this.injected_labels_)) + delim +
           CollectionUtil2.Map2String(labelManager.getLabelScores(this.estimated_labels_)) + delim +
           CollectionUtil2.Map2String(this.neighbors_) + delim +
           rwProbStr);
  }

  public static void Normalize(TObjectDoubleHashMap m) {
    Normalize(m, Integer.MAX_VALUE);
  }

  public static void Normalize(TObjectDoubleHashMap m, int keepTopK) {
    // if the number of labels to retain are not the trivial
    // default value, then keep the top scoring k labels as requested
    if (keepTopK != Integer.MAX_VALUE) {
      KeepTopScoringKeys(m, keepTopK);
    }

    TObjectDoubleIterator mi = m.iterator();
    double denom = 0;
    while (mi.hasNext()) {
      mi.advance();
      denom += mi.value();
    }
    // assert (denom > 0);

    if (denom > 0) {
      mi = m.iterator();
      while (mi.hasNext()) {
        mi.advance();
        double newVal = mi.value() / denom;
        mi.setValue(newVal);
      }
    }
  }

  public static void KeepTopScoringKeys(TObjectDoubleHashMap m, int keepTopK) {
    ArrayList<ObjectDoublePair> lsps = CollectionUtil2.ReverseSortMap(m);

    // the array is sorted from large to small, so start
    // from beginning and retain only top scoring k keys.
    m.clear();
    int totalAdded = 0;
    int totalSorted = lsps.size();
    // for (int li = lsps.size() - 1; li >= 0 && totalAdded <= keepTopK; --li) {
    for (int li = 0; li < totalSorted && totalAdded < keepTopK; ++li) {
      ++totalAdded;

      if (lsps.get(li).GetScore() > 0) {
        m.put(lsps.get(li).GetLabel(), lsps.get(li).GetScore());
      }
    }
		
    // size of the new map is upper bounded by the max
    // number of entries requested
    assert (m.size() <= keepTopK);
  }
}
