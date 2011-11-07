package upenn.junto.util;

/**
 * Used, e.g., to keep track of an Object and its associated score.
 */
public class ObjectDoublePair {
  private Object label_;
  private double score_;
	
  public ObjectDoublePair (Object l, double s) {
    this.label_ = l;
    this.score_ = s;
  }
	
  public Object GetLabel() {
    return label_;
  }
	
  public double GetScore() {
    return score_;
  }
}
