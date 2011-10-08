package upenn.junto.util;

import gnu.trove.map.hash.TObjectDoubleHashMap;

public class Constants {

  public static String _kContProb = "cont_prob";
  public static String _kInjProb = "inj_prob";
  public static String _kTermProb = "term_prob";

  public static double GetSmallConstant() {
    return (1e-12);
  }

  public static String GetDummyLabel() {
    return ("__DUMMY__");
  }

  public static String GetDocPrefix() {
    return ("DOC_");
  }

  public static String GetFeatPrefix() {
    // return ("FEAT_");
    return ("C#");
  }
	
  public static String GetPrecisionString() {
    return ("precision");
  }
	
  public static String GetMRRString() {
    return ("mrr");
  }

  public static String GetMDBRRString() {
    return ("mdmbrr");
  }

  public static double GetStoppingThreshold() {
    return (0.001);
  }

  public static TObjectDoubleHashMap GetDummyLabelDist() {
    TObjectDoubleHashMap ret = new TObjectDoubleHashMap();
    ret.put(Constants.GetDummyLabel(), 1.0);
    return (ret);
  }

}
