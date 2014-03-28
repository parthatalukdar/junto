package upenn.junto.algorithm.mad_sketch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import upenn.junto.util.ObjectDoublePair;
import upenn.junto.util.RyanAlphabet;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;
import gnu.trove.TObjectFloatHashMap;
import gnu.trove.TObjectFloatIterator;

public class CollectionUtil2 {

  public static ArrayList<ObjectDoublePair> ReverseSortMap(TObjectDoubleHashMap m) {
    ArrayList<ObjectDoublePair> lsps = new ArrayList<ObjectDoublePair>();

    TObjectDoubleIterator mi = m.iterator();
    while (mi.hasNext()) {
      mi.advance();
      lsps.add(new ObjectDoublePair(mi.key(), mi.value()));
    }

    ObjectDoublePairComparator lspComparator = new ObjectDoublePairComparator();
    Collections.sort(lsps, lspComparator);

    return (lsps);
  }
  
  public static ArrayList<ObjectDoublePair> ReverseSortMap(TObjectFloatHashMap m) {
	    ArrayList<ObjectDoublePair> lsps = new ArrayList<ObjectDoublePair>();

	    TObjectFloatIterator mi = m.iterator();
	    while (mi.hasNext()) {
	      mi.advance();
	      lsps.add(new ObjectDoublePair(mi.key(), mi.value()));
	    }

	    ObjectDoublePairComparator lspComparator = new ObjectDoublePairComparator();
	    Collections.sort(lsps, lspComparator);

	    return (lsps);
	  }
	
  protected static class ObjectDoublePairComparator implements Comparator<ObjectDoublePair> {
    public int compare(ObjectDoublePair p1, ObjectDoublePair p2) {
      double diff = p2.GetScore() - p1.GetScore();
      return (diff > 0 ? 1 : (diff < 0 ? -1 : 0));
    }
  }

  public static TObjectDoubleHashMap String2Map(String inp) {
    return (String2Map(null, inp));
  }

  public static TObjectDoubleHashMap String2Map(TObjectDoubleHashMap retMap,
                                                String inp) {
    if (retMap == null) {
      retMap = new TObjectDoubleHashMap();
    }

    if (inp.length() > 0) {
      String[] fields = inp.split(" ");
      for (int i = 0; i < fields.length; i += 2) {
        retMap.put(fields[i], Double.parseDouble(fields[i + 1]));
      }
    }

    return (retMap);
  }
	
  public static String Map2String(TObjectDoubleHashMap m) {
    return (Map2String(m, null));
  }
  
  public static String Map2String(TObjectFloatHashMap m) {
	    return (Map2String(m, null));
  }

  public static String Map2String(TObjectDoubleHashMap m, RyanAlphabet a) {
    // String retString = "";
    TObjectDoubleIterator mIter = m.iterator();
    StringBuffer retBuffer = new StringBuffer();
		
    ArrayList<ObjectDoublePair> sortedMap = ReverseSortMap(m);
    int n = sortedMap.size();
    for (int i = 0; i < n; ++i) {
      String label = (String) sortedMap.get(i).GetLabel();
      if (a != null) {
        Integer li = String2Integer(label);
        if (li != null) {
          label = (String) a.lookupObject(li.intValue());
        }
      }
      // retString += " " + label + " " + sortedMap.get(i).GetScore();
      if (sortedMap.get(i).GetScore() > Constants.GetStoppingThreshold()) {
    	  retBuffer.append(" " + label + " " + sortedMap.get(i).GetScore());
      }
    }
		
    return (retBuffer.toString().trim());
  }
  
  public static String Map2String(TObjectFloatHashMap m, RyanAlphabet a) {
	    String retString = "";
	    TObjectFloatIterator mIter = m.iterator();
			
	    ArrayList<ObjectDoublePair> sortedMap = ReverseSortMap(m);
	    int n = sortedMap.size();
	    for (int i = 0; i < n; ++i) {
	      String label = (String) sortedMap.get(i).GetLabel();
	      if (a != null) {
	        Integer li = String2Integer(label);
	        if (li != null) {
	          label = (String) a.lookupObject(li.intValue());
	        }
	      }
	      retString += " " + label + " " + sortedMap.get(i).GetScore();
	    }
			
	    return (retString.trim());
	  }
	
  public static Integer String2Integer(String str) {
    Integer retInt = null;
    try {
      int ri = Integer.parseInt(str);
      retInt = new Integer(ri);
    } catch (NumberFormatException nfe) {
      // don't do anything
    }
    return (retInt);
  }
	
  public static String Map2StringPrettyPrint(Hashtable m) {
    String retString = "";
    Iterator iter = m.keySet().iterator();

    while (iter.hasNext()) {
      String key = (String) iter.next();
      retString += key + " = " + m.get(key) + "\n";
    }

    return (retString.trim());
  }

  public static String Join(String[] fields, String delim) {
    String retString = "";
    for (int si = 0; si < fields.length; ++si) {
      if (si > 0) {
        retString += delim + fields[si];
      } else {
        retString = fields[0];
      }
    }
    return (retString);
  }
	
  public static ArrayList<String> GetIntersection(TObjectDoubleHashMap m1,
                                                  ArrayList<String> l2) {
    ArrayList<String> retList = new ArrayList<String>();
    for (int i = 0; i < l2.size(); ++i) {
      if (m1.containsKey(l2.get(i))) {
        retList.add(l2.get(i));
      }
    }
    return (retList);
  }
	
}
