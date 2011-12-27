package upenn.junto.util;

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

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IoUtil {
    private static Log LOG = LogFactory.getLog(IoUtil.class);

  public static ArrayList<String> LoadFile(String fileName) {
    ArrayList<String> retList = new ArrayList<String>();
		
    try {
      BufferedReader bfr = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = bfr.readLine()) != null) {
        if (!retList.contains(line)) {
          retList.add(line);
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
		
    LOG.info("Total " + retList.size() +
                         " entries loaded from " + fileName);
    return (retList);
  }
	
  public static ArrayList<String> LoadFirstFieldFile(String fileName) {
    ArrayList<String> retList = new ArrayList<String>();
		
    try {
      BufferedReader bfr = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = bfr.readLine()) != null) {
        String[] fields = line.split("\t");
        if (!retList.contains(fields[0])) {
          retList.add(fields[0]);
        }
      }
    } catch (IOException ioe) {
        throw new RuntimeException(ioe);
    }
		
    LOG.info("Total " + retList.size() +
                         " entries loaded from " + fileName);
    return (retList);
  }
	
  public static RyanAlphabet LoadAlphabet(String fileName) {
    RyanAlphabet retAlpha = new RyanAlphabet();
		
    try {
      BufferedReader bfr = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = bfr.readLine()) != null) {
        String[] fields = line.split("\t");
        retAlpha.lookupIndex(fields[0], true);
        assert (retAlpha.lookupIndex(fields[0]) == Integer.parseInt(fields[1]));
      }
    } catch (IOException ioe) {
        throw new RuntimeException(ioe);
    }

    LOG.info("Total " + retAlpha.size() +
                         " entries loaded from " + fileName);
    return (retAlpha);
  }
}
