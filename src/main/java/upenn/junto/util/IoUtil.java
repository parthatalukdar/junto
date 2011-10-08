package upenn.junto.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;

import upenn.junto.type.ObjectDoublePair;
import upenn.junto.type.RyanAlphabet;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.iterator.TObjectDoubleIterator;

public class IoUtil {

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
      ioe.printStackTrace();
    }
		
    MessagePrinter.Print("Total " + retList.size() +
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
      ioe.printStackTrace();
    }
		
    MessagePrinter.Print("Total " + retList.size() +
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
      ioe.printStackTrace();
    }

    MessagePrinter.Print("Total " + retAlpha.size() +
                         " entries loaded from " + fileName);
    return (retAlpha);
  }
}
