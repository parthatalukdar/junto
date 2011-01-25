package upenn.junto.util;

import java.util.Hashtable;

public class Defaults {
	
  public static String GetValueOrDie(Hashtable config, String key) {
    if (!config.containsKey(key)) {
      MessagePrinter.PrintAndDie("Must specify " + key + "");
    }
    return ((String) config.get(key));
  }

  public static String GetValueOrDefault(String valStr, String defaultVal) {
    String res = defaultVal;
    if (valStr != null) {
      res = valStr;
    }
    return (res);
  }

  public static double GetValueOrDefault(String valStr, double defaultVal) {
    double res = defaultVal;
    if (valStr != null) {
      res = Double.parseDouble(valStr);
    }
    return (res);
  }

  public static boolean GetValueOrDefault(String valStr, boolean defaultVal) {
    boolean res = defaultVal;
    if (valStr != null) {
      res = Boolean.parseBoolean(valStr);
    }
    return (res);
  }

  public static int GetValueOrDefault(String valStr, int defaultVal) {
    int res = defaultVal;
    if (valStr != null) {
      res = Integer.parseInt(valStr);
    }
    return (res);
  }

}
