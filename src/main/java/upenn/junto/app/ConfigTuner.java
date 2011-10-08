package upenn.junto.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import upenn.junto.config.ConfigReader;
import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.Defaults;
import upenn.junto.util.MessagePrinter;

public class ConfigTuner {

  private static ArrayList<Hashtable> 
    GetAllCombinations(Hashtable tuningConfig) {
    ArrayList<Hashtable> configs = new ArrayList<Hashtable>();

    Iterator iter = tuningConfig.keySet().iterator();
    while (iter.hasNext()) {
      String paramKey = (String) iter.next();
      String paramVal = (String) tuningConfig.get(paramKey);
			
      // e.g. mu1 = 1e-8,1,1e-8
      String[] fields = paramVal.split(",");
      int currSize = configs.size();
      for (int fi = 0; fi < fields.length; ++fi) {
        // add the first configuration, if none exists
        if (configs.size() == 0) {
          configs.add(new Hashtable());
          ++currSize;
        }
        for (int ci = 0; ci < currSize; ++ci) {
          // the first value can be added to existing
          // configurations.
          if (fi == 0) {
            configs.get(ci).put(paramKey, fields[fi]);
          } else {
            Hashtable nc = (Hashtable) configs.get(ci).clone();
            nc.put(paramKey, fields[fi]);
						
            // append the new config to the end of the list
            configs.add(nc);
          }
        }
      }
    }

    System.out.println("Total config (non-unique) combinations: " + configs.size());
    return (configs);
  }

  private static void Run(Hashtable tuningConfig) {
    // some essential options terminate if they are note specified
    String idenStr = Defaults.GetValueOrDie(tuningConfig, "iden_str");
    String logDir = Defaults.GetValueOrDie(tuningConfig, "log_output_dir");
    String opDir = Defaults.GetValueOrDefault(
                                           (String) tuningConfig.get("output_dir"), null);
    boolean skipExistingConfigs =
      Defaults.GetValueOrDefault((String) tuningConfig.get("skip_existing_config"), false); 

    // config file with post-tuning testing details (i.e. final test file etc.) 
    String finalTestConfigFile = (String) tuningConfig.get("final_config_file");
    tuningConfig.remove("final_config_file");

    // generate all possible combinations (non unique)
    ArrayList<Hashtable> configs = GetAllCombinations(tuningConfig);
		
    ArrayList<ArrayList> results = new ArrayList<ArrayList>();
    HashSet<String> uniqueConfigs = new HashSet<String>();
		
    // map from algo to the current best scores and the corresponding config
    HashMap<String,Hashtable> algo2BestConfig = new HashMap<String,Hashtable>(); 
    TObjectDoubleHashMap algo2BestScore = new TObjectDoubleHashMap(); 
		
    // store console
    PrintStream consoleOut = System.out;
    PrintStream consoleErr = System.err;

    for (int ci = 0; ci < configs.size(); ++ci) {
      Hashtable c = configs.get(ci);
			
      // if this a post-tune config, then generate seed and test files
      if (Defaults.GetValueOrDefault((String) c.get("is_final_run"), false)) {
        String splitId = Defaults.GetValueOrDie(c, "split_id");
        c.put("seed_file", c.remove("seed_base") + "." + splitId + ".train");
        c.put("test_file", c.remove("test_base") + "." + splitId + ".test");
      }
			
      // output file name is considered a unique identifier of a configuration
      String outputFile = GetOutputFileName(c, opDir, idenStr);
      if (uniqueConfigs.contains(outputFile)) {
        continue;
      }
      uniqueConfigs.add(outputFile);
      if (opDir != null) {
        c.put("output_file", outputFile);
      }

      System.out.println("Working with config: " + c.toString());

      try {
        // reset System.out so that the log printed using System.out.println
        // is directed to the right log file
        String logFile = GetLogFileName(c, logDir, idenStr);
				
        // if the log file exists, then don't repeat
        File lf = new File(logFile);
        if (skipExistingConfigs && lf.exists()) {
          continue;
        }
				
        FileOutputStream fos = new FileOutputStream(new File(logFile));
        PrintStream ps = new PrintStream(fos);
        System.setOut(ps);
        System.setErr(ps);
			
        results.add(new ArrayList());
        JuntoConfigRunner.apply(c, results.get(results.size() - 1));
        UpdateBestConfig((String) c.get("algo"), algo2BestScore,
                         algo2BestConfig, c, results.get(results.size() - 1));

        // reset System.out back to the original console value
        System.setOut(consoleOut);
        System.setErr(consoleErr);
				
        // close log file
        fos.close();

      } catch (FileNotFoundException fnfe) {
        fnfe.printStackTrace();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
		
    // print out the best parameters for each algorithm
    Iterator algoIter = algo2BestConfig.keySet().iterator();
    while (algoIter.hasNext()) {
      String algo = (String) algoIter.next();
      System.out.println("\n#################\n" +
                         "BEST_CONFIG_FOR " + algo + " " +
                         algo2BestScore.get(algo) + "\n" +
                         CollectionUtil.Map2StringPrettyPrint(algo2BestConfig.get(algo)));
			
      // run test with tuned parameters, if requested
      if (finalTestConfigFile != null) {
        Hashtable finalTestConfig = (Hashtable) algo2BestConfig.get(algo).clone();
				
        // add additional config options from the file to the tuned params
        finalTestConfig = ConfigReader.read_config(finalTestConfig, finalTestConfigFile);
        JuntoConfigRunner.apply(finalTestConfig, null);
      }
    }
  }
	
  private static String GetOutputFileName(Hashtable c, String opDir, String idenStr) {
    String outputFile = " ";
    if (c.get("algo").equals("mad") ||
        c.get("algo").equals("lgc") ||
        c.get("algo").equals("am") ||
        c.get("algo").equals("lclp")) {
      outputFile = opDir + "/" + GetBaseName2(c, idenStr);

    } else if (c.get("algo").equals("maddl")) {
      outputFile = opDir + "/" +
        GetBaseName2(c, idenStr) +
        ".mu4_" + c.get("mu4");

    } else if (c.get("algo").equals("adsorption") || c.get("algo").equals("lp_zgl")) {
      outputFile = opDir + "/" + GetBaseName(c, idenStr);
    } else {
      MessagePrinter.PrintAndDie("output_1 file can't be empty!");
    }
    return (outputFile);
  }
	
  private static String GetLogFileName(Hashtable c, String logDir, String idenStr) {
    String logFile = "";
    if (c.get("algo").equals("mad") ||
        c.get("algo").equals("lgc") ||
        c.get("algo").equals("am") ||
        c.get("algo").equals("lclp")) {
      logFile = logDir + "/" + "log." + GetBaseName2(c, idenStr);

    } else if (c.get("algo").equals("maddl")) {
      logFile = logDir + "/" +
        "log." +
        GetBaseName2(c, idenStr) +
        ".mu4_" + c.get("mu4");

    } else if (c.get("algo").equals("adsorption") || c.get("algo").equals("lp_zgl")) {
      logFile = logDir + "/" +
        "log." + GetBaseName(c, idenStr);
    } else {
      MessagePrinter.PrintAndDie("output_2 file can't be empty!");
    }
    return (logFile);
  }
	
  private static String GetBaseName(Hashtable c, String idenStr) {
    String base = idenStr;
						
    if (c.containsKey("max_seeds_per_class")) {
      base += ".spc_" + c.get("max_seeds_per_class");
    }
		
    base += "." + c.get("algo");
    if (c.containsKey("use_bipartite_optimization")) {
      base += ".bipart_opt_" + c.get("use_bipartite_optimization");
    }
    if (c.containsKey("top_k_neighbors")) {
      base += ".K_" + c.get("top_k_neighbors");
    }
    if (c.containsKey("prune_threshold")) {
      base += ".P_" + c.get("prune_threshold");
    }
    if (c.containsKey("high_prune_thresh")) {
      base += ".feat_prune_high_" + c.get("high_prune_thresh");
    }
    if (c.containsKey("keep_top_k_labels")) {
      base += ".top_labels_" + c.get("keep_top_k_labels");
    }
    if (c.containsKey("train_fract")) {
      base += ".train_fract_" + c.get("train_fract");
    }
    if (Defaults.GetValueOrDefault((String) c.get("set_gaussian_kernel_weights"), false)) {
      double sigmaFactor = Double.parseDouble(Defaults.GetValueOrDie(c, "gauss_sigma_factor"));
      base += ".gk_sig_" + sigmaFactor;
    }
		
    if (c.containsKey("algo") && (c.get("algo").equals("adsorption") || 
                                  c.get("algo").equals("mad") ||
                                  c.get("algo").equals("maddl"))) {
      double beta = Defaults.GetValueOrDefault((String) c.get("beta"), 2.0);
      base += ".beta_" + beta;
    }
		
    // if this a post-tune config, then generate seed and test files
    if (Defaults.GetValueOrDefault((String) c.get("is_final_run"), false)) {
      base += ".split_id_" + Defaults.GetValueOrDie(c, "split_id");
    }
		
    return (base);
  }
	
  private static String GetBaseName2(Hashtable c, String idenStr) {
    String base = GetBaseName(c, idenStr) +
      ".mu1_" + c.get("mu1") +
      ".mu2_" + c.get("mu2") +
      ".mu3_" + c.get("mu3") +
      ".norm_" + c.get("norm");
    return (base);
  }
	
  private static void UpdateBestConfig(String algo, TObjectDoubleHashMap algo2BestScore,
                                       HashMap<String,Hashtable> algo2BestConfig, Hashtable config,
                                       ArrayList perIterMultiScores) {
    TDoubleArrayList perIterScores = new TDoubleArrayList();
    for (int i = 1; i < perIterMultiScores.size(); ++i) {
      TObjectDoubleHashMap r = (TObjectDoubleHashMap) perIterMultiScores.get(i);
      perIterScores.add(r.get(Constants.GetMRRString()));
    }

    if (perIterScores.size() > 0) {
      //			System.out.println("SIZE: " + perIterScores.size());
      int mi = 0;
      for (int i = 1; i < perIterScores.size(); ++i) {
        if (perIterScores.get(i) > perIterScores.get(mi)) {
          mi = i;
        }
      }
      //			System.out.println("max_idx: " + mi + " " + perIterScores.toString());
      double maxScore = perIterScores.get(mi); // perIterScores.max();
      if (algo2BestScore.size() == 0 || algo2BestScore.get(algo) < maxScore) {
        //				System.out.println("new best score: " + maxScore);
        // best iteration
        int bestIter = perIterScores.indexOf(maxScore) + 1;

        algo2BestScore.put(algo, maxScore);
        algo2BestConfig.put(algo, (Hashtable) config.clone());
        algo2BestConfig.get(algo).put("iters", bestIter);
      }
    }
  }

  public static void main(String[] args) {
    Hashtable tuningConfig = ConfigReader.read_config(args);
    Run(tuningConfig);
  }
}
