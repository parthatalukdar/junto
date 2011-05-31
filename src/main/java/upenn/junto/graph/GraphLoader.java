package upenn.junto.graph;

import upenn.junto.util.Defaults;
import upenn.junto.util.MessagePrinter;

import java.util.Hashtable;


public class GraphLoader {
  public static Graph LoadGraph(Hashtable config) {
    Graph g = new Graph();
		
    System.out.println("Going to build graph ...");
    String dataFormat =Defaults.GetValueOrDefault(
                                               (String) config.get("data_format"), "edge_factored");
		
    // this is used mostly for the data received from Mark, i.e.
    // for information extraction machine learning work.
    int maxSeedsPerClass = Defaults.GetValueOrDefault(
                                                   (String) config.get("max_seeds_per_class"), Integer.MAX_VALUE);
		
    double beta = Defaults.GetValueOrDefault((String) config.get("beta"), 2.0);
    boolean isDirected = Defaults.GetValueOrDefault((String) config.get("is_directed"), false);

    // edge factored representation is mostly used for the
    // information integration work.
    if (dataFormat != null && dataFormat.equals("edge_factored")) {
      g = BuildGraphFromEdgeFactoredData.Build(g,
                                               (String) config.get("graph_file"),
                                               (String) config.get("seed_file"),
                                               maxSeedsPerClass,
                                               (String) config.get("test_file"),           /* can be null */
                                               (String) config.get("source_freq_file"),    /* can be null */
                                               (String) config.get("target_filter_file"),  /* can be null */
                                               (String) config.get("prune_threshold"),
                                               beta,
                                               isDirected);
			
      // gold labels for some or all of the nodes 
      if (config.containsKey("gold_labels_file")) {
        g.SetGoldLabels((String) config.get("gold_labels_file"));
      }			
    } else if (dataFormat != null && dataFormat.equals("node_factored")) {
      g = BuildGraphFromNodeFactoredData.Build(g,
                                               (String) config.get("graph_file"),
                                               (String) config.get("seed_file"),
                                               maxSeedsPerClass,
                                               (String) config.get("test_file"),
                                               beta,
                                               isDirected);
    }
		
    // set Gaussian Kernel weights, if requested. In this case, we assume that existing
    // edge weights are distance squared i.e. || x_i - x_j ||^2  
    boolean setGaussianWeights =
      Defaults.GetValueOrDefault((String) config.get("set_gaussian_kernel_weights"), false);
    if (setGaussianWeights) {
      double sigmaFactor = Double.parseDouble(Defaults.GetValueOrDie(config, "gauss_sigma_factor"));
			
      // append sigma factor to the output file name
      // ConfigReader.AppendOptionValue(config, "output_file", ".gk_sig_" + sigmaFactor);
			
      MessagePrinter.Print("Going to set Gaussian Kernel weights ...");
      g.SetGaussianWeights(sigmaFactor);
    }
		
    // keep only top K neighbors: kNN, if requested
    if (config.containsKey("top_k_neighbors")) {
      int maxNeighbors = Defaults.GetValueOrDefault(
                                                 (String) config.get("top_k_neighbors"), Integer.MAX_VALUE);
      g.KeepTopKNeighbors(maxNeighbors);
    }
		
    // check whether random train and test splits are to be generated
    if (config.containsKey("train_fract")) {
      double trainFraction = Double.parseDouble(Defaults.GetValueOrDie(config, "train_fract"));
      CrossValidationGenerator.Split(g, trainFraction);
      g.SetSeedInjected();
    }
		
    MessagePrinter.Print("Seed injected: " + (g.IsSeedInjected() ? "true" : "false"));
    if (g.IsSeedInjected()) {
      // remove seed labels which are not present in any of the test nodes
      // g.RemoveTrainOnlyLabels();
		
      // check whether the seed information is consistent
      // g.CheckAndStoreSeedLabelInformation(maxSeedsPerClass);
		
      // calculate random walk probabilities.
      // random walk probability computation depends on the seed label information,
      // and hence this can be done only after the seed labels have been injected.
      g.CalculateRandomWalkProbabilities(beta);
    }

    // print out graph statistics
    MessagePrinter.Print(GraphStats.PrintStats(g));
		
    // save graph in file, if requested
    if (config.containsKey("graph_output_file")) {
      // g.WriteToFile((String) config.get("graph_output_file"));
      g.WriteToFileWithAlphabet((String) config.get("graph_output_file"));
    }
		
    return (g);
  }

}
