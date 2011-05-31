package upenn.junto.graph;

import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.regex.Pattern;

public class BuildGraphFromEdgeFactoredData {
	
  public static Graph Build(Graph g,
                            String fileName,
                            String seedFile,
                            int maxSeedsPerClass,
                            String testFile,
                            String srcFilterFile,
                            String trgFilterFile,
                            String pruneThreshold,
                            double beta,
                            boolean isDirected) {
    // build the graph itself
    g = BuildInstanceMultiLine(g, fileName, srcFilterFile, 
                               trgFilterFile, pruneThreshold,
                               isDirected);

    // Inject seed labels
    if (seedFile != null) {
      g = InjectSeedLabels(g, seedFile, maxSeedsPerClass, (testFile == null));
      g.SetSeedInjected();
    }
		
    // Mark all test nodes which will be used
    // during evaluation.
    if (testFile != null) {
      g = MarkTestNodes(g, testFile);
    }
		
    if (pruneThreshold != null) {
      g.PruneLowDegreeNodes(Integer.parseInt(pruneThreshold));
    }
		
    // calculate random walk probabilities
    g.CalculateRandomWalkProbabilities(beta);
		
    return (g);
  }
	
  public static Graph BuildInstanceMultiLine(Graph g, 
                                             String fileName,
                                             String srcFilterFile, 
                                             String trgFilterFile, 
                                             String pruneThresholdStr,
                                             boolean isDirected) {
    try {
      int pruneThreshold = Integer.parseInt(pruneThresholdStr);
      String[] fileNames = fileName.split(";");
			
      int totalSkipped = 0;
      for (int fi = 0; fi < fileNames.length; ++fi) {
        MessagePrinter.Print("Loading from file: " + fileNames[fi]);
        BufferedReader br = new BufferedReader(new FileReader(fileNames[fi]));
        String line;
        int totalProcessed = 0;
				
        TObjectIntHashMap srcValAlphabet = null;
        if (srcFilterFile != null) {
          srcValAlphabet = LoadValAlphabetFile(srcFilterFile, pruneThreshold);
        }
				
        HashSet trgValFilter = null;
        if (trgFilterFile != null) {
          trgValFilter = LoadFilterFile(trgFilterFile);
        }
				
        while ((line = br.readLine()) != null) {
          ++totalProcessed;
          if (totalProcessed % 100000 == 0) {
            System.out.println("processed so far: " + totalProcessed);
          }
          line.trim();

          // doc feat val
          String[] fields = line.split("\t");
					
          // If a line has only one field, then it is
          // considered a vertex. The vertex is added
          // to the graph and then moved onto next line.
          if (false && fields.length == 1) {
            g.AddVertex(fields[0], "");
            continue;
          }
					
          if (fields.length != 3) {
            fields = line.split(" ");
						
            // check whether space is the delimiter
            if (fields.length != 3) {
              ++totalSkipped;
              continue;
            }
          }
					
          // document node
          String srcNodeNameStr = fields[0];
          if (srcNodeNameStr.trim().length() <= 0) { continue; }
	
          String trgNodeName = fields[1];
          if (trgValFilter != null && !trgValFilter.contains(trgNodeName)) {
            ++totalSkipped;
            continue;
          }
					
          // if source frequency filter is to be applied and the current source
          // node is not present in the map, then skip current edge.
          if (srcFilterFile != null && !srcValAlphabet.containsKey(srcNodeNameStr)) {
            ++totalSkipped;
            continue;
          }
					
          String srcNodeName = fields[0];
          if (srcValAlphabet != null) {
            srcNodeName = Integer.toString(srcValAlphabet.get(srcNodeNameStr));
          }
					
          //				// TODO(partha): need to check whether to make this permanent
          //				// we also do not want numbers to be source/value nodes.
          //				if (Pattern.matches("^[0-9][0-9]*$", srcNodeName) ||
          //					Pattern.matches("^ [ ]*$", srcNodeName) ||
          //					srcNodeName.length() <= 1) {
          //					++totalSkipped;
          //					continue;
          //				}
	
          // doc -> feat
          Vertex dv = g.AddVertex(srcNodeName, Constants.GetDummyLabel());
          dv.AddNeighbor(trgNodeName, Double.parseDouble(fields[2]));
	
          // feat -> doc
          if (!isDirected) {
            Vertex fv = g.AddVertex(trgNodeName, Constants.GetDummyLabel());
            fv.AddNeighbor(srcNodeName, Double.parseDouble(fields[2]));
          }
        }
        br.close();
      }
			
      System.out.println("Total skipped: " + totalSkipped);
			
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }
	
  public static TObjectIntHashMap<String> LoadValAlphabetFile(String fileName,
                                                              int freqThreshold) {
    TObjectIntHashMap retMap = new TObjectIntHashMap();
    try {
      BufferedReader bfr = new BufferedReader(new FileReader(fileName));
      String line;
      int lineNum = 0;
      while ((line = bfr.readLine()) != null) {
        ++lineNum;
        line = line.trim();

        // value_count value_string
        String[] fields = line.split("\t");
        if (fields.length != 2 || fields[0].length() <= 0 || fields[1].length() <= 0 ||
            fields[1].length() > 100) {
          // System.out.println("Skipping:>>" + line + "<<");
          continue;
        }
        int valCnt = Integer.parseInt(fields[0]);
        if (valCnt > freqThreshold) {
          retMap.put(fields[1], lineNum);
        }
      }
      bfr.close();
    } catch(IOException ioe) {
      ioe.printStackTrace();
    }
    System.out.println("Total " + retMap.size() + " loaded from " + fileName);
    return (retMap);
  }
	
  public static HashSet LoadFilterFile(String filterFile) {
    HashSet retSet = new HashSet();
    try {
      BufferedReader bfr = new BufferedReader(new FileReader(filterFile));
      String line;
      while ((line = bfr.readLine()) != null) {
        retSet.add(line);
      }
      bfr.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    System.out.println("Total " + retSet.size() + " loaded from " + filterFile);
    return (retSet);
  }
	
  public static Graph InjectSeedLabels(Graph g, String seedFile,
                                        int maxSeedsPerClass, boolean alsoMarkTest) {
    try {
      //			TObjectIntHashMap totalSeedsPerClass = LoadUniqSeedClassCounts(g, seedFile);
			
      TObjectIntHashMap currSeedsPerClassCount = new TObjectIntHashMap();
      String[] seedFiles = seedFile.split(",");

      for (int fi = 0; fi < seedFiles.length; ++fi) {
        BufferedReader br = new BufferedReader(new FileReader(seedFiles[fi]));
        String line;
        while ((line = br.readLine()) != null) {
          line.trim();
          // node_idx seed_label score
          String[] fields = line.split("\t");
          assert(fields.length == 3);
					
          //					// if a class's overall frequency is lower (or equal) than the
          //					// required seed count, then continue further.
          //					if (totalSeedsPerClass.get(fields[1]) < maxSeedsPerClass) {
          ////						System.out.println("SKIPPING: " + fields[1] + " " +
          ////								totalSeedsPerClass.get(fields[1]));
          //						continue;
          //					}
					
          if (!currSeedsPerClassCount.containsKey(fields[1])) {
            currSeedsPerClassCount.put(fields[1], 0);
          }

          Vertex v = g._vertices.get(fields[0]);
          if (v != null) {
            // update gold label of the current node
            v.SetGoldLabel(fields[1], Double.parseDouble(fields[2]));

            if (currSeedsPerClassCount.get(fields[1]) < maxSeedsPerClass) {
              //							System.out.println("Injecting label for node: " + fields[0]);
							
              // add current label to the node's injected labels if not
              // already present
              if (!v.GetInjectedLabelScores().containsKey(fields[1])) {
                v.SetInjectedLabelScore(fields[1],
                                        Double.parseDouble(fields[2]));
                v.SetSeedNode();
                currSeedsPerClassCount.increment(fields[1]);
              }
            } else if (alsoMarkTest) {
              v.SetTestNode();
            }
          }
        }
        br.close();
      }

      int testNodeCnt = 0;
      int seedNodeCnt = 0;
      for (String vName : g._vertices.keySet()) {
        Vertex v = g._vertices.get(vName);
        if (v.IsTestNode()) {
          ++testNodeCnt;
        }
        if (v.IsSeedNode()) {
          ++seedNodeCnt;
        }
      }
      System.out.println("Total seeded nodes:: " + seedNodeCnt);
      if (alsoMarkTest) {
        System.out.println("Total test nodes:: " + testNodeCnt);
      }
      // System.out.println("Total labels: " + seedsPerClassCount.size());
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }
	
  public static TObjectIntHashMap LoadUniqSeedClassCounts(Graph g, String seedFile) {
    TObjectIntHashMap classFreq = new TObjectIntHashMap();
    try {
      HashSet<String> alreadyProcessed = new HashSet<String>();

      BufferedReader bfr = new BufferedReader(new FileReader(seedFile));
      String line;
      while ((line = bfr.readLine()) != null) {
        // if the current line is a duplicate then continue
        if (alreadyProcessed.contains(line)) {
          continue;
        } else {
          alreadyProcessed.add(line);
        }
        String[] fields = line.split("\t");
        assert (fields.length == 3);
				
        // increment the count only if first field is present in
        // the graph as a node
        if (g._vertices.containsKey(fields[0])) {
          classFreq.adjustOrPutValue(fields[1], 1, 1);
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (classFreq);
  }
	
  public static Graph MarkTestNodes(Graph g, String testFile) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(testFile));
      String id;
      int cnt = 0;
      while ((id = br.readLine()) != null) {
        String[] fields = id.split("\t");
        assert(fields.length == 3);
				
        Vertex v = g._vertices.get(fields[0]);
        assert(v != null);
				
        v.SetGoldLabel(fields[1], Double.parseDouble(fields[2]));
        v.SetTestNode();
        ++cnt;
      }			
      br.close();
			
      System.out.println("Total " + cnt + " nodes marked as test nodes!");
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return (g);
  }


  public static String GetMD5Hash(String input) {
    MessageDigest m = null;
    try {
      m = MessageDigest.getInstance("MD5");
      m.update(input.getBytes(), 0, input.length());
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return (new BigInteger(1, m.digest()).toString(16));
  }

}
