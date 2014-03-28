package upenn.junto.algorithm.mad_sketch;

import java.util.Iterator;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectDoubleIterator;
import gnu.trove.TObjectFloatHashMap;
// import upenn.junto.graph.ILabelScores;
import upenn.junto.util.RyanAlphabet;
// import upenn.junto.util.CollectionUtil;
import upenn.junto.util.Constants;
import upenn.junto.util.MessagePrinter;

public class CountMinSketchLabelManager {
	private RyanAlphabet la;
	public CountMinSketch cms;
	private int[] dummyLabel;
	
	public CountMinSketchLabelManager(int depth, int width) {
		la = new RyanAlphabet(String.class);
		la.allowGrowth();
		
		// add dummy label
		la.lookupIndex(Constants.GetDummyLabel(), true);
		
		cms = new CountMinSketch(depth, width, System.currentTimeMillis());
		dummyLabel = getLabelHash(Constants.GetDummyLabel());
	}
	
	public CountMinSketchLabel GetDummyLabelDist() {
		CountMinSketchLabel cmsl = new CountMinSketchLabel(cms.depth, cms.width);
		for (int di = 0; di < dummyLabel.length; ++di) {
			cmsl.table[di][dummyLabel[di]] = 1;
		}
		return (cmsl);
	}
	
	public int[] GetDummyLabel() {
		return (dummyLabel);
	}
	
	public CountMinSketchLabel getEmptyLabelDist(){
		return (getEmptyLabelDist(cms.depth, cms.width));
	}
	
	public static CountMinSketchLabel getEmptyLabelDist(int depth, int width){
		return (new CountMinSketchLabel(depth, width));
	}

	public int[] getLabelHash(String strLabel) {
//		if (!la.contains(strLabel)) {
//			MessagePrinter.PrintAndDie("UNKNOWN LABEL: " + strLabel);
//		}
		int[] h = new int[cms.depth];
		for (int di = 0; di < cms.depth; ++di) {
			// h[di] = cms.hash(strLabel, di);
			h[di] = cms.hash((long) la.lookupIndex(strLabel) + 1, di);
			
			// System.out.println("LABEL HASH: " + strLabel + " " + h[di]);
		}
		// System.out.print("\n");
		return (h);
	}
	
	public float getScore(CountMinSketchLabel lab, String label) {
		return (getScore(lab, getLabelHash(label)));
	}
	
	public boolean contains(CountMinSketchLabel lab, String label) {
		return (getScore(lab, getLabelHash(label)) > 0);
	}
	
	private float getScore(CountMinSketchLabel lab, int[] labHash) {
		float score = Float.MAX_VALUE;
		for (int di = 0; di < lab.depth; ++di) {
			score = Math.min(score, lab.table[di][labHash[di]]);
		}
		return (score);
	}
	
	public static void add(CountMinSketchLabel cmsl1, float mult1,
							CountMinSketchLabel cmsl2, float mult2) {
		assert(cmsl1 != null);
		if (cmsl2 == null) { return; }
		for (int di = 0; di < cmsl1.depth; ++di) {
			for (int wi = 0; wi < cmsl1.width; ++wi) {
				cmsl1.table[di][wi] =
						mult1 * cmsl1.table[di][wi] + mult2 * cmsl2.table[di][wi];
			}
		}
	}
	
	public void add(CountMinSketchLabel cmsl1, float mult1, String label, float mult2) {
		if (mult2 == 0) { return; }

		int[] labHash = this.getLabelHash(label);
		if (!la.contains(label)) { la.lookupIndex(label, true); }
		
		add(cmsl1, mult1, labHash, mult2);
	}
	
	private static void add(CountMinSketchLabel cmsl1, float mult1,
														int[] labelHash, float mult2) {
		for (int di = 0; di < cmsl1.depth; ++di) {
			cmsl1.table[di][labelHash[di]] =
						mult1 * cmsl1.table[di][labelHash[di]] + mult2;
		}
	}

	public static void divScores(CountMinSketchLabel lab, double divisor) {
		assert (divisor > 0);
		
		for (int di = 0; di < lab.depth; ++di) {
			for (int wi = 0; wi < lab.width; ++wi) {
				lab.table[di][wi] /= divisor;
			}
		}
	}
	
	public static CountMinSketchLabel clear(CountMinSketchLabel lab) {
		for (int di = 0; di < lab.depth; ++di) {
			for (int wi = 0; wi < lab.width; ++wi) {
				lab.table[di][wi] = 0;
			}
		}
		return (lab);
	}
	
	public static boolean isEmpty(CountMinSketchLabel lab) {
		boolean isEmpty = true;
		for (int di = 0; di < lab.depth; ++di) {
			for (int wi = 0; wi < lab.width; ++wi) {
				if (lab.table[di][wi] != 0) {
					isEmpty = false;
					break;
				}
			}
			if (!isEmpty) { break; }
		}
		return (isEmpty);
	}
	
	public static CountMinSketchLabel clone(CountMinSketchLabel inp) {
		CountMinSketchLabel res = new CountMinSketchLabel(inp.depth, inp.width);
		res.table = inp.table.clone();		
		return res;
	}
	
	public TObjectDoubleHashMap getLabelScores(CountMinSketchLabel labelScores) {
		TObjectDoubleHashMap ret = new TObjectDoubleHashMap();
		
		if (labelScores != null) {
			Iterator labIter = la.iterator();
			while (labIter.hasNext()) {
				String lab = (String) labIter.next();

				float score = getScore(labelScores, getLabelHash(lab));
				if (score > 0) {
					ret.put(lab, score);
				}
			}
		}
		return (ret);
	}
	
	public TObjectFloatHashMap getLabelScores2(CountMinSketchLabel labelScores) {
		TObjectFloatHashMap ret = new TObjectFloatHashMap();
		
		Iterator labIter = la.iterator();
		while (labIter.hasNext()) {
			String lab = (String) labIter.next();
			
			float score = getScore(labelScores, getLabelHash(lab));
			if (score > 0) {
				ret.put(lab, score);
			}
		}
		return (ret);
	}
	
//	public String printPrettyLabels(CountMinSketchLabel labelScores) {
//		TObjectDoubleHashMap<String> stringLabelScores = new TObjectDoubleHashMap<String>();
//		
//		TObjectDoubleIterator<Integer> intLabIter = labelScores.getLabels().iterator();
//		while (intLabIter.hasNext()) {
//			intLabIter.advance();
//			// System.out.println(">> " + intLabIter.key());
//			int intLab = intLabIter.key().intValue();
//			String strLab = "UNK_" + intLab;
//			if (intLab >= 0 && intLab < la.size()) {
//				strLab = (String) la.lookupObject(intLab);
//			}
//			stringLabelScores.put(strLab, intLabIter.value());
//		}
//		// System.out.println("");
//		return (CollectionUtil2.Map2String(stringLabelScores));
//	}
	
	public Class<CountMinSketchLabel> getLabelType() {
		return CountMinSketchLabel.class;
	}
	
	public String toString() {
		return (la.toString());
	}
}
