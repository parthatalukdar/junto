package upenn.junto.util;

import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.iterator.TIntDoubleIterator;
import java.io.*;
import java.util.*;

public class RyanFeatureVector implements Comparable, Serializable {

    public int index;
    public double value;
    public RyanFeatureVector next;
    
    public RyanFeatureVector(int i, double v, RyanFeatureVector n) {
	index = i;
	value = v;
	next = n;
    }
    
    public RyanFeatureVector add(String feat, double val, RyanAlphabet dataAlphabet) {
    	int num = dataAlphabet.lookupIndex(feat);
    	if(num >= 0)
    		return new RyanFeatureVector(num,val,this);
    	return this;
    }

    
    public void add(int i1, double v1) {
	
	RyanFeatureVector new_node = new RyanFeatureVector(this.index, this.value, this.next);
	
	this.index = i1;
	this.value = v1;
	this.next = new_node;
	
    }



    public static RyanFeatureVector cat(RyanFeatureVector fv1, RyanFeatureVector fv2) {
	RyanFeatureVector result = new RyanFeatureVector(-1,-1.0,null);
	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    result = new RyanFeatureVector(curr.index,curr.value,result);
	}
	for(RyanFeatureVector curr = fv2; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    result = new RyanFeatureVector(curr.index,curr.value,result);
	}
	return result;
	
    }
    
    // fv1 - fv2
    public static RyanFeatureVector getDistVector(RyanFeatureVector fv1, RyanFeatureVector fv2) {
    	RyanFeatureVector result = new RyanFeatureVector(-1, -1.0, null);
    	for (RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
			if (curr.index < 0)
				continue;
			result = new RyanFeatureVector(curr.index, curr.value, result);
		}
		for (RyanFeatureVector curr = fv2; curr.next != null; curr = curr.next) {
			if (curr.index < 0)
				continue;
			result = new RyanFeatureVector(curr.index, -curr.value, result);
		}
		return result;
    }
    
    public static RyanFeatureVector getAddedVector(RyanFeatureVector fv1, RyanFeatureVector fv2, double rate) {
    	
    	TIntDoubleHashMap hm = new TIntDoubleHashMap();
    	for (RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
    		if (curr.index >= 0) {
    			hm.put(curr.index, (hm.containsKey(curr.index) ? hm.get(curr.index) : 0) + curr.value);
    		}
    	}

    	for (RyanFeatureVector curr = fv2; curr.next != null; curr = curr.next) {
    		if (curr.index >= 0) {
    			hm.put(curr.index, (hm.containsKey(curr.index) ? hm.get(curr.index) : 0) + rate * curr.value);
    		}
    	}

    	RyanFeatureVector result = new RyanFeatureVector(-1, -1, null);
    	TIntDoubleIterator hmIter = hm.iterator();
    	while (hmIter.hasNext()) {
    		hmIter.advance();
    		result = new RyanFeatureVector(hmIter.key(), hmIter.value(), result);
    	}
		return result;
    }
	
    public static double dotProduct(RyanFeatureVector fv1, RyanFeatureVector fv2) {
	double result = 0.0;
	TIntDoubleHashMap hm1 = new TIntDoubleHashMap();
	TIntDoubleHashMap hm2 = new TIntDoubleHashMap();

	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    hm1.put(curr.index,hm1.get(curr.index)+curr.value);
	}
	for(RyanFeatureVector curr = fv2; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    hm2.put(curr.index,hm2.get(curr.index)+curr.value);
	}

	int[] keys = hm1.keys();

	for(int i = 0; i < keys.length; i++) {
	    double v1 = hm1.get(keys[i]);
	    double v2 = hm2.get(keys[i]);
	    result += v1*v2;
	}
		
	return result;
		
    }

    public static double oneNorm(RyanFeatureVector fv1) {
	double sum = 0.0;
	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    sum += curr.value;
	}
	return sum;
    }
	
    public static int size(RyanFeatureVector fv1) {
	int sum = 0;
	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    sum++;
	}
	return sum;
    }
	
    public static double twoNorm(RyanFeatureVector fv1) {
	TIntDoubleHashMap hm = new TIntDoubleHashMap();
	double sum = 0.0;
	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    hm.put(curr.index,hm.get(curr.index)+curr.value);
	}
	int[] keys = hm.keys();

	for(int i = 0; i < keys.length; i++)
	    sum += Math.pow(hm.get(keys[i]),2.0);
		
	return Math.sqrt(sum);
    }

    public static RyanFeatureVector twoNormalize(RyanFeatureVector fv1) {
	return normalize(fv1,twoNorm(fv1));
    }
	

    public static RyanFeatureVector oneNormalize(RyanFeatureVector fv1) {
	return normalize(fv1,oneNorm(fv1));
    }
	
    public static RyanFeatureVector normalize(RyanFeatureVector fv1, double norm) {
	RyanFeatureVector result = new RyanFeatureVector(-1,-1.0,null);
	for(RyanFeatureVector curr = fv1; curr.next != null; curr = curr.next) {
	    if(curr.index < 0)
		continue;
	    result = new RyanFeatureVector(curr.index,curr.value/norm,result);
	}
	return result;
    }

    public String toString() {
	if (next == null)
	   return "" + index + ":" + value;
	return index + ":" + value + " " + next.toString();
    }


    public void sort() {
	ArrayList features = new ArrayList();

	for(RyanFeatureVector curr = this; curr != null; curr = curr.next)
	    if(curr.index >= 0)
		features.add(curr);

	Object[] feats = features.toArray();

	Arrays.sort(feats);

	RyanFeatureVector fv = new RyanFeatureVector(-1,-1.0,null);
	for(int i = feats.length-1; i >= 0; i--) {
	    RyanFeatureVector tmp = (RyanFeatureVector)feats[i];
	    fv = new RyanFeatureVector(tmp.index,tmp.value,fv);
	}
	
	this.index = fv.index;
	this.value = fv.value;
	this.next = fv.next;

    }

    public int compareTo(Object o) {
	RyanFeatureVector fv = (RyanFeatureVector)o;
	if(index < fv.index)
	    return -1;
	if(index > fv.index)
	    return 1;
	return 0;
    }


    public double dotProdoct(double[] weights) {
    	double score = 0.0;
	for(RyanFeatureVector curr = this; curr != null; curr = curr.next) {
	    if (curr.index >= 0)
		score += weights[curr.index]*curr.value;
	}
	return score;
    }
    

}
