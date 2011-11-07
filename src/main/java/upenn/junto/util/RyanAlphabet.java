/* Copyright (C) 2002 Univ. of Massachusetts Amherst, Computer Science Dept.
   This file is part of "MALLET" (MAchine Learning for LanguagE Toolkit).
   http://www.cs.umass.edu/~mccallum/mallet
   This software is provided under the terms of the Common Public License,
   version 1.0, as published by http://www.opensource.org.  For further
   information, see the file `LICENSE' included with this distribution. */

/** 
 @author Andrew McCallum <a href="mailto:mccallum@cs.umass.edu">mccallum@cs.umass.edu</a>
 */

package upenn.junto.util;

import java.util.ArrayList;
import java.io.*;
import java.util.*;

import gnu.trove.map.hash.TObjectIntHashMap;

public class RyanAlphabet implements Serializable {
  TObjectIntHashMap map;
  ArrayList entries;
  boolean growthStopped = false;
  Class entryClass = null;

  public RyanAlphabet(int capacity, Class entryClass) {
    this.map = new TObjectIntHashMap(capacity);
    this.entries = new ArrayList(capacity);
    this.entryClass = entryClass;
  }

  public RyanAlphabet(Class entryClass) {
    this(8, entryClass);
  }

  public RyanAlphabet(int capacity) {
    this(capacity, null);
  }

  public RyanAlphabet() {
    this(8, null);
  }

  public Object clone() {
    //try {
    // Wastes effort, because we over-write ivars we create
    RyanAlphabet ret = new RyanAlphabet();
    ret.map = new TObjectIntHashMap(map);
    ret.entries = (ArrayList) entries.clone();
    ret.growthStopped = growthStopped;
    ret.entryClass = entryClass;
    return ret;
    //} catch (CloneNotSupportedException e) {
    //e.printStackTrace();
    //throw new IllegalStateException ("Couldn't clone InstanceList Vocabuary");
    //}
  }

  /** Return -1 if entry isn't present. */
  public int lookupIndex(Object entry, boolean addIfNotPresent) {
    if (entry == null)
      throw new IllegalArgumentException(
                                         "Can't lookup \"null\" in an RyanAlphabet.");
    if (entryClass == null)
      entryClass = entry.getClass();
    else
      // Insist that all entries in the RyanAlphabet are of the same
      // class.  This may not be strictly necessary, but will catch a
      // bunch of easily-made errors.
      if (entry.getClass() != entryClass)
        throw new IllegalArgumentException("Non-matching entry class, "
                                           + entry.getClass() + ", was " + entryClass);
    int ret = map.get(entry);
    if (!map.containsKey(entry) && !growthStopped && addIfNotPresent) {

      //xxxx: not necessary, fangfang, Aug. 2003
      //			if (entry instanceof String)
      //				entry = ((String)entry).intern();

      ret = entries.size();
      map.put(entry, entries.size());
      entries.add(entry);
    }
    return ret;
  }

  public int lookupIndex(Object entry) {
    return lookupIndex(entry, true);
  }

  public Object lookupObject(int index) {
    return entries.get(index);
  }

  public Object[] toArray() {
    return entries.toArray();
  }

  // xxx This should disable the iterator's remove method...
  public Iterator iterator() {
    return entries.iterator();
  }

  public Object[] lookupObjects(int[] indices) {
    Object[] ret = new Object[indices.length];
    for (int i = 0; i < indices.length; i++)
      ret[i] = entries.get(indices[i]);
    return ret;
  }

  public int[] lookupIndices(Object[] objects, boolean addIfNotPresent) {
    int[] ret = new int[objects.length];
    for (int i = 0; i < objects.length; i++)
      ret[i] = lookupIndex(objects[i], addIfNotPresent);
    return ret;
  }

  public boolean contains(Object entry) {
    return map.contains(entry);
  }

  public int size() {
    return entries.size();
  }

  public void stopGrowth() {
    growthStopped = true;
  }

  public void allowGrowth() {
    growthStopped = false;
  }

  public boolean growthStopped() {
    return growthStopped;
  }

  public Class entryClass() {
    return entryClass;
  }

  /** Return String representation of all RyanAlphabet entries, each
      separated by a newline. */
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < entries.size(); i++) {
      sb.append(entries.get(i).toString());
      sb.append('\n');
    }
    return sb.toString();
  }

  public void dump() {
    dump(System.out);
  }

  public void dump(PrintStream out) {
    for (int i = 0; i < entries.size(); i++) {
      out.println(i + " => " + entries.get(i));
    }
  }

  public void dump(String outputFile) {
    try {
      BufferedWriter bwr = new BufferedWriter(new FileWriter(outputFile));
      for (int i = 0; i < entries.size(); i++) {
        bwr.write(entries.get(i) + "\t" + map.get(entries.get(i)) + "\n");
      }
      bwr.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  // Serialization 

  private static final long serialVersionUID = 1;
  private static final int CURRENT_SERIAL_VERSION = 0;

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(CURRENT_SERIAL_VERSION);
    out.writeInt(entries.size());
    for (int i = 0; i < entries.size(); i++)
      out.writeObject(entries.get(i));
    out.writeBoolean(growthStopped);
    out.writeObject(entryClass);
  }

  private void readObject(ObjectInputStream in) throws IOException,
                                                       ClassNotFoundException {
    int version = in.readInt();
    int size = in.readInt();
    entries = new ArrayList(size);
    map = new TObjectIntHashMap(size);
    for (int i = 0; i < size; i++) {
      Object o = in.readObject();
      map.put(o, i);
      entries.add(o);
    }
    growthStopped = in.readBoolean();
    entryClass = (Class) in.readObject();
  }

  //   public String toString()
  //  {
  // 	return Arrays.toString(map.keys());
  //}
}
