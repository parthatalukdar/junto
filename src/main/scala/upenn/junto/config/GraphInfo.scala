package upenn.junto.config

/**
 * Copyright 2011 Jason Baldridge
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class Edge (val source: String, val target: String, val weight: Double) extends Serializable {
  override def toString = source + "\t" + target + "\t" + weight
}

object EdgeCreator {
  def apply (source: String, target: String) = new Edge(source, target, 1.0)
}

object EdgeFileWriter {
  import java.io._
  def apply (edges: List[Edge], edgeFile: String) = {
    val out = new FileWriter(new File(edgeFile))
    edges foreach { edge => out.write(edge.toString + "\n") }
    out.flush
    out.close
  }
}

object EdgeFileReader {
  def apply (filename: String): List[Edge] = {
    (for (line <- io.Source fromFile(filename) getLines) yield {
      // source target edge_weight
      val fields = line.trim split("\t")
      assert(fields.length == 3, "Invalid entry in graph file: " + line)
      new Edge(fields(0), fields(1), fields(2).toDouble)
    }).toList
  }
}

class Label (val vertex: String, val label: String, val score: Double) extends Serializable {
  override def toString = vertex + "\t" + label + "\t" + score
}

object LabelCreator {
  def apply (vertex: String, label: String) = new Label(vertex, label, 1.0)
}

object LabelFileWriter {
  import java.io._
  def apply (seeds: List[Label], seedFile: String) = {
    val out = new FileWriter(new File(seedFile))
    seeds foreach { seed => out.write(seed.toString + "\n") }
    out.flush
    out.close
  }
}

object LabelFileReader {
  def apply (filename: String): List[Label] = {
    (for (line <- io.Source fromFile(filename) getLines) yield {
      val Array(vertex, label, score) = line.trim split("\t")
      new Label(vertex, label, score.toDouble)
    }).toList
  }
}
