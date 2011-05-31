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

class Edge (val source: String, val target: String, val weight: Double) {
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
      val Array(source, target, weight) = line.trim split("\t")
      new Edge(source, target, weight.toDouble)
    }).toList
  }
}

class Seed (val vertex: String, val label: String, val score: Double) {
  override def toString = vertex + "\t" + label + "\t" + score
}

object SeedCreator {
  def apply (vertex: String, label: String) = new Seed(vertex, label, 1.0)
}

object SeedFileWriter {
  import java.io._
  def apply (seeds: List[Seed], seedFile: String) = {
    val out = new FileWriter(new File(seedFile))
    seeds foreach { seed => out.write(seed.toString + "\n") }
    out.flush
    out.close
  }
}

object SeedFileReader {
  def apply (filename: String): List[Seed] = {
    (for (line <- io.Source fromFile(filename) getLines) yield {
      val Array(vertex, label, score) = line.trim split("\t")
      new Seed(vertex, label, score.toDouble)
    }).toList
  }
}
