----------------------------------------------------------
The Junto Label Propagation Toolkit

Author: Partha Pratim Talukdar (partha@talukdar.net)
Contributors: Jason Baldridge (jbaldrid@mail.utexas.edu)
----------------------------------------------------------


Introduction
============

This package provides an implementation of the Adsorption and 
Modified Adsorption (MAD) algorithms described in the following 
papers.

  Weakly Supervised Acquisition of Labeled Class Instances using Graph
  Random Walks. Talukdar et al., EMNLP 2008
  
  New Regularized Algorithms for Transductive Learning. Partha Pratim
  Talukdar, Koby Crammer, ECML-PKDD 2009
  
  Experiments in Graph-based Semi-Supervised Learning Methods for
  Class-Instance Acquisition. Partha Pratim Talukdar, Fernando Pereira,
  ACL 2010

Please cite Talukdar and Crammer (2009) and/or Talukdar and Pereira
(2010) if you use this library.

Additionally, LP_ZGL, one of the first label propagation algorithms 
is also implemented.

  Xiaojin Zhu and Zoubin Ghahramani. Learning from labeled and
  unlabeled data with label propagation.  Technical Report
  CMU-CALD-02-107, Carnegie Mellon University, 2002.


This file contains the configuration and build instructions. 

Why is the toolkit named Junto? The core code was written while Partha
Talukdar was at the University of Pennsylvania, and Ben Franklin (the
founder of the University) established a club called Junto that
provided a structured forum for him and his friends to debate and
exchange knowledge:

http://en.wikipedia.org/wiki/Junto_(club)

This has a nice parallel with how label propagation works: nodes are
connected and influence each other based on their connections. Also
"junto" means "along" and "together" in a number of Latin languages,
and carries the connotation of cooperation---also a good fit for label
propagation.


Requirements
============

* Version 1.6 of the Java 2 SDK (http://java.sun.com)


Configuring your environment variables
======================================

The easiest thing to do is to set the environment variables JAVA_HOME
and JUNTO_DIR to the relevant locations on your system. Set JAVA_HOME
to match the top level directory containing the Java installation you
want to use.

For example, on Windows:

C:\> set JAVA_HOME=C:\Program Files\jdk1.5.0_04

or on Unix:

% setenv JAVA_HOME /usr/local/java
  (csh)
> export JAVA_HOME=/usr/java
  (ksh, bash)

On Windows, to get these settings to persist, it's actually easiest to
set your environment variables through the System Properties from the
Control Panel. For example, under WinXP, go to Control Panel, click on
System Properties, choose the Advanced tab, click on Environment
Variables, and add your settings in the User variables area.

Next, likewise set JUNTO_DIR to be the top level directory where you
unzipped the download. In Unix, type 'pwd' in the directory where
this file is and use the path given to you by the shell as
JUNTO_DIR.  You can set this in the same manner as for JAVA_HOME
above.

Next, add the directory JUNTO_DIR/bin to your path. For example, you
can set the path in your .bashrc file as follows:

export PATH="$PATH:$JUNTO_DIR/bin"

On Windows, you should also add the python main directory to your path.

Once you have taken care of these three things, you should be able to
build and use the Junto Library.

Note: Spaces are allowed in JAVA_HOME but not in JUNTO_DIR.  To set
an environment variable with spaces in it, you need to put quotes around
the value when on Unix, but you must *NOT* do this when under Windows.


Building the system from source
===============================

Junto uses SBT (Simple Build Tool) with a standard directory
structure.  To build Junto, go to JUNTO_DIR and type:

$ bin/build update compile

This will compile the source files and put them in
./target/classes. If this is your first time running it, you will see
messages about Scala being dowloaded -- this is fine and
expected. Once that is over, the Junto code will be compiled.

To try out other build targets, do:

$ bin/build

This will drop you into the SBT interface.  The build targets that are
supported are listeded here:

https://github.com/harrah/xsbt/wiki/Getting-Started-Running

Note: if you have SBT already installed on your system, you can also
just call it directly with "sbt".

If you wish to use Junto as an API, you can create a self-contained
assembly jar by using the "assembly" action in SBT. Also, you can just do:

$ bin/build assembly


Trying it out
=============

If you've managed to configure and build the system, you should be 
able to go to $JUNTO_DIR/examples/simple and run:

$ junto config simple_config

Please look into the examples/simple/simple_config file for various 
options available. Sample (dummy) data is made available in the 
examples/simple/data directory.

A more extensive example on prepositional phrase attachment is in
examples/ppa. See the README in that directory for more details.

Hadoop
======

If you are interested in trying out the Hadoop implementations, then please 
look into examples/hadoop/README


Bug Reports
===========

Please report bugs on the GitHub site: 

  https://github.com/parthatalukdar/junto


Getting help
============

Documentation is admittedly thin. If you get stuck, you can get help
by posting questions to the junto-open group:

http://groups.google.com/group/junto-open
