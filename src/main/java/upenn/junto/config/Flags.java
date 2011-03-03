package upenn.junto.config;

/**
 * Copyright 2011 Partha Talukdar
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

/**
 * Tests for configuration flags.
 */
public class Flags {
	
  public static boolean IsOriginalMode(String mode) {
    return (mode.equals("original")) ? true : false;
  }

  public static boolean IsModifiedMode(String mode) {
    return (mode.equals("modified")) ? true : false;
  }

  public static boolean IsColumnNode(String nodeName) {
    return (nodeName.startsWith("C#"));
  }
	
}
