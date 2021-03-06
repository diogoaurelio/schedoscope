/**
 * Copyright 2017 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.util.model;

import org.schedoscope.metascope.model.MetascopeTable;

import java.util.ArrayList;
import java.util.List;

public class Node implements Comparable<Node> {

  private MetascopeTable table;
  private int id;
  private int level;
  private int distanceToCentralNode;
  private List<Node> nexts;
  private List<Node> previous;

  public Node() {
    this.nexts = new ArrayList<>();
    this.previous = new ArrayList<>();
  }

  public MetascopeTable getTable() {
    return table;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setTable(MetascopeTable table) {
    this.table = table;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public int getDistanceToCentralNode() {
    return distanceToCentralNode;
  }

  public void setDistanceToCentralNode(int distanceToCentralNode) {
    this.distanceToCentralNode = distanceToCentralNode;
  }

  public List<Node> getNexts() {
    return nexts;
  }

  public void setNexts(List<Node> nexts) {
    this.nexts = nexts;
  }

  public List<Node> getPrevious() {
    return previous;
  }

  public void setPrevious(List<Node> previous) {
    this.previous = previous;
  }

  public void addToNexts(Node node) {
    if (this.nexts == null) {
      this.nexts = new ArrayList<>();
    }
    this.nexts.add(node);
  }

  public void addToPrevious(Node node) {
    if (this.previous == null) {
      this.previous = new ArrayList<>();
    }
    this.previous.add(node);
  }

  @Override
  public int compareTo(Node o) {
    return o.getDistanceToCentralNode() - getDistanceToCentralNode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Node node = (Node) o;

    return table.getFqdn().equals(node.table.getFqdn());
  }

  @Override
  public int hashCode() {
    return table.hashCode();
  }
}
