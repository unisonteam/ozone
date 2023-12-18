/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

/**
 * Testable cluster without rack awareness.
 */
public class ClusterWithoutRackAwareness {
  private static int nodeId = 0;
  private final List<DatanodeDetails> dnDetailsList;
  private final NetworkTopologyImpl topology;
  private final String clusterID;

  public ClusterWithoutRackAwareness(int numberOfNodes) {
    dnDetailsList = new ArrayList<>(numberOfNodes);
    topology = initTopology();
    clusterID = UUID.randomUUID().toString();

    // create datanodes and add containers to them
    for (int i = 0; i < numberOfNodes; i++) {
      dnDetailsList.add(i, createDatanodeUtils());
    }
  }

  public @Nonnull List<DatanodeDetails> getDatanodeList() {
    return dnDetailsList;
  }

  public @Nonnull NetworkTopologyImpl getTopology() {
    return topology;
  }

  private NetworkTopologyImpl initTopology() {
    NodeSchema[] schemas = new NodeSchema[]{ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    return new NetworkTopologyImpl(NodeSchemaManager.getInstance());
  }

  public static @Nonnull DatanodeDetails createDatanodeUtils() {
    return buildNode(MockDatanodeDetails.randomDatanodeDetails(), buildNodeWithDefaultCost());
  }

  public static @Nonnull DatanodeDetails buildNode(@Nonnull DatanodeDetails dn, @Nonnull Node node) {
    return DatanodeDetails.newBuilder()
        .setUuid(dn.getUuid())
        .setHostName(dn.getHostName())
        .setIpAddress(dn.getIpAddress())
        .addPort(dn.getPort(DatanodeDetails.Port.Name.STANDALONE))
        .addPort(dn.getPort(DatanodeDetails.Port.Name.RATIS))
        .addPort(dn.getPort(DatanodeDetails.Port.Name.REST))
        .setNetworkLocation(node.getNetworkLocation())
        .build();
  }

  public static @Nonnull Node buildNodeWithDefaultCost() {
    return new NodeImpl("h" + nodeId++, "/r1", NetConstants.NODE_COST_DEFAULT);
  }

  public String getClusterId() {
    return clusterID;
  }
}
