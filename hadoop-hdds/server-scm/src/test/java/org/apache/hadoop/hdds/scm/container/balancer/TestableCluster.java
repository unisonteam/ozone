/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Class is used for creating test cluster with a required number of datanodes.
 * <p>
 * Create an unbalanced cluster by generating some data.
 * <p>
 * Nodes in the cluster have utilization values determined by
 * generateUtilization method.
 */
final class TestableCluster {
  static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestableCluster.class);
  public static final long STORAGE_UNIT = OzoneConsts.GB;
  private final int nodeCount;
  private final double[] nodeUtilizationList;
  private final DatanodeUsageInfo[] nodesInCluster;
  private final Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private final Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
      new HashMap<>();
  private final Map<DatanodeUsageInfo, Set<ContainerID>>
      datanodeToContainersMap = new HashMap<>();
  private final double averageUtilization;

  TestableCluster(int numberOfNodes) {
    nodeCount = numberOfNodes;
    nodeUtilizationList = createUtilizationList(nodeCount);
    nodesInCluster = new DatanodeUsageInfo[nodeCount];

    generateData();
    createReplicasForContainers();
    long clusterCapacity = 0, clusterUsedSpace = 0;

    // for each node utilization, calculate that datanode's used space and
    // capacity
    for (int i = 0; i < nodeUtilizationList.length; i++) {
      long datanodeUsedSpace = 0, datanodeCapacity = 0;
      Set<ContainerID> containerIDSet =
          datanodeToContainersMap.get(nodesInCluster[i]);

      for (ContainerID containerID : containerIDSet) {
        datanodeUsedSpace += cidToInfoMap.get(containerID).getUsedBytes();
      }

      // use node utilization and used space to determine node capacity
      if (nodeUtilizationList[i] == 0) {
        datanodeCapacity = STORAGE_UNIT * RANDOM.nextInt(10, 60);
      } else {
        datanodeCapacity = (long) (datanodeUsedSpace / nodeUtilizationList[i]);
      }
      SCMNodeStat stat = new SCMNodeStat(datanodeCapacity, datanodeUsedSpace,
          datanodeCapacity - datanodeUsedSpace);
      nodesInCluster[i].setScmNodeStat(stat);
      clusterUsedSpace += datanodeUsedSpace;
      clusterCapacity += datanodeCapacity;
    }

    averageUtilization = (double) clusterUsedSpace / clusterCapacity;
  }

  Map<DatanodeUsageInfo, Set<ContainerID>> getDatanodeToContainersMap() {
    return datanodeToContainersMap;
  }

  @Nonnull Map<ContainerID, ContainerInfo> getCidToInfoMap() {
    return cidToInfoMap;
  }

  int getNodeCount() {
    return nodeCount;
  }

  double getAverageUtilization() {
    return averageUtilization;
  }

  DatanodeUsageInfo[] getNodesInCluster() {
    return nodesInCluster;
  }

  double[] getNodeUtilizationList() {
    return nodeUtilizationList;
  }

  @Nonnull Map<ContainerID, Set<ContainerReplica>> getCidToReplicasMap() {
    return cidToReplicasMap;
  }

  /**
   * Determines unBalanced nodes, that is, over and under utilized nodes,
   * according to the generated utilization values for nodes and the threshold.
   *
   * @param threshold a percentage in the range 0 to 100
   * @return list of DatanodeUsageInfo containing the expected(correct)
   * unBalanced nodes.
   */
  @Nonnull List<DatanodeUsageInfo> getUnBalancedNodes(double threshold) {
    threshold /= 100;
    double lowerLimit = averageUtilization - threshold;
    double upperLimit = averageUtilization + threshold;

    // use node utilizations to determine over and under utilized nodes
    List<DatanodeUsageInfo> expectedUnBalancedNodes = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      if (nodeUtilizationList[i] > upperLimit) {
        expectedUnBalancedNodes.add(nodesInCluster[i]);
      }
    }
    for (int i = 0; i < nodeCount; i++) {
      if (nodeUtilizationList[i] < lowerLimit) {
        expectedUnBalancedNodes.add(nodesInCluster[i]);
      }
    }
    return expectedUnBalancedNodes;
  }

  /**
   * Create some datanodes and containers for each node.
   */
  private void generateData() {
    // create datanodes and add containers to them
    for (int i = 0; i < nodeCount; i++) {
      DatanodeUsageInfo usageInfo =
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              new SCMNodeStat());
      nodesInCluster[i] = usageInfo;

      // create containers with varying used space
      Set<ContainerID> containerIDSet = new HashSet<>();
      int sizeMultiple = 0;
      for (int j = 0; j < i; j++) {
        sizeMultiple %= 5;
        sizeMultiple++;
        ContainerInfo container =
            createContainer((long) i * i + j, sizeMultiple);

        cidToInfoMap.put(container.containerID(), container);
        containerIDSet.add(container.containerID());

        // create initial replica for this container and add it
        Set<ContainerReplica> containerReplicaSet = new HashSet<>();
        containerReplicaSet.add(createReplica(container.containerID(),
            usageInfo.getDatanodeDetails(), container.getUsedBytes()));
        cidToReplicasMap.put(container.containerID(), containerReplicaSet);
      }
      datanodeToContainersMap.put(usageInfo, containerIDSet);
    }
  }

  private @Nonnull ContainerInfo createContainer(long id, int multiple) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder()
        .setContainerID(id)
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestContainerBalancer")
        .setUsedBytes(STORAGE_UNIT * multiple);

    /*
    Make it a RATIS container if id is even, else make it an EC container
     */
    if (id % 2 == 0) {
      builder.setReplicationConfig(RatisReplicationConfig
          .getInstance(HddsProtos.ReplicationFactor.THREE));
    } else {
      builder.setReplicationConfig(new ECReplicationConfig(3, 2));
    }

    return builder.build();
  }


  /**
   * Create the required number of replicas for each container. Note that one
   * replica already exists and nodes with utilization value 0 should not
   * have any replicas.
   */
  private void createReplicasForContainers() {
    for (ContainerInfo container : cidToInfoMap.values()) {
      // one replica already exists; create the remaining ones
      ReplicationConfig replicationConfig = container.getReplicationConfig();
      for (int i = 0; i < replicationConfig.getRequiredNodes() - 1; i++) {
        // randomly pick a datanode for this replica
        int dnIndex = RANDOM.nextInt(0, nodeCount);
        // don't put replicas in DNs that are supposed to have 0 utilization
        if (Math.abs(nodeUtilizationList[dnIndex] - 0.0d) > 0.00001) {
          DatanodeDetails node = nodesInCluster[dnIndex].getDatanodeDetails();
          ContainerID key = container.containerID();
          Set<ContainerReplica> replicas = cidToReplicasMap.get(key);
          replicas.add(createReplica(key, node, container.getUsedBytes()));
          cidToReplicasMap.put(key, replicas);
          datanodeToContainersMap.get(nodesInCluster[dnIndex]).add(key);
        }
      }
    }
  }


  /**
   * Generates a range of equally spaced utilization(that is, used / capacity)
   * values from 0 to 1.
   *
   * @param count Number of values to generate. Count must be greater than or
   *              equal to 1.
   * @return double array of node utilization values
   * @throws IllegalArgumentException If the value of the parameter count is
   *                                  less than 1.
   */
  private static double[] createUtilizationList(int count)
      throws IllegalArgumentException {
    if (count < 1) {
      LOG.warn("The value of argument count is {}. However, count must be " +
          "greater than 0.", count);
      throw new IllegalArgumentException();
    }
    double[] result = new double[count];
    for (int i = 0; i < count; i++) {
      result[i] = (i / (double) count);
    }
    return result;
  }

  private @Nonnull ContainerReplica createReplica(
      @Nonnull ContainerID containerID,
      @Nonnull DatanodeDetails datanodeDetails,
      long usedBytes
  ) {
    return ContainerReplica.newBuilder()
        .setContainerID(containerID)
        .setContainerState(
            StorageContainerDatanodeProtocolProtos.
                ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(datanodeDetails.getUuid())
        .setSequenceId(1000L)
        .setBytesUsed(usedBytes)
        .build();
  }
}
