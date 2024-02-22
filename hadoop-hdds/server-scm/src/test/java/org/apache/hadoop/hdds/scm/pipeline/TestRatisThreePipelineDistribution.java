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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test fair pipeline distribution for non-HA SCM cluster.
 */
public class TestRatisThreePipelineDistribution {
  private static final Logger LOG = LoggerFactory.getLogger(TestRatisThreePipelineDistribution.class);

  // todo: setup log level from config file
  @BeforeAll
  public static void setup() {
    GenericTestUtils.setLogLevel(PipelinePlacementPolicy.LOG, Level.DEBUG);
  }

  private static Stream<Arguments> createTestRatisThreeSCMs() throws IOException {
    return Stream.of(
        Arguments.of(createTestableSCM(4, 30)),
        Arguments.of(createTestableSCM(5, 30)),
        Arguments.of(createTestableSCM(6, 30)),
        Arguments.of(createTestableSCM(7, 30)),
        Arguments.of(createTestableSCM(8, 30)),
        Arguments.of(createTestableSCM(9, 30)),
        Arguments.of(createTestableSCM(10, 30)),
        Arguments.of(createTestableSCM(11, 30)),
        Arguments.of(createTestableSCM(13, 50)),
        Arguments.of(createTestableSCM(15, 100)),
        Arguments.of(createTestableSCM(17, 100)),
        Arguments.of(createTestableSCM(20, 150)));
  }

  private static TestableSCM createTestableSCM(int datanodeCount, int dnPipelineLimit)
      throws IOException {
    ClusterWithoutRackAwareness cluster = new ClusterWithoutRackAwareness(datanodeCount);
    return new TestableSCM.Builder()
        .createTestDir(TestRatisThreePipelineDistribution.class.getSimpleName() + UUID.randomUUID())
        .setOzoneConfig((testDir) -> {
          OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
          conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, dnPipelineLimit);
          conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN, 10, StorageUnit.MB);
          return conf;
        })
        .createDbStore()
        .setCluster(() -> new ClusterWithoutRackAwareness(datanodeCount))
        .setNodeManager(() -> new MockNodeManager(cluster.getTopology(), cluster.getDatanodeList(),
            false, datanodeCount))
        .build();
  }

  @ParameterizedTest(name = "RatisThreeSCM #{index}: {0}")
  @MethodSource("createTestRatisThreeSCMs")
  public void greedyPipelineCreation(@Nonnull TestableSCM scm) throws IOException {
    List<DatanodeDetails> healthyNodes = scm.getNodeManager().getNodes(NodeStatus.inServiceHealthy());
    int nodeCount = greegyPipelineCreation(scm, healthyNodes);
    /*
     * Make sure that we create pipelines in a fair way: each DN participates in as many pipelines as possible.
     * */
    int totalPipelineCount = scm.getTotalPipelineCount();
    assertEquals(totalPipelineCount, MathUtils.combinationWithoutRepetitions(nodeCount, 3));
    for (int i = 0; i < nodeCount; ++i) {
      DatanodeDetails dn = healthyNodes.get(i);
      assertEquals(
          currentRatisThreePipelineCount(scm.getPipelineManager(), scm.getNodeManager(), dn),
          MathUtils.combinationWithoutRepetitions(nodeCount - 1, 2));
    }
  }

  @ParameterizedTest(name = "RatisThreeSCM #{index}: {0}")
  @MethodSource("createTestRatisThreeSCMs")
  public void fairPipelineDistributionWhenTwoNodesAdded(@Nonnull TestableSCM scm) throws IOException {
    List<DatanodeDetails> healthyNodes = scm.getNodeManager().getNodes(NodeStatus.inServiceHealthy());
    int nodeCount = greegyPipelineCreation(scm, healthyNodes);

    int totalPipelineCount = scm.getTotalPipelineCount();
    assertEquals(totalPipelineCount, MathUtils.combinationWithoutRepetitions(nodeCount, 3));

    scm.getNodeManager().addDatanode(ClusterWithoutRackAwareness.createDatanodeUtils());
    scm.getNodeManager().addDatanode(ClusterWithoutRackAwareness.createDatanodeUtils());

    // Add 20 RATIS_THREE pipelines
    for (int i = 0; i < 20; ++i) {
      scm.addRatisThreePipeline();
    }

    verifyPipelineCountForNodes(scm.getPipelineManager(), scm.getNodeManager());
  }

  public static void verifyPipelineCountForNodes(
      @Nonnull PipelineManager pipelineManager,
      @Nonnull NodeManager nodeManager
  ) {
    /*
     * Make sure that:
     * 1. The difference between the number of pipelines for all DN pairs is less than THREE.
     * 2. Could be only TWO DNs which has the least number of pipelines due to RATIS THREE algo selection. If so,
     *    than these DNs has to participate in an equal number of pipelines.
     * */
    List<DatanodeDetails> nodeList = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    int nodeCount = nodeList.size();

    int maxPipelineCount = 0;
    int[] pipelinesCountPerDn = new int[nodeCount];
    for (int i = 0; i < nodeCount; ++i) {
      pipelinesCountPerDn[i] = currentRatisThreePipelineCount(pipelineManager, nodeManager, nodeList.get(i));
      maxPipelineCount = Math.max(pipelinesCountPerDn[i], maxPipelineCount);
    }

    Set<Integer> nodeIndexesWithPipelineCountDiff = new HashSet<>();
    for (int i = 0; i < nodeCount - 1; ++i) {
      for (int j = i + 1; j < nodeCount; ++j) {
        int pipelineCountDiff = Math.abs(pipelinesCountPerDn[i] - pipelinesCountPerDn[j]);
        if (pipelineCountDiff >= 3) {
          if (pipelinesCountPerDn[i] > pipelinesCountPerDn[j]) {
            nodeIndexesWithPipelineCountDiff.add(j);
          } else {
            nodeIndexesWithPipelineCountDiff.add(i);
          }
        }
      }
    }
    Integer[] nodeIndexesArr = nodeIndexesWithPipelineCountDiff.toArray(new Integer[0]);

    int maxNodeCountWithTheLeastNumberOfPipelines = nodeIndexesArr.length;
    assertThat(maxNodeCountWithTheLeastNumberOfPipelines).isGreaterThanOrEqualTo(0);
    assertThat(maxNodeCountWithTheLeastNumberOfPipelines).isLessThanOrEqualTo(2);
    switch (maxNodeCountWithTheLeastNumberOfPipelines) {
    case 1:
      // todo: Uncomment to make TestMiniOzoneClusterFairPipelineDistribution fail
      int nodeIdx = nodeIndexesArr[0];
      assertThat(pipelinesCountPerDn[nodeIdx]).isGreaterThan(0);
      break;
    case 2:
      int firstNodeIdx = nodeIndexesArr[0];
      int firstNodePipelineCount = pipelinesCountPerDn[firstNodeIdx];
      int secondNodeIndex = nodeIndexesArr[1];
      int secondNodePipelineCount = pipelinesCountPerDn[secondNodeIndex];
      assertEquals(firstNodePipelineCount, secondNodePipelineCount);

      // todo: Uncomment to make TestMiniOzoneClusterFairPipelineDistribution fail
      assertThat(firstNodePipelineCount).isGreaterThan(0);
      assertThat(secondNodePipelineCount).isGreaterThan(0);
      break;
    default:
      break;
    }
  }

  private static int greegyPipelineCreation(
      @Nonnull TestableSCM scm,
      @Nonnull List<DatanodeDetails> healthyNodes
  ) throws IOException {
    int nodeCount = healthyNodes.size();
    for (int i = 0; i < nodeCount - 2; ++i) {
      DatanodeDetails dn1 = healthyNodes.get(i);
      for (int j = i + 1; j < nodeCount - 1; ++j) {
        DatanodeDetails dn2 = healthyNodes.get(j);
        for (int k = j + 1; k < nodeCount; ++k) {
          DatanodeDetails dn3 = healthyNodes.get(k);
          scm.addRatisThreePipeline(dn1, dn2, dn3);
        }
      }
    }
    return nodeCount;
  }

  private static int currentRatisThreePipelineCount(
      @Nonnull PipelineManager pipelineManager,
      @Nonnull NodeManager nodeManager,
      @Nonnull DatanodeDetails dnA
  ) {
    // Safe to cast collection's size to int
    return (int) nodeManager.getPipelines(dnA).stream()
        .map(id -> {
          try {
            return pipelineManager.getPipeline(id);
          } catch (PipelineNotFoundException e) {
            LOG.debug("Pipeline not found in pipeline state manager during pipeline creation. PipelineID: {}", id, e);
            return null;
          }
        })
        .filter(PipelinePlacementPolicy::isNonClosedRatisThreePipeline)
        .count();
  }
}
