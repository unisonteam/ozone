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

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.balancer.iteration.ContainerBalanceIteration;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancerTask {
  @BeforeAll
  public static void setup() {
    GenericTestUtils.setLogLevel(ContainerBalancerTask.LOG, Level.DEBUG);    
  }

  private static Stream<Arguments> createMockedSCMWithDatanodeLimits() {
    return Stream.of(
        Arguments.of(MockedSCM.getMockedSCM(4), false),
        Arguments.of(MockedSCM.getMockedSCM(5), false),
        Arguments.of(MockedSCM.getMockedSCM(6), false),
        Arguments.of(MockedSCM.getMockedSCM(7), false),
        Arguments.of(MockedSCM.getMockedSCM(7), false),
        Arguments.of(MockedSCM.getMockedSCM(8), false),
        Arguments.of(MockedSCM.getMockedSCM(9), false),
        Arguments.of(MockedSCM.getMockedSCM(10), true),
        Arguments.of(MockedSCM.getMockedSCM(10), false)
    );
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testCalculationOfUtilization(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    MockedCluster cluster = mockedSCM.getCluster();
    DatanodeUsageInfo[] nodesInCluster = cluster.getNodesInCluster();
    double[] nodeUtilizationList = cluster.getNodeUtilizationList();
    Assertions.assertEquals(nodesInCluster.length, nodeUtilizationList.length);
    for (int i = 0; i < nodesInCluster.length; i++) {
      Assertions.assertEquals(nodeUtilizationList[i],
          nodesInCluster[i].calculateUtilization(), 0.0001);
    }
    mockedSCM.startBalancerTask(mockedSCM.getBalancerConfig());

    double averageUtilization = cluster.getAverageUtilization();

    // should be equal to average utilization of the cluster
    Assertions.assertEquals(averageUtilization,
        ContainerBalanceIteration.calculateAvgUtilization(
            Arrays.asList(nodesInCluster)),
        0.0001);
  }

  /**
   * Checks whether ContainerBalancer is correctly updating the list of
   * unBalanced nodes with varying values of Threshold.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testBalancerTaskAfterChangingThresholdValue(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // check for random threshold values
    ContainerBalancer balancer = mockedSCM.createContainerBalancer();
    MockedCluster cluster = mockedSCM.getCluster();
    for (int i = 0; i < 50; i++) {
      double randomThreshold = MockedCluster.RANDOM.nextDouble() * 100;

      List<DatanodeUsageInfo> expectedUnBalancedNodes =
          cluster.getUnBalancedNodes(randomThreshold);
      // sort unbalanced nodes as it is done inside ContainerBalancerTask:217
      //  in descending order by node utilization (most used node)
      expectedUnBalancedNodes.sort(
          (d1, d2) ->
              Double.compare(
                  d2.calculateUtilization(),
                  d1.calculateUtilization()
              )
      );

      ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
      config.setThreshold(randomThreshold);
      ContainerBalancerTask task =
          mockedSCM.startBalancerTask(balancer, config);

      List<DatanodeUsageInfo> actualUnBalancedNodes = getUnBalancedNodes(task);

      Assertions.assertEquals(
          expectedUnBalancedNodes.size(),
          actualUnBalancedNodes.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        Assertions.assertEquals(
            expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            actualUnBalancedNodes.get(j).getDatanodeDetails());
      }
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testBalancerWithMoveManager(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, TimeoutException, NodeNotFoundException {
    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(mockedSCM.getBalancerConfig());

    Mockito
        .verify(mockedSCM.getMoveManager(), atLeastOnce())
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));

    Mockito
        .verify(mockedSCM.getReplicationManager(), times(0))
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  /**
   * Checks whether the list of unBalanced nodes is empty when the cluster is
   * balanced.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(99.99);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertEquals(0, getUnBalancedNodes(task).size());
    Assertions.assertEquals(0, metrics.getNumDatanodesUnbalanced());
  }

  /**
   * ContainerBalancer should not involve more datanodes than the
   * maxDatanodesRatioToInvolvePerIteration limit.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void
      containerBalancerShouldObeyMaxDatanodesToInvolveLimit(
          @Nonnull MockedSCM mockedSCM,
          boolean useDatanodeLimits
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    int percent = 40;
    config.setMaxDatanodesPercentageToInvolvePerIteration(percent);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setThreshold(1);
    config.setIterations(1);
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    ContainerBalancerMetrics metrics = task.getMetrics();
    if (useDatanodeLimits) {
      int number = percent * mockedSCM.getCluster().getNodeCount() / 100;
      Assertions.assertFalse(
          task.getCountDatanodesInvolvedPerIteration() > number);
      Assertions.assertTrue(
          metrics.getNumDatanodesInvolvedInLatestIteration() > 0);
      Assertions.assertFalse(
          metrics.getNumDatanodesInvolvedInLatestIteration() > number);
    } else {
      Assertions.assertTrue(
          metrics.getNumDatanodesInvolvedInLatestIteration() > 0);
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerBalancerShouldSelectOnlyClosedContainers(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // make all containers open, balancer should not select any of them
    Collection<ContainerInfo> containers = mockedSCM.getCluster().
        getCidToInfoMap().values();
    for (ContainerInfo containerInfo : containers) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    // no container should have been selected
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());
    /*
    Iteration result should be CAN_NOT_BALANCE_ANY_MORE because no container
    move is generated
     */
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.CAN_NOT_BALANCE_ANY_MORE,
        task.getIterationResult()
    );

    // now, close all containers
    for (ContainerInfo containerInfo : containers) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    ContainerBalancerTask nextTask = mockedSCM.startBalancerTask(config);
    nextTask.stop();

    // check whether all selected containers are closed
    for (ContainerInfo cid : containers) {
      Assertions.assertSame(cid.getState(), HddsProtos.LifeCycleState.CLOSED);
    }
  }

  /**
   * Container Balancer should not select a non-CLOSED replica for moving.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldNotSelectNonClosedContainerReplicas(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException {
    // let's mock such that all replicas have CLOSING state
    ContainerManager containerManager = mockedSCM.getContainerManager();
    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
        mockedSCM.getCluster().getCidToReplicasMap();
    when(containerManager.getContainerReplicas(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          Set<ContainerReplica> replicas = cidToReplicasMap.get(cid);
          Set<ContainerReplica> replicasToReturn =
              new HashSet<>(replicas.size());
          for (ContainerReplica replica : replicas) {
            ContainerReplica newReplica = replica
                .toBuilder()
                .setContainerState(
                    StorageContainerDatanodeProtocolProtos.
                        ContainerReplicaProto.State.CLOSING
                ).build();
            replicasToReturn.add(newReplica);
          }
          return replicasToReturn;
        });

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    // no container should have moved because all replicas are CLOSING
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerBalancerShouldObeyMaxSizeToMoveLimit(
      @Nonnull MockedSCM mockedSCM, boolean useDatanodeLimits
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(1);
    config.setMaxSizeToMovePerIteration(
        10 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);
    config.setMaxDatanodesPercentageToInvolvePerIteration(20);
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should not have moved more size than the limit
    Assertions.assertFalse(
        task.getSizeScheduledForMoveInLatestIteration() >
            10 * MockedSCM.STORAGE_UNIT);

    long size = task.getMetrics().getDataSizeMovedGBInLatestIteration();
    Assertions.assertTrue(size > 0);
    Assertions.assertFalse(size > 10);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
        mockedSCM.getCluster().getCidToReplicasMap();
    Map<ContainerID, DatanodeDetails> map = task.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : map.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails target = entry.getValue();
      Assertions.assertTrue(
          cidToReplicasMap.get(container)
              .stream()
              .map(ContainerReplica::getDatanodeDetails)
              .noneMatch(target::equals)
      );
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerMoveSelectionShouldFollowPlacementPolicy(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setIterations(1);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    MockedCluster cluster = mockedSCM.getCluster();
    Map<ContainerID, ContainerInfo> cidToInfoMap = cluster.getCidToInfoMap();

    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap = cluster
        .getCidToReplicasMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        task.getContainerToTargetMap();

    PlacementPolicy policy = mockedSCM.getPlacementPolicy();
    PlacementPolicy ecPolicy = mockedSCM.getEcPlacementPolicy();

    // for each move selection, check if {replicas - source + target}
    // satisfies placement policy
    Set<Map.Entry<ContainerID, DatanodeDetails>> cnToDnDetailsMap =
        task.getContainerToSourceMap().entrySet();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : cnToDnDetailsMap) {
      ContainerID container = entry.getKey();
      DatanodeDetails source = entry.getValue();

      List<DatanodeDetails> replicas = cidToReplicasMap
          .get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      // remove source and add target
      replicas.remove(source);
      replicas.add(containerToTargetMap.get(container));

      ContainerInfo info = cidToInfoMap.get(container);
      int requiredNodes = info.getReplicationConfig().getRequiredNodes();
      ContainerPlacementStatus status =
          (info.getReplicationType() == HddsProtos.ReplicationType.RATIS)
              ? policy.validateContainerPlacement(replicas, requiredNodes)
              : ecPolicy.validateContainerPlacement(replicas, requiredNodes);

      Assertions.assertTrue(status.isPolicySatisfied());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void targetDatanodeShouldBeInServiceHealthy(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    MockNodeManager nodeManager = mockedSCM.getNodeManager();
    for (DatanodeDetails target : task.getSelectedTargets()) {
      NodeStatus status = nodeManager.getNodeStatus(target);
      Assertions.assertSame(HddsProtos.NodeOperationalState.IN_SERVICE,
          status.getOperationalState());
      Assertions.assertTrue(status.isHealthy());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void selectedContainerShouldNotAlreadyHaveBeenSelected(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, NodeNotFoundException, TimeoutException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    int numContainers = task.getContainerToTargetMap().size();

  /*
  Assuming move is called exactly once for each unique container, number of
   calls to move should equal number of unique containers. If number of
   calls to move is more than number of unique containers, at least one
   container has been re-selected. It's expected that number of calls to
   move should equal number of unique, selected containers (from
   containerToTargetMap).
   */
    Mockito.verify(mockedSCM.getReplicationManager(), times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class),
            any(DatanodeDetails.class));

    /*
     Try the same test by disabling LegacyReplicationManager so that
     MoveManager is used.
     */
    mockedSCM.disableLegacyReplicationManager();
    ContainerBalancerTask nextTask = mockedSCM.startBalancerTask(config);

    numContainers = nextTask.getContainerToTargetMap().size();
    Mockito.verify(mockedSCM.getMoveManager(), times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldNotSelectConfiguredExcludeContainers(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setExcludeContainers("1, 4, 5");

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Set<ContainerID> excludeContainers = config.getExcludeContainers();
    for (ContainerID container :
        task.getContainerToSourceMap().keySet()) {
      Assertions.assertFalse(excludeContainers.contains(container));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldObeyMaxSizeEnteringTargetLimit(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }

    // no containers should be selected when the limit is just 2 MB
    config.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());

    // some containers should be selected when using default values
    ContainerBalancerConfiguration cbc =
        mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    cbc.setBalancingInterval(1);
    if (!useDatanodeLimits) {
      cbc.setAdaptBalanceWhenCloseToLimit(false);
      cbc.setAdaptBalanceWhenReachTheLimit(false);
    }
    ContainerBalancerTask nextTask = mockedSCM.startBalancerTask(cbc);

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(nextTask).isEmpty());
    Assertions.assertFalse(nextTask.getContainerToSourceMap().isEmpty());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldObeyMaxSizeLeavingSourceLimit(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }

    // no source containers should be selected when the limit is just 2 MB
    config.setMaxSizeLeavingSource(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());

    // some containers should be selected when using default values
    ContainerBalancerConfiguration newContainerBalancerConfig =
        mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    newContainerBalancerConfig.setBalancingInterval(1);
    if (!useDatanodeLimits) {
      newContainerBalancerConfig.setAdaptBalanceWhenCloseToLimit(false);
      newContainerBalancerConfig.setAdaptBalanceWhenReachTheLimit(false);
    }
    task = mockedSCM.startBalancerTask(newContainerBalancerConfig);

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertFalse(task.getContainerToSourceMap().isEmpty());
    Assertions.assertTrue(0 !=
        task.getSizeScheduledForMoveInLatestIteration());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testMetrics(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, NodeNotFoundException {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setBalancingInterval(Duration.ofMillis(2));
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(6 * MockedSCM.STORAGE_UNIT);
    // deliberately set max size per iteration to a low value, 6 GB
    config.setMaxSizeToMovePerIteration(6 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    Mockito.when(mockedSCM.getMoveManager().move(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.COMPLETED));

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertEquals(
        mockedSCM.getCluster().getUnBalancedNodes(config.getThreshold()).size(),
        metrics.getNumDatanodesUnbalanced()
    );
    Assertions.assertTrue(metrics.getDataSizeMovedGBInLatestIteration() <= 6);
    Assertions.assertTrue(metrics.getDataSizeMovedGB() > 0);
    Assertions.assertEquals(1, metrics.getNumIterations());
    Assertions.assertTrue(
        metrics.getNumContainerMovesScheduledInLatestIteration() > 0);
    Assertions.assertEquals(metrics.getNumContainerMovesScheduled(),
        metrics.getNumContainerMovesScheduledInLatestIteration());
    Assertions.assertEquals(metrics.getNumContainerMovesScheduled(),
        metrics.getNumContainerMovesCompleted() +
            metrics.getNumContainerMovesFailed() +
            metrics.getNumContainerMovesTimeout());
    Assertions.assertEquals(0, metrics.getNumContainerMovesTimeout());
    Assertions.assertEquals(1, metrics.getNumContainerMovesFailed());
  }

  /**
   * Tests if {@link ContainerBalancer} follows the includeNodes and
   * excludeNodes configurations in {@link ContainerBalancerConfiguration}.
   * If the includeNodes configuration is not empty, only the specified
   * includeNodes should be included in balancing. excludeNodes should be
   * excluded from balancing. If a datanode is specified in both include and
   * exclude configurations, then it should be excluded.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldFollowExcludeAndIncludeDatanodesConfigurations(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    DatanodeUsageInfo[] nodesInCluster =
        mockedSCM.getCluster().getNodesInCluster();

    // only these nodes should be included
    // the ones also specified in excludeNodes should be excluded
    int firstIncludeIndex = 0, secondIncludeIndex = 1;
    int thirdIncludeIndex = nodesInCluster.length - 2;
    int fourthIncludeIndex = nodesInCluster.length - 1;
    String includeNodes = String.join(", ",
        nodesInCluster[firstIncludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[secondIncludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[thirdIncludeIndex].getDatanodeDetails().getHostName(),
        nodesInCluster[fourthIncludeIndex].getDatanodeDetails().getHostName()
    );

    // these nodes should be excluded
    int firstExcludeIndex = 0, secondExcludeIndex = nodesInCluster.length - 1;
    String excludeNodes = String.join(", ",
        nodesInCluster[firstExcludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[secondExcludeIndex].getDatanodeDetails().getHostName()
    );

    config.setExcludeNodes(excludeNodes);
    config.setIncludeNodes(includeNodes);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // finally, these should be the only nodes included in balancing
    // (included - excluded)
    DatanodeDetails dn1 =
        nodesInCluster[secondIncludeIndex].getDatanodeDetails();
    DatanodeDetails dn2 =
        nodesInCluster[thirdIncludeIndex].getDatanodeDetails();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        task.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        task.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      DatanodeDetails source = entry.getValue();
      DatanodeDetails target = containerToTargetMap.get(entry.getKey());
      Assertions.assertTrue(source.equals(dn1) || source.equals(dn2));
      Assertions.assertTrue(target.equals(dn1) || target.equals(dn2));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testContainerBalancerConfiguration(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.scm.container.size", "5GB");
    ozoneConfiguration.setDouble(
        "hdds.container.balancer.utilization.threshold", 1);
    long maxSizeLeavingSource = 26;
    ozoneConfiguration.setStorageSize(
        "hdds.container.balancer.size.leaving.source.max", maxSizeLeavingSource,
        StorageUnit.GB);
    long moveTimeout = 90;
    ozoneConfiguration.setTimeDuration("hdds.container.balancer.move.timeout",
        moveTimeout, TimeUnit.MINUTES);
    long replicationTimeout = 60;
    ozoneConfiguration.setTimeDuration(
        "hdds.container.balancer.move.replication.timeout",
        replicationTimeout, TimeUnit.MINUTES);

    ContainerBalancerConfiguration cbConf =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfiguration);
    Assertions.assertEquals(1, cbConf.getThreshold(), 0.001);

    // Expected is 26 GB
    Assertions.assertEquals(maxSizeLeavingSource * 1024 * 1024 * 1024,
        cbConf.getMaxSizeLeavingSource());
    Assertions.assertEquals(moveTimeout, cbConf.getMoveTimeout().toMinutes());
    Assertions.assertEquals(replicationTimeout,
        cbConf.getMoveReplicationTimeout().toMinutes());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResult(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException, IOException, TimeoutException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    mockedSCM.enableLegacyReplicationManager();

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult()
    );

    /*
    Now, limit maxSizeToMovePerIteration but fail all container moves. The
    result should still be ITERATION_COMPLETED.
     */
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY)
        );
    config.setMaxSizeToMovePerIteration(10 * MockedSCM.STORAGE_UNIT);

    mockedSCM.startBalancerTask(config);

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult()
    );

    /*
    Try the same but use MoveManager for container move instead of legacy RM.
     */
    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(config);
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult()
    );
  }

  /**
   * Tests the situation where some container moves time out because they
   * take longer than "move.timeout".
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeout(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  )
      throws NodeNotFoundException, IOException, TimeoutException {

    CompletableFuture<MoveManager.MoveResult> completedFuture =
        CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)
            ))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000));

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult()
    );

    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertEquals(
        1,
        metrics.getNumContainerMovesCompletedInLatestIteration()
    );
    Assertions.assertTrue(
        metrics.getNumContainerMovesTimeoutInLatestIteration() >= 1
    );

    /*
    Test the same but use MoveManager instead of LegacyReplicationManager.
    The first move being 10ms falls within the timeout duration of 500ms. It
    should be successful. The rest should fail.
     */
    mockedSCM.disableLegacyReplicationManager();
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000));

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult()
    );
    Assertions.assertEquals(
        1,
        metrics.getNumContainerMovesCompletedInLatestIteration()
    );
    Assertions.assertTrue(
        metrics.getNumContainerMovesTimeoutInLatestIteration() >= 1
    );
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeoutFromReplicationManager(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future = CompletableFuture
        .supplyAsync(() -> MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT);
    CompletableFuture<MoveManager.MoveResult> future2 = CompletableFuture
        .supplyAsync(() -> MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);

    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(future, future2);

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertTrue(
        metrics.getNumContainerMovesTimeoutInLatestIteration() > 0
    );
    Assertions.assertEquals(
        0, metrics.getNumContainerMovesCompletedInLatestIteration()
    );


    /*
    Try the same test with MoveManager instead of LegacyReplicationManager.
     */
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(future)
        .thenAnswer(invocation -> future2);

    mockedSCM.disableLegacyReplicationManager();

    Assertions.assertTrue(
        metrics.getNumContainerMovesTimeoutInLatestIteration() > 0
    );
    Assertions.assertEquals(
        0, metrics.getNumContainerMovesCompletedInLatestIteration()
    );
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultException(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future =
        new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Runtime Exception"));
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.supplyAsync(
            () -> {
              try {
                Thread.sleep(1);
              } catch (Exception ignored) {
              }
              throw new RuntimeException("Runtime Exception after doing work");
            }))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    if (!useDatanodeLimits) {
      config.setAdaptBalanceWhenCloseToLimit(false);
      config.setAdaptBalanceWhenReachTheLimit(false);
    }

    mockedSCM.enableLegacyReplicationManager();

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() >= 1);

    /*
    Try the same test but with MoveManager instead of ReplicationManager.
     */
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.supplyAsync(
            () -> {
              try {
                Thread.sleep(1);
              } catch (Exception ignored) {
              }
              throw new RuntimeException("Runtime Exception after doing work");
            }))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    mockedSCM.disableLegacyReplicationManager();

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() >= 1);
  }

  @Unhealthy("HDDS-8941")
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testDelayedStart(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws InterruptedException, TimeoutException {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.setTimeDuration(
        "hdds.scm.wait.time.after.safemode.exit", 10, TimeUnit.SECONDS
    );
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);

    StorageContainerManager scm = mockedSCM.getStorageContainerManager();
    ContainerBalancer balancer = new ContainerBalancer(scm);
    ContainerBalancerTask task =
        new ContainerBalancerTask(scm, 2, balancer, config, true);
    Thread balancingThread = new Thread(task);
    // start the thread and assert that balancer is RUNNING
    balancingThread.start();
    Assertions.assertTrue(task.isRunning());

    /*
     Wait for the thread to start sleeping and assert that it's sleeping.
     This is the delay before it starts balancing.
     */
    GenericTestUtils.waitFor(
        () -> balancingThread.getState() == Thread.State.TIMED_WAITING, 1, 20);
    Assertions.assertEquals(
        Thread.State.TIMED_WAITING, balancingThread.getState()
    );

    // interrupt the thread from its sleep, wait and assert that balancer has
    // STOPPED
    balancingThread.interrupt();
    GenericTestUtils.waitFor(task::isStopped, 1, 20);
    Assertions.assertTrue(task.isStopped());

    // ensure the thread dies
    GenericTestUtils.waitFor(() -> !balancingThread.isAlive(), 1, 20);
    Assertions.assertFalse(balancingThread.isAlive());
  }

  /**
   * The expectation is that only RATIS containers should be selected for
   * balancing when LegacyReplicationManager is enabled. This is because
   * LegacyReplicationManager does not support moving EC containers.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldExcludeECContainersWhenLegacyRmIsEnabled(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // Enable LegacyReplicationManager
    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    /*
     Get all containers that were selected by balancer and assert none of
     them is an EC container.
     */
    Map<ContainerID, DatanodeDetails> containerToSource =
        task.getContainerToSourceMap();
    Assertions.assertFalse(containerToSource.isEmpty());
    Map<ContainerID, ContainerInfo> cidToInfoMap =
        mockedSCM.getCluster().getCidToInfoMap();
    for (ContainerID containerID : containerToSource.keySet()) {
      ContainerInfo containerInfo = cidToInfoMap.get(containerID);
      Assertions.assertNotSame(HddsProtos.ReplicationType.EC,
          containerInfo.getReplicationType());
    }
  }

  public static
      @Nonnull CompletableFuture<MoveManager.MoveResult> genCompletableFuture(
          int sleepMilSec
  ) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(sleepMilSec);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return MoveManager.MoveResult.COMPLETED;
    });
  }

  public static @Nonnull List<DatanodeUsageInfo> getUnBalancedNodes(
      @Nonnull ContainerBalancerTask task
  ) {
    ArrayList<DatanodeUsageInfo> result = new ArrayList<>();
    result.addAll(task.getOverUtilizedNodes());
    result.addAll(task.getUnderUtilizedNodes());
    return result;
  }
}
