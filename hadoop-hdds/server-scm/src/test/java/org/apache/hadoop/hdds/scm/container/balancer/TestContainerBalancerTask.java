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

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.balancer.iteration.ContainerBalanceIteration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancerTask {
  private ReplicationManager replicationManager;
  private MoveManager moveManager;
  private ContainerManager containerManager;
  private MockNodeManager mockNodeManager;
  private StorageContainerManager scm;
  private OzoneConfiguration conf;
  private ReplicationManagerConfiguration rmConf;
  private PlacementPolicy placementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private ContainerBalancerConfiguration balancerConfiguration;
  private final TestableCluster cluster = new TestableCluster(10);
  private final Map<String, ByteString> serviceToConfigMap = new HashMap<>();

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup()
      throws IOException, NodeNotFoundException, TimeoutException {
    conf = new OzoneConfiguration();
    rmConf = new ReplicationManagerConfiguration();
    scm = Mockito.mock(StorageContainerManager.class);
    containerManager = Mockito.mock(ContainerManager.class);
    replicationManager = Mockito.mock(ReplicationManager.class);
    StatefulServiceStateManager serviceStateManager =
        Mockito.mock(StatefulServiceStateManagerImpl.class);
    SCMServiceManager scmServiceManager = Mockito.mock(SCMServiceManager.class);
    moveManager = Mockito.mock(MoveManager.class);
    Mockito.when(moveManager.move(any(ContainerID.class),
            any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.COMPLETED));

    /*
    Disable LegacyReplicationManager. This means balancer should select RATIS
     as well as EC containers for balancing. Also, MoveManager will be used.
     */
    Mockito.when(replicationManager.getConfig()).thenReturn(rmConf);
    rmConf.setEnableLegacy(false);
    // these configs will usually be specified in each test
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(
        50 * TestableCluster.STORAGE_UNIT);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancerTask.LOG, Level.DEBUG);

    mockNodeManager = new MockNodeManager(cluster.getDatanodeToContainersMap());

    NetworkTopology clusterMap = mockNodeManager.getClusterNetworkTopologyMap();

    placementPolicy = ContainerPlacementPolicyFactory
        .getPolicy(conf, mockNodeManager, clusterMap, true,
            SCMContainerPlacementMetrics.create());
    ecPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(
        conf, mockNodeManager, clusterMap,
        true, SCMContainerPlacementMetrics.create());
    PlacementPolicyValidateProxy placementPolicyValidateProxy =
        new PlacementPolicyValidateProxy(
            placementPolicy, ecPlacementPolicy);

    Mockito.when(replicationManager
            .isContainerReplicatingOrDeleting(Mockito.any(ContainerID.class)))
        .thenReturn(false);

    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.
            completedFuture(MoveManager.MoveResult.COMPLETED));

    Mockito.when(replicationManager.getClock())
        .thenReturn(Clock.system(ZoneId.systemDefault()));

    when(containerManager.getContainerReplicas(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cluster.getCidToReplicasMap().get(cid);
        });

    when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cluster.getCidToInfoMap().get(cid);
        });

    when(containerManager.getContainers())
        .thenReturn(new ArrayList<>(cluster.getCidToInfoMap().values()));

    when(scm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(scm.getContainerPlacementPolicy()).thenReturn(placementPolicy);
    when(scm.getContainerManager()).thenReturn(containerManager);
    when(scm.getReplicationManager()).thenReturn(replicationManager);
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
    when(scm.getClusterMap()).thenReturn(null);
    when(scm.getEventQueue()).thenReturn(mock(EventPublisher.class));
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getStatefulServiceStateManager()).thenReturn(serviceStateManager);
    when(scm.getSCMServiceManager()).thenReturn(scmServiceManager);
    when(scm.getPlacementPolicyValidateProxy())
        .thenReturn(placementPolicyValidateProxy);
    when(scm.getMoveManager()).thenReturn(moveManager);

    /*
    When StatefulServiceStateManager#saveConfiguration is called, save to
    in-memory serviceToConfigMap instead.
     */
    Mockito.doAnswer(i -> {
      serviceToConfigMap.put(i.getArgument(0, String.class), i.getArgument(1,
          ByteString.class));
      return null;
    }).when(serviceStateManager).saveConfiguration(
        Mockito.any(String.class),
        Mockito.any(ByteString.class));

    /*
    When StatefulServiceStateManager#readConfiguration is called, read from
    serviceToConfigMap instead.
     */
    when(serviceStateManager.readConfiguration(Mockito.anyString())).thenAnswer(
        i -> serviceToConfigMap.get(i.getArgument(0, String.class)));

    Mockito.doNothing().when(scmServiceManager)
        .register(Mockito.any(SCMService.class));
  }

  @Test
  public void testCalculationOfUtilization() {
    DatanodeUsageInfo[] nodesInCluster = cluster.getNodesInCluster();
    double[] nodeUtilizationList = cluster.getNodeUtilizationList();
    Assertions.assertEquals(nodesInCluster.length, nodeUtilizationList.length);
    for (int i = 0; i < nodesInCluster.length; i++) {
      Assertions.assertEquals(nodeUtilizationList[i],
          nodesInCluster[i].calculateUtilization(), 0.0001);
    }
    startBalancerTask(balancerConfiguration);

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
  @Test
  public void testBalancerTaskAfterChangingThresholdValue() {
    // check for random threshold values
    ContainerBalancer sb = new ContainerBalancer(scm);
    for (int i = 0; i < 50; i++) {
      double randomThreshold = TestableCluster.RANDOM.nextDouble() * 100;

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

      balancerConfiguration.setThreshold(randomThreshold);
      ContainerBalancerTask task = new ContainerBalancerTask(scm,
          0, sb, balancerConfiguration, false);
      task.run();

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

  @Test
  public void testBalancerWithMoveManager()
      throws IOException, TimeoutException, NodeNotFoundException {
    rmConf.setEnableLegacy(false);
    startBalancerTask(balancerConfiguration);
    Mockito.verify(moveManager, atLeastOnce())
        .move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class));

    Mockito.verify(replicationManager, times(0))
        .move(Mockito.any(ContainerID.class), Mockito.any(
            DatanodeDetails.class), Mockito.any(DatanodeDetails.class));
  }

  /**
   * Checks whether the list of unBalanced nodes is empty when the cluster is
   * balanced.
   */
  @Test
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced() {
    balancerConfiguration.setThreshold(99.99);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);


    stopBalancer();
    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertEquals(0, getUnBalancedNodes(task).size());
    Assertions.assertEquals(0, metrics.getNumDatanodesUnbalanced());
  }

  /**
   * ContainerBalancer should not involve more datanodes than the
   * maxDatanodesRatioToInvolvePerIteration limit.
   */
  @Test
  public void containerBalancerShouldObeyMaxDatanodesToInvolveLimit() {
    int percent = 40;
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(
        percent);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setIterations(1);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    int number = percent * cluster.getNodeCount() / 100;
    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertFalse(
        task.getCountDatanodesInvolvedPerIteration() > number);
    Assertions.assertTrue(
        metrics.getNumDatanodesInvolvedInLatestIteration() > 0);
    Assertions.assertFalse(
        metrics.getNumDatanodesInvolvedInLatestIteration() > number);
    stopBalancer();
  }

  @Test
  public void containerBalancerShouldSelectOnlyClosedContainers() {
    // make all containers open, balancer should not select any of them
    Map<ContainerID, ContainerInfo> cidToInfoMap = cluster.getCidToInfoMap();
    Collection<ContainerInfo> containerInfos = cidToInfoMap.values();
    for (ContainerInfo containerInfo : containerInfos) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }
    balancerConfiguration.setThreshold(10);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    stopBalancer();

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
        task.getIterationResult());

    // now, close all containers
    for (ContainerInfo containerInfo : containerInfos) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    startBalancerTask(balancerConfiguration);
    stopBalancer();

    // check whether all selected containers are closed
    for (ContainerID cid :
        task.getContainerToSourceMap().keySet()) {
      Assertions.assertSame(
          cidToInfoMap.get(cid).getState(), HddsProtos.LifeCycleState.CLOSED);
    }
  }

  /**
   * Container Balancer should not select a non-CLOSED replica for moving.
   */
  @Test
  public void balancerShouldNotSelectNonClosedContainerReplicas()
      throws IOException {
    // let's mock such that all replicas have CLOSING state
    when(containerManager.getContainerReplicas(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          Set<ContainerReplica> replicas =
              cluster.getCidToReplicasMap().get(cid);
          Set<ContainerReplica> replicasToReturn =
              new HashSet<>(replicas.size());
          for (ContainerReplica replica : replicas) {
            ContainerReplica newReplica =
                replica.toBuilder().setContainerState(
                    ContainerReplicaProto.State.CLOSING).build();
            replicasToReturn.add(newReplica);
          }

          return replicasToReturn;
        });

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(
        50 * TestableCluster.STORAGE_UNIT);

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    stopBalancer();

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    // no container should have moved because all replicas are CLOSING
    Assertions.assertTrue(
        task.getContainerToSourceMap().isEmpty());
  }

  @Test
  public void containerBalancerShouldObeyMaxSizeToMoveLimit() {
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(20);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    // balancer should not have moved more size than the limit
    Assertions.assertFalse(
        task.getSizeScheduledForMoveInLatestIteration() >
            10 * TestableCluster.STORAGE_UNIT);

    long size = task.getMetrics()
        .getDataSizeMovedGBInLatestIteration();
    Assertions.assertTrue(size > 0);
    Assertions.assertFalse(size > 10);
    stopBalancer();
  }

  @Test
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    stopBalancer();
    Map<ContainerID, DatanodeDetails> map =
        task.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : map.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails target = entry.getValue();
      Assertions.assertTrue(cluster.getCidToReplicasMap().get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .noneMatch(target::equals));
    }
  }

  @Test
  public void containerMoveSelectionShouldFollowPlacementPolicy() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setIterations(1);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    stopBalancer();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        task.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        task.getContainerToTargetMap();

    // for each move selection, check if {replicas - source + target}
    // satisfies placement policy
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails source = entry.getValue();

      List<DatanodeDetails> replicas =
          cluster.getCidToReplicasMap().get(container)
              .stream()
              .map(ContainerReplica::getDatanodeDetails)
              .collect(Collectors.toList());
      // remove source and add target
      replicas.remove(source);
      replicas.add(containerToTargetMap.get(container));

      ContainerInfo containerInfo = cluster.getCidToInfoMap().get(container);
      ContainerPlacementStatus placementStatus;
      if (containerInfo.getReplicationType() ==
          HddsProtos.ReplicationType.RATIS) {
        placementStatus = placementPolicy.validateContainerPlacement(replicas,
            containerInfo.getReplicationConfig().getRequiredNodes());
      } else {
        placementStatus =
            ecPlacementPolicy.validateContainerPlacement(replicas,
                containerInfo.getReplicationConfig().getRequiredNodes());
      }
      Assertions.assertTrue(placementStatus.isPolicySatisfied());
    }
  }

  @Test
  public void targetDatanodeShouldBeInServiceHealthy()
      throws NodeNotFoundException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setIterations(1);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    stopBalancer();
    for (DatanodeDetails target : task.getSelectedTargets()) {
      NodeStatus status = mockNodeManager.getNodeStatus(target);
      Assertions.assertSame(HddsProtos.NodeOperationalState.IN_SERVICE,
          status.getOperationalState());
      Assertions.assertTrue(status.isHealthy());
    }
  }

  @Test
  public void selectedContainerShouldNotAlreadyHaveBeenSelected()
      throws IOException, NodeNotFoundException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setIterations(1);

    rmConf.setEnableLegacy(true);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    stopBalancer();
    int numContainers = task.getContainerToTargetMap().size();

  /*
  Assuming move is called exactly once for each unique container, number of
   calls to move should equal number of unique containers. If number of
   calls to move is more than number of unique containers, at least one
   container has been re-selected. It's expected that number of calls to
   move should equal number of unique, selected containers (from
   containerToTargetMap).
   */
    Mockito.verify(replicationManager, times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class),
            any(DatanodeDetails.class));

    /*
     Try the same test by disabling LegacyReplicationManager so that
     MoveManager is used.
     */
    rmConf.setEnableLegacy(false);
    ContainerBalancerTask nextTask = startBalancerTask(balancerConfiguration);
    stopBalancer();
    numContainers = nextTask.getContainerToTargetMap().size();
    Mockito.verify(moveManager, times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  @Test
  public void balancerShouldNotSelectConfiguredExcludeContainers() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(
        50 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setExcludeContainers("1, 4, 5");

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    stopBalancer();
    Set<ContainerID> excludeContainers =
        balancerConfiguration.getExcludeContainers();
    for (ContainerID container :
        task.getContainerToSourceMap().keySet()) {
      Assertions.assertFalse(excludeContainers.contains(container));
    }
  }

  @Test
  public void balancerShouldObeyMaxSizeEnteringTargetLimit() {
    conf.set("ozone.scm.container.size", "1MB");
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);

    // no containers should be selected when the limit is just 2 MB
    balancerConfiguration.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());
    stopBalancer();

    // some containers should be selected when using default values
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ContainerBalancerConfiguration cbc = ozoneConfiguration.
        getObject(ContainerBalancerConfiguration.class);
    cbc.setBalancingInterval(1);

    ContainerBalancerTask nextTask = startBalancerTask(cbc);

    stopBalancer();
    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(nextTask).isEmpty());
    Assertions.assertFalse(nextTask.getContainerToSourceMap().isEmpty());
  }

  @Test
  public void balancerShouldObeyMaxSizeLeavingSourceLimit() {
    conf.set("ozone.scm.container.size", "1MB");
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        50 * TestableCluster.STORAGE_UNIT);

    // no source containers should be selected when the limit is just 2 MB
    balancerConfiguration.setMaxSizeLeavingSource(2 * OzoneConsts.MB);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertTrue(task.getContainerToSourceMap().isEmpty());
    stopBalancer();

    // some containers should be selected when using default values
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ContainerBalancerConfiguration cbc = ozoneConfiguration.
        getObject(ContainerBalancerConfiguration.class);
    cbc.setBalancingInterval(1);
    ContainerBalancer sb = new ContainerBalancer(scm);
    task = new ContainerBalancerTask(scm, 0, sb, cbc, false);
    task.run();

    stopBalancer();
    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(task).isEmpty());
    Assertions.assertFalse(task.getContainerToSourceMap().isEmpty());
    Assertions.assertTrue(0 !=
        task.getSizeScheduledForMoveInLatestIteration());
  }

  @Test
  public void testMetrics() throws IOException, NodeNotFoundException {
    conf.set("hdds.datanode.du.refresh.period", "1ms");
    balancerConfiguration.setBalancingInterval(Duration.ofMillis(2));
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        6 * TestableCluster.STORAGE_UNIT);
    // deliberately set max size per iteration to a low value, 6 GB
    balancerConfiguration.setMaxSizeToMovePerIteration(
        6 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    Mockito.when(moveManager.move(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.COMPLETED));

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    stopBalancer();

    ContainerBalancerMetrics metrics = task.getMetrics();
    Assertions.assertEquals(cluster.getUnBalancedNodes(
            balancerConfiguration.getThreshold()).size(),
        metrics.getNumDatanodesUnbalanced());
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
  @Test
  public void balancerShouldFollowExcludeAndIncludeDatanodesConfigurations() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    DatanodeUsageInfo[] nodesInCluster = cluster.getNodesInCluster();

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

    balancerConfiguration.setExcludeNodes(excludeNodes);
    balancerConfiguration.setIncludeNodes(includeNodes);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    stopBalancer();

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

  @Test
  public void testContainerBalancerConfiguration() {
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
        ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
    Assertions.assertEquals(1, cbConf.getThreshold(), 0.001);

    // Expected is 26 GB
    Assertions.assertEquals(maxSizeLeavingSource * 1024 * 1024 * 1024,
        cbConf.getMaxSizeLeavingSource());
    Assertions.assertEquals(moveTimeout, cbConf.getMoveTimeout().toMinutes());
    Assertions.assertEquals(replicationTimeout,
        cbConf.getMoveReplicationTimeout().toMinutes());
  }

  @Test
  public void checkIterationResult()
      throws NodeNotFoundException, IOException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    rmConf.setEnableLegacy(true);

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    stopBalancer();

    /*
    Now, limit maxSizeToMovePerIteration but fail all container moves. The
    result should still be ITERATION_COMPLETED.
     */
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY));
    balancerConfiguration.setMaxSizeToMovePerIteration(
        10 * TestableCluster.STORAGE_UNIT);

    startBalancerTask(balancerConfiguration);

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    stopBalancer();

    /*
    Try the same but use MoveManager for container move instead of legacy RM.
     */
    rmConf.setEnableLegacy(false);
    startBalancerTask(balancerConfiguration);
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    stopBalancer();
  }

  /**
   * Tests the situation where some container moves time out because they
   * take longer than "move.timeout".
   */
  @Test
  public void checkIterationResultTimeout()
      throws NodeNotFoundException, IOException, TimeoutException {

    CompletableFuture<MoveManager.MoveResult> completedFuture =
        CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(500));
    rmConf.setEnableLegacy(true);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertEquals(1,
        task.getMetrics()
            .getNumContainerMovesCompletedInLatestIteration());
    Assertions.assertTrue(task.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() > 1);
    stopBalancer();

    /*
    Test the same but use MoveManager instead of LegacyReplicationManager.
    The first move being 10ms falls within the timeout duration of 500ms. It
    should be successful. The rest should fail.
     */
    rmConf.setEnableLegacy(false);
    Mockito.when(moveManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000));

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertEquals(1,
        task.getMetrics()
            .getNumContainerMovesCompletedInLatestIteration());
    Assertions.assertTrue(task.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() > 1);
    stopBalancer();
  }

  @Test
  public void checkIterationResultTimeoutFromReplicationManager()
      throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future
        = CompletableFuture.supplyAsync(() ->
        MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT);
    CompletableFuture<MoveManager.MoveResult> future2
        = CompletableFuture.supplyAsync(() ->
        MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(future, future2);

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(500));
    rmConf.setEnableLegacy(true);
    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    Assertions.assertTrue(task.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() > 0);
    Assertions.assertEquals(0, task.getMetrics()
        .getNumContainerMovesCompletedInLatestIteration());
    stopBalancer();

    /*
    Try the same test with MoveManager instead of LegacyReplicationManager.
     */
    Mockito.when(moveManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(future).thenAnswer(invocation -> future2);

    rmConf.setEnableLegacy(false);

    Assertions.assertTrue(task.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() > 0);
    Assertions.assertEquals(0, task.getMetrics()
        .getNumContainerMovesCompletedInLatestIteration());
    stopBalancer();
  }

  @Test
  public void checkIterationResultException()
      throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future =
        new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Runtime Exception"));
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.supplyAsync(() -> {
          try {
            Thread.sleep(1);
          } catch (Exception ignored) {
          }
          throw new RuntimeException("Runtime Exception after doing work");
        }))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(500));
    rmConf.setEnableLegacy(true);

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);
    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() >= 3);
    stopBalancer();

    /*
    Try the same test but with MoveManager instead of ReplicationManager.
     */
    Mockito.when(moveManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.supplyAsync(() -> {
          try {
            Thread.sleep(1);
          } catch (Exception ignored) {
          }
          throw new RuntimeException("Runtime Exception after doing work");
        }))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    rmConf.setEnableLegacy(false);

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() >= 3);
    stopBalancer();
  }

  @Unhealthy("HDDS-8941")
  @Test
  public void testDelayedStart() throws InterruptedException, TimeoutException {
    conf.setTimeDuration("hdds.scm.wait.time.after.safemode.exit", 10,
        TimeUnit.SECONDS);
    ContainerBalancer balancer = new ContainerBalancer(scm);
    ContainerBalancerTask task = new ContainerBalancerTask(scm, 2, balancer,
        balancerConfiguration, true);
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
    Assertions.assertEquals(Thread.State.TIMED_WAITING,
        balancingThread.getState());

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
  @Test
  public void balancerShouldExcludeECContainersWhenLegacyRmIsEnabled() {
    // Enable LegacyReplicationManager
    rmConf.setEnableLegacy(true);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(
        10 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(
        100 * TestableCluster.STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    ContainerBalancerTask task = startBalancerTask(balancerConfiguration);

    /*
     Get all containers that were selected by balancer and assert none of
     them is an EC container.
     */
    Map<ContainerID, DatanodeDetails> containerToSource =
        task.getContainerToSourceMap();
    Assertions.assertFalse(containerToSource.isEmpty());
    Map<ContainerID, ContainerInfo> cidToInfoMap = cluster.getCidToInfoMap();
    for (ContainerID containerID : containerToSource.keySet()) {
      ContainerInfo containerInfo = cidToInfoMap.get(containerID);
      Assertions.assertNotSame(HddsProtos.ReplicationType.EC,
          containerInfo.getReplicationType());
    }
  }


  private @Nonnull ContainerBalancerTask startBalancerTask(
      @Nonnull ContainerBalancerConfiguration config
  ) {
    ContainerBalancer containerBalancer = new ContainerBalancer(scm);
    ContainerBalancerTask task =
        new ContainerBalancerTask(scm, 0, containerBalancer, config, false);
    task.run();
    return task;
  }

  private void stopBalancer() {
    // do nothing as testcase is not threaded
  }

  public static
      CompletableFuture<MoveManager.MoveResult> genCompletableFuture(
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
