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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.container.balancer.TestContainerBalancerTask.getUnBalancedNodes;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Class for cluster balancing test when the number of nodes is changing.
 */
public class TestClusterBalancing {
  private ReplicationManager replicationManager;
  private MoveManager moveManager;
  private StorageContainerManager scm;
  private OzoneConfiguration conf;
  private ReplicationManager.ReplicationManagerConfiguration rmConf;
  private ContainerBalancerConfiguration balancerConfiguration;

  private final Map<String, ByteString> serviceToConfigMap = new HashMap<>();
  // todo: run tests on clusters with a different node count
  private final TestableCluster cluster = new TestableCluster(4);

  @BeforeEach
  public void setup()
      throws IOException, NodeNotFoundException, TimeoutException {
    conf = new OzoneConfiguration();
    rmConf = new ReplicationManager.ReplicationManagerConfiguration();
    scm = Mockito.mock(StorageContainerManager.class);
    replicationManager = Mockito.mock(ReplicationManager.class);
    StatefulServiceStateManager serviceStateManager =
        Mockito.mock(StatefulServiceStateManagerImpl.class);
    SCMServiceManager scmServiceManager = Mockito.mock(SCMServiceManager.class);
    moveManager = Mockito.mock(MoveManager.class);
    Mockito
        .when(
            moveManager.move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)
            )
        )
        .thenReturn(
            CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED)
        );

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

    // todo: ATTENTION!!! TESTING
    balancerConfiguration.setAdaptBalanceWhenCloseToLimit(false);
    balancerConfiguration.setAdaptBalanceWhenReachTheLimit(false);


    GenericTestUtils.setLogLevel(ContainerBalancerTask.LOG, Level.DEBUG);

    MockNodeManager mockNodeManager =
        new MockNodeManager(cluster.getDatanodeToContainersMap());

    NetworkTopology clusterMap = mockNodeManager.getClusterNetworkTopologyMap();

    PlacementPolicy placementPolicy = ContainerPlacementPolicyFactory
        .getPolicy(conf, mockNodeManager, clusterMap, true,
            SCMContainerPlacementMetrics.create());
    PlacementPolicy ecPlacementPolicy =
        ContainerPlacementPolicyFactory.getECPolicy(
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

    ContainerManager containerManager = mockContainerManager();

    when(scm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(scm.getContainerPlacementPolicy()).thenReturn(placementPolicy);
    when(scm.getContainerManager()).thenReturn(containerManager);
    when(scm.getReplicationManager()).thenReturn(replicationManager);
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
//    when(scm.getClusterMap()).thenReturn(null);
//    when(scm.getEventQueue()).thenReturn(mock(EventPublisher.class));
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

  private @Nonnull ContainerManager mockContainerManager()
      throws ContainerNotFoundException {
    ContainerManager containerManager = Mockito.mock(ContainerManager.class);
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
    return containerManager;
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
        .thenAnswer(invocation ->
            TestContainerBalancerTask.genCompletableFuture(2000));

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
        .getNumContainerMovesTimeoutInLatestIteration() >= 1);
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
        .thenAnswer(invocation ->
            TestContainerBalancerTask.genCompletableFuture(2000));

    Assertions.assertEquals(
        ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    Assertions.assertEquals(1,
        task.getMetrics()
            .getNumContainerMovesCompletedInLatestIteration());
    Assertions.assertTrue(task.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() >= 1);
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
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() > 0);
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
    Assertions.assertTrue(task.getMetrics().getNumContainerMovesFailed() >= 0);
    stopBalancer();
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

//    // todo: ATTENTION!!! TESTING
    balancerConfiguration.setAdaptBalanceWhenCloseToLimit(false);
    balancerConfiguration.setAdaptBalanceWhenReachTheLimit(false);

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
    cbc.setAdaptBalanceWhenCloseToLimit(false);
    cbc.setAdaptBalanceWhenReachTheLimit(false);
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

    Assertions.assertTrue(
        task.getCountDatanodesInvolvedPerIteration() > 0
    );
    stopBalancer();
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
    cbc.setAdaptBalanceWhenCloseToLimit(false);
    cbc.setAdaptBalanceWhenReachTheLimit(false);

    ContainerBalancerTask nextTask = startBalancerTask(cbc);

    stopBalancer();
    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(getUnBalancedNodes(nextTask).isEmpty());
    Assertions.assertFalse(nextTask.getContainerToSourceMap().isEmpty());
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
}
