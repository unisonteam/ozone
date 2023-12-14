/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class for test used for setting up testable StorageContainerManager.
 * Provides an access to {@link TestableCluster} and to necessary
 * mocked instances
 */
public final class MockedSCM {
  public static final long STORAGE_UNIT = OzoneConsts.GB;
  private final StorageContainerManager scm;
  private final TestableCluster cluster;
  private final ContainerBalancerConfiguration balancerCfg;
  private final OzoneConfiguration ozoneCfg;
  private final MockNodeManager mockNodeManager;
  private ContainerBalancer containerBalancer;
  private MockedReplicationManager mockedReplicaManager;
  private MoveManager moveManager;
  private ContainerManager containerManager;

  private MockedPlacementPolicies mockedPlacementPolicies;

  private MockedSCM(@Nonnull TestableCluster testableCluster,
                    @Nonnull OzoneConfiguration ozoneConfig,
                    @Nonnull ContainerBalancerConfiguration balancerConfig
  ) {
    scm = mock(StorageContainerManager.class);
    cluster = testableCluster;
    mockNodeManager = new MockNodeManager(cluster.getDatanodeToContainersMap());
    ozoneCfg = ozoneConfig;
    balancerCfg = balancerConfig;
  }

  /**
   * Mock some instances that will be used for MockedStorageContainerManager.
   */
  private void doMock() throws IOException, NodeNotFoundException, TimeoutException {
    containerManager = mockContainerManager(cluster);
    mockedReplicaManager = MockedReplicationManager.doMock();
    moveManager = mockMoveManager();
    StatefulServiceStateManager stateManager = MockedServiceStateManager.doMock();
    SCMServiceManager scmServiceManager = mockSCMServiceManger();

    mockedPlacementPolicies = MockedPlacementPolicies.doMock(ozoneCfg, mockNodeManager);

    when(scm.getConfiguration()).thenReturn(ozoneCfg);
    when(scm.getMoveManager()).thenReturn(moveManager);
    when(scm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(scm.getContainerManager()).thenReturn(containerManager);
    when(scm.getReplicationManager()).thenReturn(mockedReplicaManager.manager);
    when(scm.getContainerPlacementPolicy()).thenReturn(mockedPlacementPolicies.placementPolicy);
    when(scm.getPlacementPolicyValidateProxy()).thenReturn(mockedPlacementPolicies.validateProxyPolicy);
    when(scm.getSCMServiceManager()).thenReturn(scmServiceManager);
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
    when(scm.getClusterMap()).thenReturn(null);
    when(scm.getEventQueue()).thenReturn(mock(EventPublisher.class));
    when(scm.getStatefulServiceStateManager()).thenReturn(stateManager);
  }

  private static @Nonnull ContainerBalancerConfiguration createBalancerCfg(@Nonnull OzoneConfiguration ozoneCfg) {
    ContainerBalancerConfiguration balancerCfg = ozoneCfg.getObject(ContainerBalancerConfiguration.class);
    balancerCfg.setThreshold(10);
    balancerCfg.setIterations(1);
    balancerCfg.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerCfg.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerCfg.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    ozoneCfg.setFromObject(balancerCfg);
    return balancerCfg;
  }

  public static @Nonnull MockedSCM getMockedSCM(int datanodeCount) {
    OzoneConfiguration ozoneCfg = new OzoneConfiguration();
    ContainerBalancerConfiguration balancerCfg = createBalancerCfg(ozoneCfg);
    return getMockedSCM(datanodeCount, ozoneCfg, balancerCfg);
  }

  public static @Nonnull MockedSCM getMockedSCM(
      int datanodeCount,
      @Nonnull OzoneConfiguration ozoneCfg,
      @Nonnull ContainerBalancerConfiguration balancerCfg
  ) {
    TestableCluster cluster = new TestableCluster(datanodeCount, STORAGE_UNIT);
    MockedSCM mockedSCM = new MockedSCM(cluster, ozoneCfg, balancerCfg);
    try {
      mockedSCM.doMock();
      mockedSCM.initContainerBalancer();

    } catch (IOException | NodeNotFoundException | TimeoutException e) {
      throw new RuntimeException("Can't initialize TestOzoneHDDS: ", e);
    }
    return mockedSCM;
  }

  private void initContainerBalancer() {
    containerBalancer = new ContainerBalancer(scm);
  }

  @Override
  public String toString() {
    return cluster.toString();
  }

  public @Nonnull ContainerBalancer createContainerBalancer() {
    return new ContainerBalancer(scm);
  }

  public @Nonnull ContainerBalancerTask startBalancerTask(
      @Nonnull ContainerBalancer balancer,
      @Nonnull ContainerBalancerConfiguration config
  ) {
    ContainerBalancerTask task = new ContainerBalancerTask(scm, balancer, config);
    task.run(0, false);
    return task;
  }

  public @Nonnull ContainerBalancerTask startBalancerTask(@Nonnull ContainerBalancerConfiguration config) {
    return startBalancerTask(new ContainerBalancer(scm), config);
  }

  public void enableLegacyReplicationManager() {
    mockedReplicaManager.conf.setEnableLegacy(true);
  }

  public void disableLegacyReplicationManager() {
    mockedReplicaManager.conf.setEnableLegacy(false);
  }

  public @Nonnull MoveManager getMoveManager() {
    return moveManager;
  }

  public @Nonnull ReplicationManager getReplicationManager() {
    return mockedReplicaManager.manager;
  }

  public @Nonnull ContainerBalancerConfiguration getBalancerConfig() {
    return balancerCfg;
  }

  public @Nonnull MockNodeManager getNodeManager() {
    return mockNodeManager;
  }

  public @Nonnull OzoneConfiguration getOzoneConfig() {
    return ozoneCfg;
  }

  public @Nonnull ContainerBalancerConfiguration getBalancerConfigByOzoneConfig(@Nonnull OzoneConfiguration config) {
    return config.getObject(ContainerBalancerConfiguration.class);
  }

  public @Nonnull StorageContainerManager getStorageContainerManager() {
    return scm;
  }

  public @Nonnull TestableCluster getCluster() {
    return cluster;
  }

  public @Nonnull ContainerBalancer getContainerBalancer() {
    return containerBalancer;
  }

  public @Nonnull ContainerManager getContainerManager() {
    return containerManager;
  }

  public @Nonnull PlacementPolicy getPlacementPolicy() {
    return mockedPlacementPolicies.placementPolicy;
  }

  public @Nonnull PlacementPolicy getEcPlacementPolicy() {
    return mockedPlacementPolicies.ecPlacementPolicy;
  }
  private static @Nonnull ContainerManager mockContainerManager(@Nonnull TestableCluster cluster)
      throws ContainerNotFoundException {
    ContainerManager containerManager = mock(ContainerManager.class);
    Mockito
        .when(containerManager.getContainerReplicas(any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cluster.getCidToReplicasMap().get(cid);
        });

    Mockito
        .when(containerManager.getContainer(any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cluster.getCidToInfoMap().get(cid);
        });

    Mockito
        .when(containerManager.getContainers())
        .thenReturn(new ArrayList<>(cluster.getCidToInfoMap().values()));
    return containerManager;
  }

  private static @Nonnull SCMServiceManager mockSCMServiceManger() {
    SCMServiceManager scmServiceManager = mock(SCMServiceManager.class);

    Mockito
        .doNothing()
        .when(scmServiceManager)
        .register(Mockito.any(SCMService.class));

    return scmServiceManager;
  }

  private static @Nonnull MoveManager mockMoveManager()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MoveManager moveManager = mock(MoveManager.class);
    Mockito
        .when(moveManager
                .move(
                    any(ContainerID.class),
                    any(DatanodeDetails.class),
                    any(DatanodeDetails.class)))
        .thenReturn(
            CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED)
        );
    return moveManager;
  }

  private static final class MockedReplicationManager {
    private final ReplicationManager manager;
    private final ReplicationManager.ReplicationManagerConfiguration conf;

    private MockedReplicationManager() {
      manager = mock(ReplicationManager.class);
      conf = new ReplicationManager.ReplicationManagerConfiguration();
      /*
      Disable LegacyReplicationManager. This means balancer should select RATIS as well as EC containers for balancing.
       Also, MoveManager will be used.
      */
      conf.setEnableLegacy(false);
    }

    private static @Nonnull MockedReplicationManager doMock()
        throws NodeNotFoundException, ContainerNotFoundException, TimeoutException {
      MockedReplicationManager mockedManager = new MockedReplicationManager();

      Mockito
          .when(mockedManager.manager.getConfig())
          .thenReturn(mockedManager.conf);

      Mockito
          .when(mockedManager.manager.isContainerReplicatingOrDeleting(Mockito.any(ContainerID.class)))
          .thenReturn(false);

      Mockito
          .when(mockedManager.manager.move(
                Mockito.any(ContainerID.class),
                Mockito.any(DatanodeDetails.class),
                Mockito.any(DatanodeDetails.class)))
          .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));

      Mockito
          .when(mockedManager.manager.getClock())
          .thenReturn(Clock.system(ZoneId.systemDefault()));

      return mockedManager;
    }
  }

  private static final class MockedServiceStateManager {
    private final Map<String, ByteString> serviceToConfigMap = new HashMap<>();
    private final StatefulServiceStateManager serviceStateManager = Mockito.mock(StatefulServiceStateManagerImpl.class);

    private static @Nonnull StatefulServiceStateManager doMock() throws IOException {
      MockedServiceStateManager manager = new MockedServiceStateManager();
      // When StatefulServiceStateManager#saveConfiguration is called, save to in-memory serviceToConfigMap instead.
      Map<String, ByteString> map = manager.serviceToConfigMap;

      StatefulServiceStateManager stateManager = manager.serviceStateManager;
      Mockito
          .doAnswer(i -> {
            map.put(i.getArgument(0, String.class), i.getArgument(1, ByteString.class));
            return null;
          })
          .when(stateManager)
          .saveConfiguration(Mockito.any(String.class), Mockito.any(ByteString.class));

      // When StatefulServiceStateManager#readConfiguration is called, read from serviceToConfigMap instead.
      Mockito
          .when(stateManager.readConfiguration(Mockito.anyString()))
          .thenAnswer(i -> map.get(i.getArgument(0, String.class)));
      return stateManager;
    }
  }

  private static final class MockedPlacementPolicies {
    private final PlacementPolicy placementPolicy;
    private final PlacementPolicy ecPlacementPolicy;
    private final PlacementPolicyValidateProxy validateProxyPolicy;

    private MockedPlacementPolicies(@Nonnull PlacementPolicy placementPolicy, @Nonnull PlacementPolicy ecPolicy) {
      this.placementPolicy = placementPolicy;
      ecPlacementPolicy = ecPolicy;
      validateProxyPolicy = new PlacementPolicyValidateProxy(this.placementPolicy, ecPlacementPolicy);
    }

    private static @Nonnull MockedPlacementPolicies doMock(
        @Nonnull OzoneConfiguration ozoneConfig,
        @Nonnull NodeManager nodeManager
    ) throws SCMException {
      NetworkTopology clusterMap = nodeManager.getClusterNetworkTopologyMap();
      PlacementPolicy policy = ContainerPlacementPolicyFactory.getPolicy(
          ozoneConfig, nodeManager, clusterMap, true,
          SCMContainerPlacementMetrics.create());
      PlacementPolicy ecPolicy = ContainerPlacementPolicyFactory.getECPolicy(
          ozoneConfig, nodeManager, clusterMap, true,
          SCMContainerPlacementMetrics.create());
      return new MockedPlacementPolicies(policy, ecPolicy);
    }
  }
}
