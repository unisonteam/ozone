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

import jakarta.annotation.Nonnull;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.ozone.test.GenericTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT;

/**
 * Testable SCM.
 */
public final class TestableSCM {
  public static final ReplicationConfig REPLICATION_CONFIG_RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(RATIS, THREE);
  private final int pipelinePerDatanode;
  private final File testDir;
  private final DBStore dbStore;
  private final ClusterWithoutRackAwareness cluster;
  private final MockNodeManager nodeManager;
  private final MockPipelineManager pipelineManager;

  private TestableSCM(
      @Nonnull File testDirectory,
      @Nonnull DBStore databaseStore,
      @Nonnull ClusterWithoutRackAwareness testableCluster,
      @Nonnull MockNodeManager mockedNodeManager,
      @Nonnull OzoneConfiguration conf,
      @Nonnull SCMHAManager scmhaManager
  ) throws IOException {
    testDir = testDirectory;
    dbStore = databaseStore;
    cluster = testableCluster;
    pipelinePerDatanode = conf.getInt(OZONE_DATANODE_PIPELINE_LIMIT, OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    nodeManager = mockedNodeManager;
    nodeManager.setNumPipelinePerDatanode(pipelinePerDatanode);
    pipelineManager = new MockPipelineManager(dbStore, scmhaManager, nodeManager) {
      private final RatisPipelineProvider provider =
          new MockRatisPipelineProvider(nodeManager, getStateManager(), conf);

      @Override
      public Pipeline createPipeline(ReplicationConfig replicationConfig, List<DatanodeDetails> favoredNodes) {
        try {
          return provider.create((RatisReplicationConfig) replicationConfig, Collections.emptyList(), favoredNodes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public String toString() {
    return cluster.getDatanodeList().size() + " nodes, pipeline limit: " + pipelinePerDatanode;
  }

  public void cleanup() throws IOException {
    dbStore.close();
    FileUtil.fullyDelete(testDir);
  }

  public @Nonnull MockNodeManager getNodeManager() {
    return nodeManager;
  }

  public void addRatisThreePipeline(
      @Nonnull DatanodeDetails dn1,
      @Nonnull DatanodeDetails dn2,
      @Nonnull DatanodeDetails dn3
  ) throws IOException {
    List<DatanodeDetails> ratisThreeDn = new ArrayList<>();
    ratisThreeDn.add(dn1);
    ratisThreeDn.add(dn2);
    ratisThreeDn.add(dn3);

    Pipeline pipeline = pipelineManager.createPipeline(REPLICATION_CONFIG_RATIS_THREE, ratisThreeDn);
    nodeManager.addPipeline(pipeline);
    HddsProtos.Pipeline pipelineProto = pipeline.getProtobufMessage(ClientVersion.CURRENT_VERSION);
    pipelineManager.getStateManager().addPipeline(pipelineProto);
  }

  public void addRatisThreePipeline() throws IOException {
    addRatisThreePipeline(
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails(),
        MockDatanodeDetails.randomDatanodeDetails());
  }

  public int getTotalPipelineCount() {
    return pipelineManager.getPipelineCount(TestableSCM.REPLICATION_CONFIG_RATIS_THREE, Pipeline.PipelineState.OPEN) +
        pipelineManager.getPipelineCount(TestableSCM.REPLICATION_CONFIG_RATIS_THREE, Pipeline.PipelineState.ALLOCATED);
  }

  public @Nonnull PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  /**
   * Builder for Testable SCM.
   */
  public static class Builder {
    private File testDir;
    private DBStore dbStore;
    private OzoneConfiguration conf;
    private MockNodeManager nodeManager;
    private ClusterWithoutRackAwareness cluster;
    private SCMHAManager scmhaManager;

    public @Nonnull Builder createTestDir(@Nonnull String testDirName) {
      testDir = GenericTestUtils.getTestDir(testDirName);
      return this;
    }

    public @Nonnull Builder setOzoneConfig(@Nonnull Function<File, OzoneConfiguration> configProvider) {
      if (testDir == null) {
        throw new IllegalStateException("Can't create OzoneConfiguration, testDir is null. " +
            "Use Builder::createTestDir to create test directory.");
      }
      conf = configProvider.apply(testDir);
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
      return this;
    }

    public @Nonnull Builder createDbStore() throws IOException {
      if (conf == null) {
        throw new IllegalStateException("Can't create DB store for unknown OzoneConfiguration, ozoneConf is null. " +
            "Use Builder::setOzoneConfig to specify some OzoneConfiguration.");
      }
      dbStore = DBStoreBuilder.createDBStore(conf, new SCMDBDefinition());
      return this;
    }

    public @Nonnull Builder setNodeManager(@Nonnull Supplier<MockNodeManager> nodeManagerProvider) {
      if (cluster == null) {
        throw new IllegalStateException("Can't create NodeManager, cluster is null. " +
            "Use Builder::setCluster to specify some ClusterWithoutRackAwareness.");
      }
      nodeManager = nodeManagerProvider.get();
      return this;
    }

    public @Nonnull Builder setCluster(@Nonnull Supplier<ClusterWithoutRackAwareness> clusterProvider) {
      cluster = clusterProvider.get();
      return this;
    }

    public Builder setSCM(@Nonnull SCMHAManager otherScmhaManager) {
      scmhaManager = otherScmhaManager;
      return this;
    }

    public @Nonnull TestableSCM build() throws IOException {
      if (testDir == null || dbStore == null ||
          cluster == null || nodeManager == null ||
          conf == null
      ) {
        throw new IllegalStateException("Testable SCM isn't configured properly. Check it out.");
      }
      if (scmhaManager == null) {
        scmhaManager = SCMHAManagerStub.getInstance(true);
      }
      return new TestableSCM(testDir, dbStore, cluster, nodeManager, conf, scmhaManager);
    }
  }
}
