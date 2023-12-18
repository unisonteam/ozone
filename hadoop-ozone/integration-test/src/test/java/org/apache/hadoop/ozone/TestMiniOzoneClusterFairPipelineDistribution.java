/**
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

package org.apache.hadoop.ozone;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.scm.pipeline.TestRatisThreePipelineDistribution.verifyPipelineCountForNodes;

/**
 * Test cases to verify the metrics exposed by SCMPipelineManager.
 */
public class TestMiniOzoneClusterFairPipelineDistribution {
  private static final OzoneConfiguration OZONE_CONFIGURATION = new OzoneConfiguration();

  static {
    OZONE_CONFIGURATION.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, false);
    OZONE_CONFIGURATION.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    OZONE_CONFIGURATION.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 10);
  }

  private static MiniOzoneClusterImpl initCluster(@Nonnull String id, int datanodeCount)
      throws InterruptedException, TimeoutException, IOException {
    String omServiceId = "om-service-test" + id;
    String scmServiceId = "scm-service-test" + id;

    MiniOzoneClusterImpl cluster = (MiniOzoneClusterImpl) MiniOzoneCluster.newBuilder(
            TestMiniOzoneClusterFairPipelineDistribution.OZONE_CONFIGURATION)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumDatanodes(datanodeCount)
        .build();
    cluster.waitForClusterToBeReady();

    return cluster;
  }

  private static Stream<Arguments> generateCluster() throws IOException, InterruptedException, TimeoutException {
    return Stream.of(
        Arguments.of(initCluster("1", 3)),
        Arguments.of(initCluster("2", 4)),
        Arguments.of(initCluster("3", 5)),
        Arguments.of(initCluster("4", 6)),
        Arguments.of(initCluster("5", 7)),
        Arguments.of(initCluster("6", 8)),
        Arguments.of(initCluster("7", 9)),
        Arguments.of(initCluster("8", 10))
    );
  }

  @ParameterizedTest()
  @MethodSource("generateCluster")
  public void fairPipelineDistribution(@Nonnull MiniOzoneClusterImpl cluster) {
    StorageContainerManager scm = cluster.getActiveSCM();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    verifyPipelineCountForNodes(scm.getPipelineManager(), scmNodeManager);
    cluster.shutdown();
  }
}
