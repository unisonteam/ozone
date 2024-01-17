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

package org.apache.hadoop.hdds.scm.pipeline;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Integration test to ensure data will be written even if pipeline or container will be closed.
 * NOTE: Testing cluster with ScmConfigKeys:
 */
public class TestWritingDataWhenClosePipelineOrContainer {
  private static final int CHUNK_SIZE = (int) OzoneConsts.MB;
  private static final int BLOCK_SIZE = 4 * CHUNK_SIZE;
  private MiniOzoneCluster cluster;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private PipelineManager pipelineManager;
  private ContainerManager containerManager;
  private String keyName;

  private static @Nonnull OzoneConfiguration getDefaultOzoneConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    int numContainerPerPipeline = conf.getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);
    assertEquals(numContainerPerPipeline, ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, 100, TimeUnit.SECONDS);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 15);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);
    conf.setQuietMode(false);
    return conf;
  }

  public void startCluster(@Nonnull OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // the easiest way to create an open container is creating a key
    objectStore = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    volumeName = "datanodefailurehandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);

    StorageContainerManager scm = cluster.getStorageContainerManager();
    pipelineManager = scm.getPipelineManager();
    containerManager = scm.getContainerManager();
    keyName = UUID.randomUUID().toString();
  }

  @AfterEach
  public void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @ParameterizedTest(name = "Writeable data size is {0}MB")
  @ValueSource(ints = {1, 4, 11})
  public void testContainerClose(int chunkCount) throws Exception {
    runTestContainerCloseWithDataWriting(getDefaultOzoneConf(), fillData(chunkCount));
  }

  @ParameterizedTest(name = "Writeable data size is {0}MB")
  @ValueSource(ints = {1, 4, 11})
  public void testPipelineAndContainerClose(int chunkCount) throws Exception {
    runTestPipelineAndContainerCloseWithDataWriting(getDefaultOzoneConf(), fillData(chunkCount));
  }

  @ParameterizedTest(name = "Writeable data size is {0}MB")
  @ValueSource(ints = {1, 4, 11})
  public void testPipelineClose(int chunkCount) throws Exception {
    runTestPipelineCloseWithDataWriting(getDefaultOzoneConf(), fillData(chunkCount));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} container(s) per pipeline")
  @ValueSource(ints = {3, 8})
  public void testContainerCloseWithContainerLimit(int containerCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, containerCount);
    runTestContainerCloseWithDataWriting(conf, fillData(21));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} container(s) per pipeline")
  @ValueSource(ints = {3, 8})
  public void testPipelineAndContainerCloseWithContainerLimit(int containerCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, containerCount);
    runTestPipelineAndContainerCloseWithDataWriting(conf, fillData(21));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} container(s) per pipeline")
  @ValueSource(ints = {3, 8})
  public void testPipelineCloseWithContainerLimit(int containerCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, containerCount);
    runTestPipelineCloseWithDataWriting(conf, fillData(21));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} pipelines(s) per datanode")
  @ValueSource(ints = {3, 6, 11})
  public void testContainerCloseWithPipelinesLimit(int pipelineCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, pipelineCount);
    runTestContainerCloseWithDataWriting(conf, fillData(21));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} container(s) per pipeline")
  @ValueSource(ints = {3, 6, 11})
  public void testPipelineAndContainerCloseWithPipelinesLimit(int pipelineCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, pipelineCount);
    runTestPipelineAndContainerCloseWithDataWriting(conf, fillData(21));
  }

  @ParameterizedTest(name = "Writing 21MB data with {0} container(s) per pipeline")
  @ValueSource(ints = {3, 6, 11})
  public void testPipelineCloseWithPipelinesLimit(int pipelineCount) throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, pipelineCount);
    runTestPipelineCloseWithDataWriting(conf, fillData(21));
  }

  @Test
  public void testWriteLargeFile() throws Exception {
    OzoneConfiguration conf = getDefaultOzoneConf();
    startCluster(conf);
    OzoneClient client = OzoneClientFactory.getRpcClient(conf);

    String data = ContainerTestHelper.getFixedLengthString(keyName, (int) OzoneConsts.MB);

    CountDownLatch latch = new CountDownLatch(1);
    KeyOutputStream keyOutputStream = getKeyOutputStream(client, conf);
    OzoneOutputStream key = mockOzoneOutputStream(keyOutputStream, latch);

    new Thread(() -> {
      try {
        Thread.sleep(5000);
        List<Pipeline> pipelines = pipelineManager.getPipelines();
        for (Pipeline pipeline : pipelines) {
          PipelineID id = pipeline.getId();
          pipelineManager.closePipeline(id);
          pipelineManager.deletePipeline(id);
        }
        Assertions.assertEquals(0, pipelineManager.getPipelines().size());
        latch.countDown();
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    }).start();

    key.write(data.getBytes(UTF_8));
    cluster.waitForPipelineTobeReady(THREE, 5000);

    // get the name of a valid container
    OutputStream outputStream = key.getOutputStream();
    Assertions.assertTrue(outputStream instanceof KeyOutputStream);

    List<OmKeyLocationInfo> locationInfoList = keyOutputStream.getLocationInfoList();
    BlockID blockId = locationInfoList.get(0).getBlockID();
    key.close();

    Assertions.assertEquals(1, pipelineManager.getPipelines().size());
    // this will throw AlreadyClosedException and and current stream
    // will be discarded and write a new block
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    // Make sure a new block is written
    Assertions.assertNotEquals(keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0).getBlockID(),
        blockId);
    Assertions.assertEquals(data.getBytes(UTF_8).length, keyInfo.getDataSize());
    TestHelper.validateData(keyName, data.getBytes(UTF_8), objectStore, volumeName, bucketName);
  }

  private void runTestPipelineAndContainerCloseWithDataWriting(
      @Nonnull OzoneConfiguration conf,
      @Nonnull byte[] data
  ) throws Exception {
    startCluster(conf);
    OzoneOutputStream key = getOzoneOutputStream();

    // Write data once
    int expectedBlockCount = (int) Math.ceil((double) data.length / BLOCK_SIZE);
    writeData(data, key, expectedBlockCount);
    int expectedPipelineCount =
        conf.getInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    List<Pipeline> firstPipelineList = getPipelines(expectedPipelineCount);

    // Get container info
    CInfo firstContainerInfo = getCInfoForPipeline(firstPipelineList, key, 0);
    int nextStreamEntryIndex = key.getKeyOutputStream().getStreamEntries().size();

    // Close containers & pipelines
    closeContainers(firstContainerInfo, expectedPipelineCount);
    closePipelines(firstContainerInfo.pipelineList);
    // New pipeline will be created here org.apache.hadoop.hdds.scm.pipeline.WritableRatisContainerProvider.java:116

    // Try to write after closing containers and pipelines
    writeData(data, key, 2 * expectedBlockCount);
    // todo: why do we have a single pipeline, not two as was before closing?
    List<Pipeline> secondPipelineList = getPipelines(1);

    // Verify that we excluded closed container with from output stream
    verifyExcludeContainers(key.getKeyOutputStream().getExcludeList(), data.length);

    // Verify that a new pipeline and a new container with ID = 2 are allocated when we wrote data after closing
    // the initialContainer and initialPipeline
    CInfo secondContainerInfo = getCInfoForPipeline(secondPipelineList, key, nextStreamEntryIndex);

    // Copy streamEntryList to array, because then streamEntryList will be empty after key close.
    BlockOutputStreamEntry[] streamEntryList = getBlockOutputStreamEntryArray(key);
    verifyBlockOutputStreams(streamEntryList, firstContainerInfo, secondContainerInfo, expectedBlockCount);

    // The close will just write to the buffer
    key.close();

    verifyDataAndBlockIds(data, streamEntryList);
  }

  private void runTestContainerCloseWithDataWriting(
      @Nonnull OzoneConfiguration conf,
      @Nonnull byte[] data
  ) throws Exception {
    startCluster(conf);
    OzoneOutputStream key = getOzoneOutputStream();

    // Write data once
    int expectedBlockCount = (int) Math.ceil((double) data.length / BLOCK_SIZE);
    writeData(data, key, expectedBlockCount);
    int expectedPipelineCount =
        conf.getInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    List<Pipeline> firstPipelineList = getPipelines(expectedPipelineCount);

    // Get container info
    CInfo firstContainerInfo = getCInfoForPipeline(firstPipelineList, key, 0);
    int nextStreamEntryIndex = key.getKeyOutputStream().getStreamEntries().size();

    // Close container
    closeContainers(firstContainerInfo, expectedPipelineCount);

    // Try to write after closing containers
    writeData(data, key, 2 * expectedBlockCount);
    List<Pipeline> secondPipelineList = getPipelines(expectedPipelineCount);

    // Verify that we excluded closed container with from output stream
    verifyExcludeContainers(key.getKeyOutputStream().getExcludeList(), data.length);

    // Get container info after second writing after closing containers
    CInfo secondContainerInfo = getCInfoForPipeline(secondPipelineList, key, nextStreamEntryIndex);

    // Copy streamEntryList to array, because then streamEntryList will be empty after key close.
    BlockOutputStreamEntry[] streamEntryList = getBlockOutputStreamEntryArray(key);
    verifyBlockOutputStreams(streamEntryList, firstContainerInfo, secondContainerInfo, expectedBlockCount);

    // The close will just write to the buffer
    key.close();

    verifyDataAndBlockIds(data, streamEntryList);
  }

  private void runTestPipelineCloseWithDataWriting(
      @Nonnull OzoneConfiguration conf,
      @Nonnull byte[] data
  ) throws Exception {
    startCluster(conf);
    OzoneOutputStream key = getOzoneOutputStream();

    // Write data once
    int expectedBlockCount = (int) Math.ceil((double) data.length / BLOCK_SIZE);
    writeData(data, key, expectedBlockCount);
    List<Pipeline> firstPipelineList = getPipelines(2);

    // Get blockId, containerId, container and pipeline
    CInfo firstContainerInfo = getCInfoForPipeline(firstPipelineList, key, 0);
    int nextStreamEntryIndex = key.getKeyOutputStream().getStreamEntries().size();

    // Close pipelines
    closePipelines(firstContainerInfo.pipelineList);
    // New pipeline will be created here org.apache.hadoop.hdds.scm.pipeline.WritableRatisContainerProvider.java:116

    // Try to write after closing pipelines
    int expectedBlockCount2 = (int) Math.ceil((double) 2 * data.length / BLOCK_SIZE);
    writeData(data, key, expectedBlockCount2);

    // Verify that we didn't exclude opened container or pipeline from output stream
    ExcludeList excludeList = key.getKeyOutputStream().getExcludeList();
    assertTrue(excludeList.getPipelineIds().isEmpty());
    assertTrue(excludeList.getContainerIds().isEmpty());

    // Copy streamEntryList to array, because then streamEntryList will be empty after key close.
    BlockOutputStreamEntry[] streamEntryList = getBlockOutputStreamEntryArray(key);
    boolean dataFitInOneBlock = 2 * data.length <= BLOCK_SIZE;
    if (dataFitInOneBlock) {
      verifyBlockOutputStreams(streamEntryList, firstContainerInfo, null, expectedBlockCount);
    } else {
      List<Pipeline> secondPipelineList = getPipelines(1);
      // Get container info after second writing after closing containers
      CInfo secondContainerInfo = getCInfoForPipeline(secondPipelineList, key, nextStreamEntryIndex);
      verifyBlockOutputStreams(streamEntryList, firstContainerInfo, secondContainerInfo, expectedBlockCount);
    }

    // The close will just write to the buffer
    key.close();

    verifyDataAndBlockIds(data, streamEntryList);
  }

  private @Nonnull List<Pipeline> getPipelines(int expectedPipelineCount) {
    assertEquals(expectedPipelineCount, pipelineManager.getPipelines().size());
    return pipelineManager.getPipelines();
  }

  private static byte[] fillData(int chunkCount) {
    String keyString = UUID.randomUUID().toString();
    return ContainerTestHelper.getFixedLengthString(keyString, chunkCount * CHUNK_SIZE).getBytes(UTF_8);
  }

  private void verifyBlockOutputStreams(
      @Nonnull BlockOutputStreamEntry[] streamEntryList,
      @Nonnull CInfo firstContainerInfo,
      @Nullable CInfo secondContainerInfo,
      int expectedBlockCount
  ) {
    // Verify that block output stream entries contain expected pipelineId and containerId.
    for (int i = 0; i < streamEntryList.length; ++i) {
      if (i < expectedBlockCount) {
        verifyBlockOutputStream(streamEntryList[i], firstContainerInfo.pipelineList, firstContainerInfo.cIdList);
      } else {
        if (secondContainerInfo != null) {
          verifyBlockOutputStream(streamEntryList[i], secondContainerInfo.pipelineList, secondContainerInfo.cIdList);
        }
      }
    }
  }

  @Nonnull
  private static BlockOutputStreamEntry[] getBlockOutputStreamEntryArray(@Nonnull OzoneOutputStream key) {
    return key.getKeyOutputStream().getStreamEntries().toArray(new BlockOutputStreamEntry[0]);
  }

  @Nonnull
  private CInfo getCInfoForPipeline(
      @Nonnull List<Pipeline> pipelineList,
      @Nonnull OzoneOutputStream key,
      int nextStreamEntryIdx
  ) throws IOException {
    List<BlockOutputStreamEntry> streamEntries = key.getKeyOutputStream().getStreamEntries();
    Set<Long> cIdList = new HashSet<>();
    for (int i = nextStreamEntryIdx; i < streamEntries.size(); ++i) {
      BlockID blockId = streamEntries.get(i).getBlockID();
      ContainerInfo cInfo = containerManager.getContainer(ContainerID.valueOf(blockId.getContainerID()));
      Pipeline pipeline = pipelineManager.getPipeline(cInfo.getPipelineID());
      assertThat(pipelineList).contains(pipeline);
      assertTrue(pipeline.isHealthy() && pipeline.isOpen());
      cIdList.add(cInfo.getContainerID());
    }
    return new CInfo(pipelineList, cIdList);
  }

  @Nonnull
  private OzoneOutputStream getOzoneOutputStream() throws Exception {
    OzoneOutputStream key = TestHelper.createKey(keyName, RATIS, BLOCK_SIZE, objectStore, volumeName, bucketName);
    assertInstanceOf(KeyOutputStream.class, key.getOutputStream());
    assertEquals(1, key.getKeyOutputStream().getStreamEntries().size()); // assert that 1 block preallocated
    return key;
  }

  private static void writeData(@Nonnull byte[] data, @Nonnull OzoneOutputStream key, int expectedBlockCount)
      throws IOException {
    key.write(data);
    key.flush();
    assertEquals(expectedBlockCount, key.getKeyOutputStream().getStreamEntries().size());
  }

  private void closePipelines(@Nonnull List<Pipeline> pipelineList) throws IOException {
    for (Pipeline pipeline : pipelineList) {
      PipelineID id = pipeline.getId();
      pipelineManager.closePipeline(id);
      pipelineManager.deletePipeline(id);
    }
    assertTrue(pipelineManager.getPipelines().isEmpty());
  }

  private void closeContainers(
      @Nonnull CInfo cInfo,
      int expectedPipelineCount
  ) throws TimeoutException, InterruptedException, IOException {
    // Verify that there is still only one pipeline exists and it doesn't have any containers
    for (Long cId : cInfo.cIdList) {
      TestHelper.waitForContainerClose(cluster, cId);
    }
    for (Pipeline pipeline : cInfo.pipelineList) {
      assertEquals(0, pipelineManager.getContainersInPipeline(pipeline.getId()).size());
    }
    assertEquals(expectedPipelineCount, pipelineManager.getPipelines().size());
  }

  private static void verifyExcludeContainers(@Nonnull ExcludeList excludeList, int dataLength) {
    // Verify that we didn't exclude any pipelines
    assertTrue(excludeList.getPipelineIds().isEmpty());

    // If data size is multiple of block size, then there will be no excluded containers.
    // They will be closed automatically after writing data without any exceptions.
    Set<ContainerID> excludeListContainerIds = excludeList.getContainerIds();
    if (dataLength % BLOCK_SIZE != 0) {
      assertEquals(1, excludeListContainerIds.size());
    } else {
      assertTrue(excludeListContainerIds.isEmpty());
    }
  }

  private void verifyDataAndBlockIds(@Nonnull byte[] data, @Nonnull BlockOutputStreamEntry[] streamEntryList)
      throws Exception {
    byte[] expectedData = Arrays.copyOf(data, 2 * data.length);
    System.arraycopy(data, 0, expectedData, data.length, data.length);
    BlockID[] blockIdArr = new BlockID[streamEntryList.length];
    for (int i = 0; i < streamEntryList.length; ++i) {
      blockIdArr[i] = streamEntryList[i].getBlockID();
    }
    validateDataForKey(expectedData, blockIdArr);
  }

  private void validateDataForKey(@Nonnull byte[] expectedData, @Nonnull BlockID[] blockIds) throws Exception {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setKeyName(keyName).build();

    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup latestVersionLocations = keyInfo.getLatestVersionLocations();
    assertNotNull(latestVersionLocations);
    List<OmKeyLocationInfo> blocksLatestVersionOnly = latestVersionLocations.getBlocksLatestVersionOnly();
    for (int i = 0; i < blocksLatestVersionOnly.size(); ++i) {
      OmKeyLocationInfo omKeyLocationInfo = blocksLatestVersionOnly.get(i);
      assertEquals(blockIds[i], omKeyLocationInfo.getBlockID());
    }
    assertEquals(expectedData.length, keyInfo.getDataSize());
    TestHelper.validateData(keyName, expectedData, objectStore, volumeName, bucketName);
  }

  private static void verifyBlockOutputStream(@Nonnull BlockOutputStreamEntry streamEntry,
                                              @Nonnull List<Pipeline> expectedPipelineList,
                                              @Nonnull Set<Long> expectedContainerSet
  ) {
    BlockID blockID = streamEntry.getBlockID();
    assertThat(expectedContainerSet).contains(blockID.getContainerID());
    assertThat(expectedPipelineList).contains(streamEntry.getPipeline());
  }

  private static final class CInfo {
    private final List<Pipeline> pipelineList;
    private final Set<Long> cIdList;

    private CInfo(@Nonnull List<Pipeline> pipelineId, @Nonnull Set<Long> containerIdList) {
      pipelineList = pipelineId;
      cIdList = containerIdList;
    }
  }

  @Nonnull
  private static OzoneOutputStream mockOzoneOutputStream(
      @Nonnull KeyOutputStream keyOutputStream,
      @Nonnull CountDownLatch latch
  ) throws IOException {
    OzoneOutputStream key = Mockito.mock(OzoneOutputStream.class);

    doAnswer(invoke -> {
      byte[] arg = invoke.getArgument(0);
      latch.await();
      keyOutputStream.write(arg);
      return null;
    }).when(key).write(Mockito.any(byte[].class));

    when(key.getKeyOutputStream()).thenReturn(keyOutputStream);
    when(key.getOutputStream()).thenReturn(keyOutputStream);

    doAnswer(invocationOnMock -> {
      keyOutputStream.close();
      return null;
    }).when(key).close();
    return key;
  }

  @Nonnull
  private KeyOutputStream getKeyOutputStream(
      @Nonnull OzoneClient client,
      @Nonnull OzoneConfiguration conf
  ) throws IOException {
    org.apache.hadoop.hdds.client.ReplicationFactor factor = org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
    ReplicationConfig config = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, factor);

    boolean getLatestVersionLocation = conf.getBoolean(
        OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION,
        OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION_DEFAULT);
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(0)
        .setReplicationConfig(config)
        .addAllMetadataGdpr(new HashMap<>())
        .setLatestVersionLocation(getLatestVersionLocation);


    RpcClient proxy = (RpcClient) client.getProxy();
    OzoneManagerProtocol ozoneManagerClient = proxy.getOzoneManagerClient();
    OpenKeySession openKey = ozoneManagerClient.openKey(builder.build());

    try {
      Method m = RpcClient.class.getDeclaredMethod("createKeyOutputStream", OpenKeySession.class);
      m.setAccessible(true);
      MethodHandle createKeyOutputStream = MethodHandles.lookup().unreflect(m);
      KeyOutputStream.Builder invoke = (KeyOutputStream.Builder) createKeyOutputStream.invoke(proxy, openKey);
      return invoke.build();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
}
