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
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;

/**
 * Tests for {@link ContainerBalancer}.
 */
@Timeout(60)
public class TestContainerBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerBalancer.class);

  private MockedSCM mockedScm;

  @BeforeAll
  public static void setupAll() {
    GenericTestUtils.setLogLevel(ContainerBalancer.LOG, Level.DEBUG);
  }
  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 5, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 2, TimeUnit.SECONDS);

    ContainerBalancerConfiguration balancerConfiguration = conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(10);
    // Note: this will make container balancer task to wait for running for 6 sec as default and
    // ensure below test case have sufficient time to verify, and interrupt when stop.
    balancerConfiguration.setTriggerDuEnable(true);
    conf.setFromObject(balancerConfiguration);

    mockedScm = MockedSCM.getMockedSCM(10, conf, balancerConfiguration);
  }

  @Test
  public void testShouldRun() throws Exception {
    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    boolean doRun = containerBalancer.shouldRun();
    Assertions.assertFalse(doRun);
    containerBalancer.saveConfiguration(mockedScm.getBalancerConfig(), true, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertTrue(doRun);
    containerBalancer.saveConfiguration(mockedScm.getBalancerConfig(), false, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertFalse(doRun);
  }

  @Test
  public void testStartBalancerStop() throws Exception {
    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    containerBalancer.startBalancer(mockedScm.getBalancerConfig());
    try {
      containerBalancer.startBalancer(mockedScm.getBalancerConfig());
      Assertions.fail("Exception should be thrown when startBalancer again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    try {
      containerBalancer.start();
      Assertions.fail("Exception should be thrown when start again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.RUNNING);

    stopBalancer();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.STOPPED);

    try {
      containerBalancer.stopBalancer();
      Assertions.fail("Exception should be thrown when stop again");
    } catch (Exception e) {
      // stop failed as already stopped, valid case
    }
  }

  @Test
  public void testStartStopSCMCalls() throws Exception {
    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    containerBalancer.saveConfiguration(mockedScm.getBalancerConfig(), true, 0);
    containerBalancer.start();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    try {
      containerBalancer.start();
      Assertions.fail("Exception should be thrown when start again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed when triggered again, valid case
    }

    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.STOPPED);
    containerBalancer.saveConfiguration(mockedScm.getBalancerConfig(), false, 0);
  }

  @Test
  public void testNotifyStateChangeStopStart() throws Exception {
    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    containerBalancer.startBalancer(mockedScm.getBalancerConfig());

    StorageContainerManager scm = mockedScm.getStorageContainerManager();
    scm.getScmContext().updateLeaderAndTerm(false, 1);
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.STOPPED);
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertSame(containerBalancer.getBalancerStatus(), ContainerBalancerTask.Status.STOPPED);
  }

  /**
   * This tests that ContainerBalancer rejects certain invalid configurations while starting.
   * It should fail to start in some cases.
   */
  @Test
  public void testValidationOfConfigurations() {
    OzoneConfiguration conf = mockedScm.getOzoneConfig();
    conf.setTimeDuration("hdds.container.balancer.move.replication.timeout", 60, TimeUnit.MINUTES);
    conf.setTimeDuration("hdds.container.balancer.move.timeout", 59, TimeUnit.MINUTES);

    ContainerBalancerConfiguration balancerConfiguration = mockedScm.getBalancerConfigByOzoneConfig(conf);
    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    Assertions.assertThrowsExactly(
        InvalidContainerBalancerConfigurationException.class,
        () -> containerBalancer.startBalancer(balancerConfiguration),
        "hdds.container.balancer.move.replication.timeout should be less than hdds.container.balancer.move.timeout.");
  }

  /**
   * Tests that ContainerBalancerTask starts with a delay of "hdds.scm.wait.time.after.safemode.exit"
   * when ContainerBalancer receives status change notification in {@link ContainerBalancer#notifyStatusChanged()}.
   */
  @Test
  public void testDelayedStartOnSCMStatusChange()
      throws IllegalContainerBalancerStateException, IOException, InvalidContainerBalancerConfigurationException,
      TimeoutException, InterruptedException {
    OzoneConfiguration ozoneCfg = mockedScm.getOzoneConfig();
    long delayDuration = ozoneCfg.getTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 10, TimeUnit.SECONDS);
    ContainerBalancerConfiguration balancerConfiguration = mockedScm.getBalancerConfigByOzoneConfig(ozoneCfg);

    ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
    // Start the ContainerBalancer service.
    containerBalancer.startBalancer(balancerConfiguration);
    GenericTestUtils.waitFor(() -> containerBalancer.isBalancerRunning(), 1, 20);
    Assertions.assertTrue(containerBalancer.isBalancerRunning());

    StorageContainerManager scm = mockedScm.getStorageContainerManager();
    // Balancer should stop the current balancing thread when it receives a status change notification
    scm.getScmContext().updateLeaderAndTerm(false, 1);
    containerBalancer.notifyStatusChanged();
    Assertions.assertFalse(containerBalancer.isBalancerRunning());

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.captureLogs(ContainerBalancerTask.LOG);
    String expectedLog = "ContainerBalancer will sleep for " + delayDuration + " seconds before starting balancing.";

    // Send a status change notification again and check whether balancer starts balancing.
    // We're actually just checking for the expected log line here.
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.isBalancerRunning());
    Thread balancingThread = containerBalancer.getCurrentBalancingThread();
    GenericTestUtils.waitFor(() -> balancingThread.getState() == Thread.State.TIMED_WAITING, 2, 20);
    Assertions.assertTrue(logCapturer.getOutput().contains(expectedLog));
    stopBalancer();
  }

  private void stopBalancer() {
    try {
      ContainerBalancer containerBalancer = mockedScm.getContainerBalancer();
      if (containerBalancer.isBalancerRunning()) {
        containerBalancer.stopBalancer();
      }
    } catch (IOException | IllegalContainerBalancerStateException e) {
      LOG.warn("Failed to stop balancer", e);
    }
  }
}
