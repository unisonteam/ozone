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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.*;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL_DEFAULT;

/**
 * Container balancer task performs move of containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancerTask implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(ContainerBalancerTask.class);

  private final NodeManager nodeManager;
  private final int nextIterationIndex;
  private final OzoneConfiguration ozoneConfiguration;
  private final ContainerBalancer containerBalancer;
  private final SCMContext scmContext;
  private final StorageContainerManager scm;

  private final ContainerBalancerConfiguration config;
  private volatile Status taskStatus = Status.RUNNING;

  private final boolean delayStart;
  private ContainerBalanceIteration it;

  /**
   * Constructs ContainerBalancerTask with the specified arguments.
   *
   * @param scm the storage container manager
   * @param nextIterationIndex next iteration index for continue
   * @param containerBalancer the container balancer
   * @param config the config
   */
  public ContainerBalancerTask(StorageContainerManager scm,
                               int nextIterationIndex,
                               ContainerBalancer containerBalancer,
                               ContainerBalancerConfiguration config,
                               boolean delayStart)
  {
    this.scm = scm;
    this.nodeManager = scm.getScmNodeManager();
    this.nextIterationIndex = nextIterationIndex;
    this.delayStart = delayStart;
    this.ozoneConfiguration = scm.getConfiguration();
    this.containerBalancer = containerBalancer;
    this.config = config;
    this.scmContext = scm.getScmContext();
  }

  /**
   * Run the container balancer task.
   */
  public void run() {
    try {
      if (delayStart) {
        long delayDuration = ozoneConfiguration.getTimeDuration(
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
            TimeUnit.SECONDS);
        LOG.info("ContainerBalancer will sleep for {} seconds before starting balancing.", delayDuration);
        Thread.sleep(Duration.ofSeconds(delayDuration).toMillis());
      }
      balance();
    } catch (Exception e) {
      LOG.error("Container Balancer is stopped abnormally, ", e);
    } finally {
      synchronized (this) {
        taskStatus = Status.STOPPED;
      }
    }
  }

  /**
   * Changes the status from RUNNING to STOPPING.
   */
  public void stop() {
    synchronized (this) {
      if (taskStatus == Status.RUNNING) {
        taskStatus = Status.STOPPING;
      }
    }
  }

  private void balance() {
    int iterations = config.getIterations();
    if (iterations == -1) {
      //run balancer infinitely
      iterations = Integer.MAX_VALUE;
    }

    // nextIterationIndex is the iteration that balancer should start from on
    // leader change or restart
    for (int i = nextIterationIndex; i < iterations && isBalancerRunning(); ++i) {
      // reset some variables and metrics for this iteration
      if (it != null) {
        it.resetState();
      }
      if (config.getTriggerDuEnable()) {
        // before starting a new iteration, we trigger all the datanode
        // to run `du`. this is an aggressive action, with which we can
        // get more precise usage info of all datanodes before moving.
        // this is helpful for container balancer to make more appropriate
        // decisions. this will increase the disk io load of data nodes, so
        // please enable it with caution.
        nodeManager.refreshAllHealthyDnUsageInfo();
        try {
          long nodeReportInterval =
              ozoneConfiguration.getTimeDuration(HDDS_NODE_REPORT_INTERVAL,
                  HDDS_NODE_REPORT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
          // one for sending command , one for running du, and one for
          // reporting back make it like this for now, a more suitable
          // value. can be set in the future if needed
          long sleepTime = 3 * nodeReportInterval;
          LOG.info("ContainerBalancer will sleep for {} ms while waiting " +
              "for updated usage information from Datanodes.", sleepTime);
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("Container Balancer was interrupted while waiting for" +
              "datanodes refreshing volume usage info");
          Thread.currentThread().interrupt();
          return;
        }
      }

      if (!isBalancerRunning()) {
        return;
      }

      if (scmIsNotReady()) {
        return;
      }
      // sorted list in order from most to least used
      List<DatanodeUsageInfo> datanodeUsageInfos = nodeManager.getMostOrLeastUsedDatanodes(true);
      if (datanodeUsageInfos.isEmpty()) {
        LOG.warn("Received an empty list of datanodes from Node Manager when " +
            "trying to identify which nodes to balance");
        return;
      }
      // include/exclude nodes from balancing according to configs
      datanodeUsageInfos.removeIf(dnUsageInfo -> shouldExcludeDatanode(config, dnUsageInfo.getDatanodeDetails()));

      it = new ContainerBalanceIteration(containerBalancer, config, scm, datanodeUsageInfos);

      // initialize this iteration. stop balancing on initialization failure
      boolean b = it.findOverAndUnderUtilizedNodes(this, datanodeUsageInfos);
      if (!b) {
        // just return if the reason for initialization failure is that
        // balancer has been stopped in another thread
        if (!isBalancerRunning()) {
          return;
        }
        // otherwise, try to stop balancer
        tryStopWithSaveConfiguration("Could not initialize " +
            "ContainerBalancer's iteration number " + i);
        return;
      }

      ContainerBalanceIteration.IterationState currentState = it.doIteration(this, config);
      LOG.info("Result of this iteration of Container Balancer: {}", currentState);

      // if no new move option is generated, it means the cluster cannot be
      // balanced anymore; so just stop balancer
      if (currentState.status == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        tryStopWithSaveConfiguration(currentState.toString());
        return;
      }

      // persist next iteration index
      if (currentState.status == IterationResult.ITERATION_COMPLETED) {
        try {
          saveConfiguration(config, true, i + 1);
        } catch (IOException | TimeoutException e) {
          LOG.warn("Could not persist next iteration index value for " +
              "ContainerBalancer after completing an iteration", e);
        }
      }

      // return if balancing has been stopped
      if (!isBalancerRunning()) {
        return;
      }

      // wait for configured time before starting next iteration, unless
      // this was the final iteration
      if (i != iterations - 1) {
        try {
          Thread.sleep(config.getBalancingInterval().toMillis());
        } catch (InterruptedException e) {
          LOG.info("Container Balancer was interrupted while waiting for" +
              " next iteration.");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    
    tryStopWithSaveConfiguration("Completed all iterations.");
  }

  /**
   * Logs the reason for stop and save configuration and stop the task.
   * 
   * @param stopReason a string specifying the reason for stop
   */
  private void tryStopWithSaveConfiguration(String stopReason) {
    synchronized (this) {
      try {
        LOG.info("Save Configuration for stopping. Reason: {}", stopReason);
        saveConfiguration(config, false, 0);
        stop();
      } catch (IOException | TimeoutException e) {
        LOG.warn("Save configuration failed. Reason for " +
            "stopping: {}", stopReason, e);
      }
    }
  }

  private void saveConfiguration(ContainerBalancerConfiguration configuration,
                                 boolean shouldRun, int index)
      throws IOException, TimeoutException {
    if (scmIsNotReady()) {
      LOG.warn("Save configuration is not allowed as not in valid State.");
      return;
    }
    synchronized (this) {
      if (isBalancerRunning()) {
        containerBalancer.saveConfiguration(configuration, shouldRun, index);
      }
    }
  }

  private boolean scmIsNotReady() {
    if (scmContext.isInSafeMode()) {
      LOG.error("Container Balancer cannot operate while SCM is in Safe Mode.");
      return true;
    }
    if (!scmContext.isLeaderReady()) {
      LOG.warn("Current SCM is not the leader.");
      return true;
    }
    return false;
  }

  /**
   * Consults the configurations {@link ContainerBalancerConfiguration#includeNodes} and
   * {@link ContainerBalancerConfiguration#excludeNodes} to check if the specified
   * Datanode should be excluded from balancing.
   *
   * @param config
   * @param datanode DatanodeDetails to check
   * @return true if Datanode should be excluded, else false
   */
  private static boolean shouldExcludeDatanode(@NotNull ContainerBalancerConfiguration config,
                                               @NotNull DatanodeDetails datanode)
  {
    Set<String> includeNodes = config.getIncludeNodes();
    Set<String> excludeNodes = config.getExcludeNodes();
    if (excludeNodes.contains(datanode.getHostName()) || excludeNodes.contains(datanode.getIpAddress())) {
      return true;
    } else if (!includeNodes.isEmpty()) {
      return !includeNodes.contains(datanode.getHostName()) && !includeNodes.contains(datanode.getIpAddress());
    }
    return false;
  }

  /**
   * Checks if ContainerBalancerTask is currently running.
   *
   * @return true if the status is RUNNING, otherwise false
   */
  boolean isBalancerRunning() {
    return taskStatus == Status.RUNNING;
  }

  /**
   * Gets the list of unBalanced nodes, that is, the over and under utilized
   * nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing unBalanced nodes.
   */
  @VisibleForTesting
  List<DatanodeUsageInfo> getUnBalancedNodes() {
    return it.getUnBalancedNodes();
  }

  /**
   * Gets a map with selected containers and their source datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return it.getContainerToSourceMap();
  }

  /**
   * Gets a map with selected containers and target datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}.
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return it.getContainerToTargetMap();
  }

  @VisibleForTesting
  Set<DatanodeDetails> getSelectedTargets() {
    return it.getSelectedTargets();
  }

  @VisibleForTesting
  int getCountDatanodesInvolvedPerIteration() {
    return it.getDatanodeCountUsedIteration();
  }

  @VisibleForTesting
  public long getSizeScheduledForMoveInLatestIteration() {
    return it.getSizeScheduledForMoveInLatestIteration();
  }

  @VisibleForTesting
  public ContainerBalancerMetrics getMetrics() {
    return it.getMetrics();
  }

  @VisibleForTesting
  IterationResult getIterationResult() {
    return it.getIterationResult();
  }

  public boolean isRunning() {
    return taskStatus == Status.RUNNING;
  }

  public boolean isStopped() {
    return taskStatus == Status.STOPPED;
  }

  public Status getBalancerStatus() {
    return taskStatus;
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer Task status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", isBalancerRunning());
    return status + config.toString();
  }

  static class MoveState {
    final ContainerMoveSelection moveSelection;
    final CompletableFuture<MoveManager.MoveResult> result;

    public MoveState(ContainerMoveSelection moveSelection, CompletableFuture<MoveManager.MoveResult> future) {
      this.moveSelection = moveSelection;
      this.result = future;
    }
  }

  /**
   * The result of {@link ContainerBalancerTask#doIteration(ContainerBalanceIteration)}.
   */
  enum IterationResult {
    ITERATION_COMPLETED,
    ITERATION_INTERRUPTED,
    CAN_NOT_BALANCE_ANY_MORE
  }

  /**
   * The status of {@link ContainerBalancerTask}.
   */
  enum Status {
    RUNNING,
    STOPPING,
    STOPPED
  }
}
