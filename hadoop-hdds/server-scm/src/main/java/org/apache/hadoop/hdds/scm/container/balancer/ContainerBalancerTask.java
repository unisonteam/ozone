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
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  private final ContainerManager containerManager;
  private final ReplicationManager replicationManager;
  private final MoveManager moveManager;
  private final OzoneConfiguration ozoneConfiguration;
  private final ContainerBalancer containerBalancer;
  private final SCMContext scmContext;
  private final StorageContainerManager scm;
  private List<DatanodeUsageInfo> unBalancedNodes;
  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;
  private List<DatanodeUsageInfo> withinThresholdUtilizedNodes;

  private final ContainerBalancerConfiguration config;
  private final ContainerBalancerMetrics metrics;
  private final ContainerBalancerSelectionCriteria selectionCriteria;
  private volatile Status taskStatus = Status.RUNNING;

  /*
  Since a container can be selected only once during an iteration, these maps
   use it as a primary key to track source to target pairings.
  */
  private final Map<ContainerID, DatanodeDetails> containerToSourceMap;
  private final Map<ContainerID, DatanodeDetails> containerToTargetMap;

  private final Set<DatanodeDetails> selectedTargets;
  private final Set<DatanodeDetails> selectedSources;
  private final FindTargetStrategy findTargetStrategy;
  private final FindSourceStrategy findSourceStrategy;
  private Map<ContainerMoveSelection, CompletableFuture<MoveManager.MoveResult>> moveSelectionToFutureMap;
  private final IterationState iterationState;
  private final boolean delayStart;

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
    this.containerManager = scm.getContainerManager();
    this.replicationManager = scm.getReplicationManager();
    this.moveManager = scm.getMoveManager();
    this.moveManager.setMoveTimeout(config.getMoveTimeout().toMillis());
    this.moveManager.setReplicationTimeout(config.getMoveReplicationTimeout().toMillis());
    this.delayStart = delayStart;
    this.ozoneConfiguration = scm.getConfiguration();
    this.containerBalancer = containerBalancer;
    this.config = config;
    this.metrics = containerBalancer.getMetrics();
    this.scmContext = scm.getScmContext();
    this.overUtilizedNodes = new ArrayList<>();
    this.underUtilizedNodes = new ArrayList<>();
    this.withinThresholdUtilizedNodes = new ArrayList<>();
    this.unBalancedNodes = new ArrayList<>();
    this.containerToSourceMap = new HashMap<>();
    this.containerToTargetMap = new HashMap<>();
    this.selectedSources = new HashSet<>();
    this.selectedTargets = new HashSet<>();
    findSourceStrategy = new FindSourceGreedy(nodeManager);
    findTargetStrategy = FindTargetStrategyFactory.create(scm, config.getNetworkTopologyEnable());
    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        nodeManager, replicationManager, containerManager, findSourceStrategy);
    iterationState = new IterationState(nextIterationIndex);
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
    for (int i = iterationState.startIndex; i < iterations && isBalancerRunning(); ++i) {
      // reset some variables and metrics for this iteration
      resetState();
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

      ContainerBalanceIteration it = new ContainerBalanceIteration(config, scm, datanodeUsageInfos);

      // initialize this iteration. stop balancing on initialization failure
      if (!initializeIteration(datanodeUsageInfos, it)) {
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

      IterationState currentState = doIteration(it);
      metrics.incrementNumIterations(1);
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

  /**
   * Initializes an iteration during balancing. Recognizes over, under, and
   * within threshold utilized nodes. Decides whether balancing needs to
   * continue or should be stopped.
   *
   * @return true if successfully initialized, otherwise false.
   */
  private boolean initializeIteration(List<DatanodeUsageInfo> datanodeUsageInfos, ContainerBalanceIteration it) {
    long totalOverUtilizedBytes = 0L, totalUnderUtilizedBytes = 0L;
    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      if (!isBalancerRunning()) {
        return false;
      }
      double utilization = datanodeUsageInfo.calculateUtilization();
      SCMNodeStat scmNodeStat = datanodeUsageInfo.getScmNodeStat();
      Long capacity = scmNodeStat.getCapacity().get();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Utilization for node {} with capacity {}B, used {}B, and " +
                "remaining {}B is {}",
            datanodeUsageInfo.getDatanodeDetails().getUuidString(),
            capacity,
            scmNodeStat.getScmUsed().get(),
            scmNodeStat.getRemaining().get(),
            utilization);
      }
      if (Double.compare(utilization, it.upperLimit) > 0) {
        overUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes greater than upper limit in this node
        long overUtilizedBytes = ratioToBytes(capacity, utilization) - ratioToBytes(capacity, it.upperLimit);
        totalOverUtilizedBytes += overUtilizedBytes;
      } else if (Double.compare(utilization, it.lowerLimit) < 0) {
        underUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes lesser than lower limit in this node
        long underUtilizedBytes = ratioToBytes(capacity, it.lowerLimit) - ratioToBytes(capacity, utilization);
        totalUnderUtilizedBytes += underUtilizedBytes;
      } else {
        withinThresholdUtilizedNodes.add(datanodeUsageInfo);
      }
    }
    metrics.incrementDataSizeUnbalancedGB(
        Math.max(totalOverUtilizedBytes, totalUnderUtilizedBytes) /
            OzoneConsts.GB);
    Collections.reverse(underUtilizedNodes);

    unBalancedNodes = new ArrayList<>(overUtilizedNodes.size() + underUtilizedNodes.size());
    unBalancedNodes.addAll(overUtilizedNodes);
    unBalancedNodes.addAll(underUtilizedNodes);

    if (unBalancedNodes.isEmpty()) {
      LOG.info("Did not find any unbalanced Datanodes.");
      return false;
    }

    LOG.info("Container Balancer has identified {} Over-Utilized and {} " +
            "Under-Utilized Datanodes that need to be balanced.",
        overUtilizedNodes.size(), underUtilizedNodes.size());

    if (LOG.isDebugEnabled()) {
      overUtilizedNodes.forEach(entry -> {
        LOG.debug("Datanode {} {} is Over-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });

      underUtilizedNodes.forEach(entry -> {
        LOG.debug("Datanode {} {} is Under-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });
    }

    return true;
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

  private IterationState doIteration(ContainerBalanceIteration it) {
    // note that potential and selected targets are updated in the following
    // loop
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    List<DatanodeUsageInfo> potentialTargets = getPotentialTargets();
    findTargetStrategy.reInitialize(potentialTargets);
    findSourceStrategy.reInitialize(getPotentialSources(), config, it.lowerLimit);

    moveSelectionToFutureMap = new HashMap<>(unBalancedNodes.size());
    boolean isMoveGeneratedInThisIteration = false;
    boolean canAdaptWhenNearingLimits = true;
    boolean canAdaptOnReachingLimits = true;
    iterationState.reset();

    // match each source node with a target
    while (true) {
      if (!isBalancerRunning()) {
        iterationState.status = IterationResult.ITERATION_INTERRUPTED;
        break;
      }

      // break out if we've reached max size to move limit
      if (reachedMaxSizeToMovePerIteration(it, iterationState.sizeScheduledForMoveInLatestIteration)) {
        break;
      }

      /* if balancer is approaching the iteration limits for max datanodes to
       involve, take some action in adaptWhenNearingIterationLimits()
      */
      if (canAdaptWhenNearingLimits) {
        if (adaptWhenNearingIterationLimits(it, iterationState.datanodeCountUsedIteration)) {
          canAdaptWhenNearingLimits = false;
        }
      }
      if (canAdaptOnReachingLimits) {
        // check if balancer has hit the iteration limits and take some action
        if (adaptOnReachingIterationLimits(it, iterationState.datanodeCountUsedIteration)) {
          canAdaptOnReachingLimits = false;
          canAdaptWhenNearingLimits = false;
        }
      }

      DatanodeDetails source = findSourceStrategy.getNextCandidateSourceDataNode();
      if (source == null) {
        // no more source DNs are present
        break;
      }

      ContainerMoveSelection moveSelection = matchSourceWithTarget(source, it,
          iterationState.sizeScheduledForMoveInLatestIteration);
      if (moveSelection != null) {
        if (processMoveSelection(source, moveSelection, iterationState)) {
          isMoveGeneratedInThisIteration = true;
        }
      } else {
        // can not find any target for this source
        findSourceStrategy.removeCandidateSourceDataNode(source);
      }
    }

    if (!isMoveGeneratedInThisIteration) {
      /*
       If no move was generated during this iteration then we don't need to check the move results
       */
      iterationState.status = IterationResult.CAN_NOT_BALANCE_ANY_MORE;
    } else {
      checkIterationMoveResults();
      iterationState.datanodeCountUsedIteration = selectedSources.size() + selectedTargets.size();
      collectMetrics(iterationState.datanodeCountUsedIteration, iterationState.sizeActuallyMovedInLatestIteration);
    }
    return iterationState;
  }

  private boolean processMoveSelection(DatanodeDetails source,
                                       ContainerMoveSelection moveSelection, IterationState iterationResult) {
    ContainerID containerID = moveSelection.getContainerID();
    if (containerToSourceMap.containsKey(containerID) ||
        containerToTargetMap.containsKey(containerID)) {
      LOG.warn("Container {} has already been selected for move from source " +
              "{} to target {} earlier. Not moving this container again.",
          containerID,
          containerToSourceMap.get(containerID),
          containerToTargetMap.get(containerID));
      return false;
    }

    ContainerInfo containerInfo;
    try {
      containerInfo =
          containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not get container {} from Container Manager before " +
          "starting a container move", containerID, e);
      return false;
    }
    LOG.info("ContainerBalancer is trying to move container {} with size " +
            "{}B from source datanode {} to target datanode {}",
        containerID.toString(),
        containerInfo.getUsedBytes(),
        source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    if (moveContainer(source, moveSelection, iterationResult)) {
      // consider move successful for now, and update selection criteria
      updateTargetsAndSelectionCriteria(moveSelection, source, iterationResult);
    }
    return true;
  }

  /**
   * Checks the results of all move operations when exiting an iteration.
   */
  private void checkIterationMoveResults() {
    CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(
        moveSelectionToFutureMap.values()
            .toArray(new CompletableFuture[moveSelectionToFutureMap.size()]));
    try {
      allFuturesResult.get(config.getMoveTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Container balancer is interrupted");
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      long timeoutCounts = cancelMovesThatExceedTimeoutDuration();
      LOG.warn("{} Container moves are canceled.", timeoutCounts);
      metrics.incrementNumContainerMovesTimeoutInLatestIteration(timeoutCounts);
    } catch (ExecutionException e) {
      LOG.error("Got exception while checkIterationMoveResults", e);
    }
  }

  private void collectMetrics(int datanodeCountUsedIteration, long sizeActuallyMovedInLatestIteration) {
    metrics.incrementNumDatanodesInvolvedInLatestIteration(datanodeCountUsedIteration);
    metrics.incrementNumContainerMovesScheduled(metrics.getNumContainerMovesScheduledInLatestIteration());
    metrics.incrementNumContainerMovesCompleted(metrics.getNumContainerMovesCompletedInLatestIteration());
    metrics.incrementNumContainerMovesTimeout(metrics.getNumContainerMovesTimeoutInLatestIteration());
    metrics.incrementDataSizeMovedGBInLatestIteration(sizeActuallyMovedInLatestIteration / OzoneConsts.GB);
    metrics.incrementDataSizeMovedGB(metrics.getDataSizeMovedGBInLatestIteration());
    metrics.incrementNumContainerMovesFailed(metrics.getNumContainerMovesFailedInLatestIteration());
    LOG.info("Iteration Summary. Number of Datanodes involved: {}. Size moved: {} ({} Bytes)." +
            "Number of Container moves completed: {}.",
        datanodeCountUsedIteration,
        StringUtils.byteDesc(sizeActuallyMovedInLatestIteration),
        sizeActuallyMovedInLatestIteration,
        metrics.getNumContainerMovesCompletedInLatestIteration());
  }

  /**
   * Cancels container moves that are not yet done. Note that if a move
   * command has already been sent out to a Datanode, we don't yet have the
   * capability to cancel it. However, those commands in the DN should time out
   * if they haven't been processed yet.
   *
   * @return number of moves that did not complete (timed out) and were
   * cancelled.
   */
  private long cancelMovesThatExceedTimeoutDuration() {
    Set<Map.Entry<ContainerMoveSelection,
        CompletableFuture<MoveManager.MoveResult>>>
        entries = moveSelectionToFutureMap.entrySet();
    Iterator<Map.Entry<ContainerMoveSelection,
        CompletableFuture<MoveManager.MoveResult>>>
        iterator = entries.iterator();

    int numCancelled = 0;
    // iterate through all moves and cancel ones that aren't done yet
    while (iterator.hasNext()) {
      Map.Entry<ContainerMoveSelection,
          CompletableFuture<MoveManager.MoveResult>>
          entry = iterator.next();
      if (!entry.getValue().isDone()) {
        LOG.warn("Container move timed out for container {} from source {}" +
                " to target {}.", entry.getKey().getContainerID(),
            containerToSourceMap.get(entry.getKey().getContainerID())
                                .getUuidString(),
            entry.getKey().getTargetNode().getUuidString());

        entry.getValue().cancel(true);
        numCancelled += 1;
      }
    }

    return numCancelled;
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(DatanodeDetails source, ContainerBalanceIteration it,
                                                       long sizeScheduledForMoveInLatestIteration)
  {
    NavigableSet<ContainerID> candidateContainers =
        selectionCriteria.getCandidateContainers(source, sizeScheduledForMoveInLatestIteration);

    if (candidateContainers.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerBalancer could not find any candidate containers " +
            "for datanode {}", source.getUuidString());
      }
      return null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerBalancer is finding suitable target for source " +
          "datanode {}", source.getUuidString());
    }
    ContainerMoveSelection moveSelection =
        findTargetStrategy.findTargetForContainerMove(
            source, candidateContainers, config.getMaxSizeEnteringTarget(), it.upperLimit);

    if (moveSelection == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ContainerBalancer could not find a suitable target for " +
            "source node {}.", source.getUuidString());
      }
      return null;
    }
    LOG.info("ContainerBalancer matched source datanode {} with target " +
            "datanode {} for container move.", source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    return moveSelection;
  }

  private static boolean reachedMaxSizeToMovePerIteration(ContainerBalanceIteration it,
                                                          long sizeScheduledForMoveInLatestIteration)
  {
    // since candidate containers in ContainerBalancerSelectionCriteria are
    // filtered out according to this limit, balancer should not have crossed it
    if (sizeScheduledForMoveInLatestIteration >= it.maxSizeToMovePerIteration) {
      LOG.warn("Reached max size to move limit. {} bytes have already been" +
              " scheduled for balancing and the limit is {} bytes.",
          sizeScheduledForMoveInLatestIteration, it.maxSizeToMovePerIteration);
      return true;
    }
    return false;
  }

  /**
   * Restricts potential target datanodes to nodes that have
   * already been selected if balancer is one datanode away from
   * "datanodes.involved.max.percentage.per.iteration" limit.
   * @return true if potential targets were restricted, else false
   */
  private boolean adaptWhenNearingIterationLimits(ContainerBalanceIteration it, int datanodeCountUsedIteration) {
    // check if we're nearing max datanodes to involve
    int maxDatanodesToInvolve = (int) (it.maxDatanodesRatioToInvolvePerIteration * it.totalNodesInCluster);
    if (datanodeCountUsedIteration + 1 == maxDatanodesToInvolve) {
      /* We're one datanode away from reaching the limit. Restrict potential
      targets to targets that have already been selected.
       */
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      LOG.debug("Approaching max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit is " +
              "{}. Only already selected targets can be selected as targets" +
              " now.",
          datanodeCountUsedIteration, maxDatanodesToInvolve);
      return true;
    }

    // return false if we didn't adapt
    return false;
  }

  /**
   * Restricts potential source and target datanodes to nodes that have
   * already been selected if balancer has reached
   * "datanodes.involved.max.percentage.per.iteration" limit.
   * @return true if potential sources and targets were restricted, else false
   */
  private boolean adaptOnReachingIterationLimits(ContainerBalanceIteration it, int datanodeCountUsedIteration) {
    // check if we've reached max datanodes to involve limit
    int maxDatanodesToInvolve = (int) (it.maxDatanodesRatioToInvolvePerIteration * it.totalNodesInCluster);
    if (datanodeCountUsedIteration == maxDatanodesToInvolve) {
      // restrict both to already selected sources and targets
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      findSourceStrategy.resetPotentialSources(selectedSources);
      LOG.debug("Reached max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit " +
              "is {}. Only already selected sources and targets can be " +
              "involved in balancing now.",
          datanodeCountUsedIteration, maxDatanodesToInvolve);
      return true;
    }

    // return false if we didn't adapt
    return false;
  }

  /**
   * Asks {@link ReplicationManager} or {@link MoveManager} to move the
   * specified container from source to target.
   *
   * @param source          the source datanode
   * @param moveSelection   the selected container to move and target datanode
   * @param iterationResult
   * @return false if an exception occurred or the move completed with a
   * result other than MoveManager.MoveResult.COMPLETED. Returns true
   * if the move completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(DatanodeDetails source, ContainerMoveSelection moveSelection, IterationState iterationResult) {
    ContainerID containerID = moveSelection.getContainerID();
    CompletableFuture<MoveManager.MoveResult> future;
    try {
      ContainerInfo containerInfo = containerManager.getContainer(containerID);

      /*
      If LegacyReplicationManager is enabled, ReplicationManager will
      redirect to it. Otherwise, use MoveManager.
       */
      if (replicationManager.getConfig().isLegacyEnabled()) {
        future = replicationManager
            .move(containerID, source, moveSelection.getTargetNode());
      } else {
        future = moveManager.move(containerID, source,
            moveSelection.getTargetNode());
      }
      metrics.incrementNumContainerMovesScheduledInLatestIteration(1);

      future = future.whenComplete((result, ex) -> {
        metrics.incrementCurrentIterationContainerMoveMetric(result, 1);
        if (ex != null) {
          LOG.info("Container move for container {} from source {} to " +
                  "target {} failed with exceptions.",
              containerID.toString(),
              source.getUuidString(),
              moveSelection.getTargetNode().getUuidString(), ex);
          metrics.incrementNumContainerMovesFailedInLatestIteration(1);
        } else {
          if (result == MoveManager.MoveResult.COMPLETED) {
            iterationResult.sizeActuallyMovedInLatestIteration += containerInfo.getUsedBytes();
            LOG.debug("Container move completed for container {} from " +
                    "source {} to target {}", containerID,
                source.getUuidString(),
                moveSelection.getTargetNode().getUuidString());
          } else {
            LOG.warn(
                "Container move for container {} from source {} to target" +
                    " {} failed: {}",
                moveSelection.getContainerID(), source.getUuidString(),
                moveSelection.getTargetNode().getUuidString(), result);
          }
        }
      });
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} for container move",
          containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    } catch (NodeNotFoundException | TimeoutException |
             ContainerReplicaNotFoundException e) {
      LOG.warn("Container move failed for container {}", containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    }

    /*
    If the future hasn't failed yet, put it in moveSelectionToFutureMap for
    processing later
     */
    if (future.isDone()) {
      if (future.isCompletedExceptionally()) {
        return false;
      } else {
        MoveManager.MoveResult result = future.join();
        moveSelectionToFutureMap.put(moveSelection, future);
        return result == MoveManager.MoveResult.COMPLETED;
      }
    } else {
      moveSelectionToFutureMap.put(moveSelection, future);
      return true;
    }
  }

  /**
   * Update targets, sources, and selection criteria after a move.
   *
   * @param moveSelection   latest selected target datanode and container
   * @param source          the source datanode
   * @param iterationResult
   */
  private void updateTargetsAndSelectionCriteria(
      ContainerMoveSelection moveSelection, DatanodeDetails source, IterationState iterationResult) {
    ContainerID containerID = moveSelection.getContainerID();
    DatanodeDetails target = moveSelection.getTargetNode();

    // count source if it has not been involved in move earlier
    if (!selectedSources.contains(source)) {
      iterationResult.datanodeCountUsedIteration += 1;
    }
    // count target if it has not been involved in move earlier
    if (!selectedTargets.contains(target)) {
      iterationResult.datanodeCountUsedIteration += 1;
    }

    incSizeSelectedForMoving(source, moveSelection, iterationResult);
    containerToSourceMap.put(containerID, source);
    containerToTargetMap.put(containerID, target);
    selectedTargets.add(target);
    selectedSources.add(source);
    selectionCriteria.setSelectedContainers(
        new HashSet<>(containerToSourceMap.keySet()));
  }

  /**
   * Calculates the number of used bytes given capacity and utilization ratio.
   *
   * @param nodeCapacity     capacity of the node.
   * @param utilizationRatio used space by capacity ratio of the node.
   * @return number of bytes
   */
  private long ratioToBytes(Long nodeCapacity, double utilizationRatio) {
    return (long) (nodeCapacity * utilizationRatio);
  }

  /**
   * Get potential targets for container move. Potential targets are under
   * utilized and within threshold utilized nodes.
   *
   * @return A list of potential target DatanodeUsageInfo.
   */
  private List<DatanodeUsageInfo> getPotentialTargets() {
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    return underUtilizedNodes;
  }

  /**
   * Get potential sourecs for container move. Potential sourecs are over
   * utilized and within threshold utilized nodes.
   *
   * @return A list of potential source DatanodeUsageInfo.
   */
  private List<DatanodeUsageInfo> getPotentialSources() {
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    return overUtilizedNodes;
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
   * Updates conditions for balancing, such as total size moved by balancer,
   * total size that has entered a datanode, and total size that has left a
   * datanode in this iteration.
   *
   * @param source          source datanode
   * @param moveSelection   selected target datanode and container
   * @param iterationResult
   */
  private void incSizeSelectedForMoving(DatanodeDetails source, ContainerMoveSelection moveSelection, IterationState iterationResult) {
    DatanodeDetails target = moveSelection.getTargetNode();
    ContainerInfo container;
    try {
      container =
          containerManager.getContainer(moveSelection.getContainerID());
    } catch (ContainerNotFoundException e) {
      LOG.warn("Could not find Container {} while matching source and " +
              "target nodes in ContainerBalancer",
          moveSelection.getContainerID(), e);
      return;
    }
    long size = container.getUsedBytes();
    iterationResult.sizeScheduledForMoveInLatestIteration += size;

    // update sizeLeavingNode map with the recent moveSelection
    findSourceStrategy.increaseSizeLeaving(source, size);

    // update sizeEnteringNode map with the recent moveSelection
    findTargetStrategy.increaseSizeEntering(target, size, config.getMaxSizeEnteringTarget());
  }

  /**
   * Resets some variables and metrics for this iteration.
   */
  private void resetState() {
    moveManager.resetState();
    this.overUtilizedNodes.clear();
    this.underUtilizedNodes.clear();
    this.unBalancedNodes.clear();
    this.containerToSourceMap.clear();
    this.containerToTargetMap.clear();
    this.selectedSources.clear();
    this.selectedTargets.clear();
    metrics.resetDataSizeMovedGBInLatestIteration();
    metrics.resetNumContainerMovesCompletedInLatestIteration();
    metrics.resetNumContainerMovesTimeoutInLatestIteration();
    metrics.resetNumDatanodesInvolvedInLatestIteration();
    metrics.resetDataSizeUnbalancedGB();
    metrics.resetNumDatanodesUnbalanced();
    metrics.resetNumContainerMovesFailedInLatestIteration();
  }

  /**
   * Checks if ContainerBalancerTask is currently running.
   *
   * @return true if the status is RUNNING, otherwise false
   */
  private boolean isBalancerRunning() {
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
    return unBalancedNodes;
  }

  /**
   * Gets a map with selected containers and their source datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return containerToSourceMap;
  }

  /**
   * Gets a map with selected containers and target datanodes.
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}.
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return containerToTargetMap;
  }

  @VisibleForTesting
  Set<DatanodeDetails> getSelectedTargets() {
    return selectedTargets;
  }

  @VisibleForTesting
  int getCountDatanodesInvolvedPerIteration() {
    return iterationState.datanodeCountUsedIteration;
  }

  @VisibleForTesting
  public long getSizeScheduledForMoveInLatestIteration() {
    return iterationState.sizeScheduledForMoveInLatestIteration;
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  IterationResult getIterationResult() {
    return iterationState.status;
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

  private static class IterationState {
    private final int startIndex;
    private IterationResult status = IterationResult.ITERATION_COMPLETED;
    private int datanodeCountUsedIteration = 0;
    private long sizeScheduledForMoveInLatestIteration = 0;
    private long sizeActuallyMovedInLatestIteration = 0;

    public IterationState(int startIndex) {
      this.startIndex = startIndex;
    }

    public void reset() {
      status = IterationResult.ITERATION_COMPLETED;
      datanodeCountUsedIteration = 0;
      sizeScheduledForMoveInLatestIteration = 0;
      sizeActuallyMovedInLatestIteration = 0;
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
