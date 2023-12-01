package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ContainerBalanceIteration {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBalanceIteration.class);

  public final int maxDatanodeCountToUseInIteration;
  public final long maxSizeToMovePerIteration;
  public final int totalNodesInCluster;
  public final double upperLimit;
  public final double lowerLimit;

  private final List<DatanodeUsageInfo> unBalancedNodes;
  private final List<DatanodeUsageInfo> overUtilizedNodes;
  private final List<DatanodeUsageInfo> underUtilizedNodes;
  private final List<DatanodeUsageInfo> withinThresholdUtilizedNodes;

  private final MoveManager moveManager;

  private final FindTargetStrategy findTargetStrategy;
  private final FindSourceStrategy findSourceStrategy;

  private final IterationState currentState;
  private final ContainerBalancerSelectionCriteria selectionCriteria;

  /*
    Since a container can be selected only once during an iteration, these maps
    use it as a primary key to track source to target pairings.
  */
  private final Map<ContainerID, DatanodeDetails> containerToSourceMap;
  private final Map<ContainerID, DatanodeDetails> containerToTargetMap;

  private final Set<DatanodeDetails> selectedTargets;
  private final Set<DatanodeDetails> selectedSources;
  private final ArrayList<ContainerBalancerTask.MoveState> moveStateList;
  private final StorageContainerManager scm;
  private final ContainerBalancerMetrics metrics;

  ContainerBalanceIteration(ContainerBalancer balancer, ContainerBalancerConfiguration config,
                            StorageContainerManager scm,
                            List<DatanodeUsageInfo> datanodeUsageInfos)
  {
    this.scm = scm;
    metrics = balancer.getMetrics();

    findSourceStrategy = new FindSourceGreedy(scm.getScmNodeManager());
    findTargetStrategy = FindTargetStrategyFactory.create(scm, config.getNetworkTopologyEnable());
    double threshold = config.getThresholdAsRatio();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    this.totalNodesInCluster = datanodeUsageInfos.size();
    this.maxDatanodeCountToUseInIteration = (int) (config.getMaxDatanodesRatioToInvolvePerIteration() * totalNodesInCluster);

    moveManager = scm.getMoveManager();
    moveManager.setMoveTimeout(config.getMoveTimeout().toMillis());
    moveManager.setReplicationTimeout(config.getMoveReplicationTimeout().toMillis());

    double clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
    LOGGER.debug("Average utilization of the cluster is {}", clusterAvgUtilisation);


    // over utilized nodes have utilization(that is, used / capacity) greater than upper limit
    upperLimit = clusterAvgUtilisation + threshold;
    // under utilized nodes have utilization(that is, used / capacity) less than lower limit
    lowerLimit = clusterAvgUtilisation - threshold;

    LOGGER.debug("Lower limit for utilization is {} and Upper limit for utilization is {}", lowerLimit, upperLimit);

    overUtilizedNodes = new ArrayList<>();
    underUtilizedNodes = new ArrayList<>();
    withinThresholdUtilizedNodes = new ArrayList<>();
    unBalancedNodes = new ArrayList<>();
    currentState = new IterationState();
    containerToSourceMap = new HashMap<>();
    containerToTargetMap = new HashMap<>();
    selectedSources = new HashSet<>();
    selectedTargets = new HashSet<>();
    selectionCriteria = new ContainerBalancerSelectionCriteria(config,
        scm.getScmNodeManager(), scm.getReplicationManager(), scm.getContainerManager(), findSourceStrategy);
    moveStateList = new ArrayList<>();
  }

  /**
   * Resets some variables and metrics for this iteration.
   */
  void resetState() {
    moveManager.resetState();
    metrics.resetDataSizeMovedGBInLatestIteration();
    metrics.resetNumContainerMovesCompletedInLatestIteration();
    metrics.resetNumContainerMovesTimeoutInLatestIteration();
    metrics.resetNumDatanodesInvolvedInLatestIteration();
    metrics.resetDataSizeUnbalancedGB();
    metrics.resetNumDatanodesUnbalanced();
    metrics.resetNumContainerMovesFailedInLatestIteration();
  }

  public boolean findOverAndUnderUtilizedNodes(ContainerBalancerTask task, List<DatanodeUsageInfo> datanodeUsageInfos) {
    long totalOverUtilizedBytes = 0L, totalUnderUtilizedBytes = 0L;
    // find over and under utilized nodes
    for (DatanodeUsageInfo datanodeUsageInfo : datanodeUsageInfos) {
      if (!task.isBalancerRunning()) {
        return false;
      }
      double utilization = datanodeUsageInfo.calculateUtilization();
      SCMNodeStat scmNodeStat = datanodeUsageInfo.getScmNodeStat();
      Long capacity = scmNodeStat.getCapacity().get();

      LOGGER.debug("Utilization for node {} with capacity {}B, used {}B, and " +
              "remaining {}B is {}",
          datanodeUsageInfo.getDatanodeDetails().getUuidString(),
          capacity,
          scmNodeStat.getScmUsed().get(),
          scmNodeStat.getRemaining().get(),
          utilization);
      if (Double.compare(utilization, upperLimit) > 0) {
        overUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes greater than upper limit in this node
        long overUtilizedBytes = ratioToBytes(capacity, utilization) - ratioToBytes(capacity, upperLimit);
        totalOverUtilizedBytes += overUtilizedBytes;
      } else if (Double.compare(utilization, lowerLimit) < 0) {
        underUtilizedNodes.add(datanodeUsageInfo);
        metrics.incrementNumDatanodesUnbalanced(1);

        // amount of bytes lesser than lower limit in this node
        long underUtilizedBytes = ratioToBytes(capacity, lowerLimit) - ratioToBytes(capacity, utilization);
        totalUnderUtilizedBytes += underUtilizedBytes;
      } else {
        withinThresholdUtilizedNodes.add(datanodeUsageInfo);
      }
    }
    metrics.incrementDataSizeUnbalancedGB(Math.max(totalOverUtilizedBytes, totalUnderUtilizedBytes) / OzoneConsts.GB);
    Collections.reverse(underUtilizedNodes);

    unBalancedNodes.addAll(overUtilizedNodes);
    unBalancedNodes.addAll(underUtilizedNodes);

    if (unBalancedNodes.isEmpty()) {
      LOGGER.info("Did not find any unbalanced Datanodes.");
      return false;
    }

    LOGGER.info("Container Balancer has identified {} Over-Utilized and {} " +
            "Under-Utilized Datanodes that need to be balanced.",
        overUtilizedNodes.size(), underUtilizedNodes.size());

    if (LOGGER.isDebugEnabled()) {
      overUtilizedNodes.forEach(entry -> {
        LOGGER.debug("Datanode {} {} is Over-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });

      underUtilizedNodes.forEach(entry -> {
        LOGGER.debug("Datanode {} {} is Under-Utilized.",
            entry.getDatanodeDetails().getHostName(),
            entry.getDatanodeDetails().getUuid());
      });
    }

    return true;
  }

  public IterationState doIteration(ContainerBalancerTask task, ContainerBalancerConfiguration config) {
    // note that potential and selected targets are updated in the following
    // loop
    //TODO(jacksonyao): take withinThresholdUtilizedNodes as candidate for both
    // source and target
    List<DatanodeUsageInfo> potentialTargets = getPotentialTargets();
    findTargetStrategy.reInitialize(potentialTargets);
    findSourceStrategy.reInitialize(getPotentialSources(), config, lowerLimit);

    boolean isMoveGeneratedInThisIteration = false;
    boolean canAdaptWhenNearingLimits = true;
    boolean canAdaptOnReachingLimits = true;
    currentState.reset();

    // match each source node with a target
    while (true) {
      if (!task.isBalancerRunning()) {
        currentState.status = ContainerBalancerTask.IterationResult.ITERATION_INTERRUPTED;
        break;
      }

      // break out if we've reached max size to move limit
      if (reachedMaxSizeToMovePerIteration()) {
        break;
      }

      /* if balancer is approaching the iteration limits for max datanodes to
       involve, take some action in adaptWhenNearingIterationLimits()
      */
      if (canAdaptWhenNearingLimits) {
        if (adaptWhenNearingIterationLimits()) {
          canAdaptWhenNearingLimits = false;
        }
      }
      if (canAdaptOnReachingLimits) {
        // check if balancer has hit the iteration limits and take some action
        if (adaptOnReachingIterationLimits()) {
          canAdaptOnReachingLimits = false;
          canAdaptWhenNearingLimits = false;
        }
      }

      DatanodeDetails source = findSourceStrategy.getNextCandidateSourceDataNode();
      if (source == null) {
        // no more source DNs are present
        break;
      }

      ContainerMoveSelection moveSelection = matchSourceWithTarget(config, source
      );
      if (moveSelection != null) {
        if (processMoveSelection(config, source, moveSelection)) {
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
      currentState.status = ContainerBalancerTask.IterationResult.CAN_NOT_BALANCE_ANY_MORE;
    } else {
      checkIterationMoveResults(config);
      currentState.datanodeCountUsedIteration = selectedSources.size() + selectedTargets.size();
      collectMetrics();
    }
    metrics.incrementNumIterations(1);
    return currentState;
  }

  /**
   * Match a source datanode with a target datanode and identify the container
   * to move.
   *
   * @return ContainerMoveSelection containing the selected target and container
   */
  private ContainerMoveSelection matchSourceWithTarget(ContainerBalancerConfiguration config, DatanodeDetails source) {
    NavigableSet<ContainerID> candidateContainers =
        selectionCriteria.getCandidateContainers(source, currentState.sizeScheduledForMoveInLatestIteration);

    if (candidateContainers.isEmpty()) {
      LOGGER.debug("ContainerBalancer could not find any candidate containers for datanode {}", source.getUuidString());
      return null;
    }

    LOGGER.debug("ContainerBalancer is finding suitable target for source datanode {}", source.getUuidString());
    ContainerMoveSelection moveSelection =
        findTargetStrategy.findTargetForContainerMove(
            source, candidateContainers, config.getMaxSizeEnteringTarget(), upperLimit);

    if (moveSelection == null) {
      LOGGER.debug("ContainerBalancer could not find a suitable target for source node {}.", source.getUuidString());
      return null;
    }
    LOGGER.info("ContainerBalancer matched source datanode {} with target " +
            "datanode {} for container move.", source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    return moveSelection;
  }

  private boolean reachedMaxSizeToMovePerIteration() {
    // since candidate containers in ContainerBalancerSelectionCriteria are
    // filtered out according to this limit, balancer should not have crossed it
    if (currentState.sizeScheduledForMoveInLatestIteration >= maxSizeToMovePerIteration) {
      LOGGER.warn("Reached max size to move limit. {} bytes have already been" +
              " scheduled for balancing and the limit is {} bytes.",
          currentState.sizeScheduledForMoveInLatestIteration, maxSizeToMovePerIteration);
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
  private boolean adaptWhenNearingIterationLimits() {
    // check if we're nearing max datanodes to involve
    if (currentState.datanodeCountUsedIteration + 1 == maxDatanodeCountToUseInIteration) {
      /* We're one datanode away from reaching the limit. Restrict potential
      targets to targets that have already been selected.
       */
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      LOGGER.debug("Approaching max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit is " +
              "{}. Only already selected targets can be selected as targets" +
              " now.",
          currentState.datanodeCountUsedIteration, maxDatanodeCountToUseInIteration);
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
  private boolean adaptOnReachingIterationLimits() {
    // check if we've reached max datanodes to involve limit
    if (currentState.datanodeCountUsedIteration == maxDatanodeCountToUseInIteration) {
      // restrict both to already selected sources and targets
      findTargetStrategy.resetPotentialTargets(selectedTargets);
      findSourceStrategy.resetPotentialSources(selectedSources);
      LOGGER.debug("Reached max datanodes to involve limit. {} datanodes " +
              "have already been selected for balancing and the limit " +
              "is {}. Only already selected sources and targets can be " +
              "involved in balancing now.",
          currentState.datanodeCountUsedIteration, maxDatanodeCountToUseInIteration);
      return true;
    }

    // return false if we didn't adapt
    return false;
  }


  private boolean processMoveSelection(ContainerBalancerConfiguration config, DatanodeDetails source,
                                       ContainerMoveSelection moveSelection)
  {
    ContainerID containerID = moveSelection.getContainerID();
    if (containerToSourceMap.containsKey(containerID) ||
        containerToTargetMap.containsKey(containerID)) {
      LOGGER.warn("Container {} has already been selected for move from source " +
              "{} to target {} earlier. Not moving this container again.",
          containerID,
          containerToSourceMap.get(containerID),
          containerToTargetMap.get(containerID));
      return false;
    }

    ContainerInfo containerInfo;
    try {
      containerInfo = scm.getContainerManager().getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not get container {} from Container Manager before " +
          "starting a container move", containerID, e);
      return false;
    }
    LOGGER.info("ContainerBalancer is trying to move container {} with size " +
            "{}B from source datanode {} to target datanode {}",
        containerID.toString(),
        containerInfo.getUsedBytes(),
        source.getUuidString(),
        moveSelection.getTargetNode().getUuidString());

    if (moveContainer(source, moveSelection)) {
      // consider move successful for now, and update selection criteria
      updateTargetsAndSelectionCriteria(config, moveSelection, source);
    }
    return true;
  }

  /**
   * Asks {@link ReplicationManager} or {@link MoveManager} to move the
   * specified container from source to target.
   *
   * @param source        the source datanode
   * @param moveSelection the selected container to move and target datanode
   * @return false if an exception occurred or the move completed with a
   * result other than MoveManager.MoveResult.COMPLETED. Returns true
   * if the move completed with MoveResult.COMPLETED or move is not yet done
   */
  private boolean moveContainer(DatanodeDetails source, ContainerMoveSelection moveSelection) {
    ContainerID containerID = moveSelection.getContainerID();
    CompletableFuture<MoveManager.MoveResult> future;
    try {
      ContainerInfo containerInfo = scm.getContainerManager().getContainer(containerID);

      /*
      If LegacyReplicationManager is enabled, ReplicationManager will redirect to it. Otherwise, use MoveManager.
       */
      ReplicationManager replicationManager = scm.getReplicationManager();
      if (replicationManager.getConfig().isLegacyEnabled()) {
        future = replicationManager.move(containerID, source, moveSelection.getTargetNode());
      } else {
        future = scm.getMoveManager().move(containerID, source, moveSelection.getTargetNode());
      }
      metrics.incrementNumContainerMovesScheduledInLatestIteration(1);

      future = future.whenComplete((result, ex) -> {
        metrics.incrementCurrentIterationContainerMoveMetric(result, 1);
        if (ex != null) {
          LOGGER.info("Container move for container {} from source {} to " +
                  "target {} failed with exceptions.",
              containerID.toString(),
              source.getUuidString(),
              moveSelection.getTargetNode().getUuidString(), ex);
          metrics.incrementNumContainerMovesFailedInLatestIteration(1);
        } else {
          if (result == MoveManager.MoveResult.COMPLETED) {
            currentState.sizeActuallyMovedInLatestIteration += containerInfo.getUsedBytes();
            LOGGER.debug("Container move completed for container {} from source {} to target {}",
                containerID,
                source.getUuidString(),
                moveSelection.getTargetNode().getUuidString());
          } else {
            LOGGER.warn(
                "Container move for container {} from source {} to target {} failed: {}",
                moveSelection.getContainerID(), source.getUuidString(),
                moveSelection.getTargetNode().getUuidString(), result);
          }
        }
      });
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not find Container {} for container move",
          containerID, e);
      metrics.incrementNumContainerMovesFailedInLatestIteration(1);
      return false;
    } catch (NodeNotFoundException | TimeoutException |
             ContainerReplicaNotFoundException e) {
      LOGGER.warn("Container move failed for container {}", containerID, e);
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
        moveStateList.add(new ContainerBalancerTask.MoveState(moveSelection, future));
        return result == MoveManager.MoveResult.COMPLETED;
      }
    } else {
      moveStateList.add(new ContainerBalancerTask.MoveState(moveSelection, future));
      return true;
    }
  }

  /**
   * Update targets, sources, and selection criteria after a move.
   *
   * @param config
   * @param moveSelection latest selected target datanode and container
   * @param source        the source datanode
   */
  private void updateTargetsAndSelectionCriteria(ContainerBalancerConfiguration config,
                                                 ContainerMoveSelection moveSelection, DatanodeDetails source)
  {
    ContainerID containerID = moveSelection.getContainerID();
    DatanodeDetails target = moveSelection.getTargetNode();

    // count source if it has not been involved in move earlier
    if (!selectedSources.contains(source)) {
      currentState.datanodeCountUsedIteration += 1;
    }
    // count target if it has not been involved in move earlier
    if (!selectedTargets.contains(target)) {
      currentState.datanodeCountUsedIteration += 1;
    }

    incSizeSelectedForMoving(config, source, moveSelection);
    containerToSourceMap.put(containerID, source);
    containerToTargetMap.put(containerID, target);
    selectedTargets.add(target);
    selectedSources.add(source);
    selectionCriteria.setSelectedContainers(new HashSet<>(containerToSourceMap.keySet()));
  }

  /**
   * Updates conditions for balancing, such as total size moved by balancer,
   * total size that has entered a datanode, and total size that has left a
   * datanode in this iteration.
   *
   * @param config
   * @param source        source datanode
   * @param moveSelection selected target datanode and container
   */
  private void incSizeSelectedForMoving(ContainerBalancerConfiguration config, DatanodeDetails source,
                                        ContainerMoveSelection moveSelection)
  {
    DatanodeDetails target = moveSelection.getTargetNode();
    ContainerInfo container;
    try {
      container = scm.getContainerManager().getContainer(moveSelection.getContainerID());
    } catch (ContainerNotFoundException e) {
      LOGGER.warn("Could not find Container {} while matching source and target nodes in ContainerBalancer",
          moveSelection.getContainerID(), e);
      return;
    }
    long size = container.getUsedBytes();
    currentState.sizeScheduledForMoveInLatestIteration += size;

    // update sizeLeavingNode map with the recent moveSelection
    findSourceStrategy.increaseSizeLeaving(source, size);

    // update sizeEnteringNode map with the recent moveSelection
    findTargetStrategy.increaseSizeEntering(target, size, config.getMaxSizeEnteringTarget());
  }


  /**
   * Checks the results of all move operations when exiting an iteration.
   */
  @SuppressWarnings("rawtypes")
  private void checkIterationMoveResults(ContainerBalancerConfiguration config) {
    CompletableFuture[] futureArray = new CompletableFuture[moveStateList.size()];
    for (int i = 0; i < moveStateList.size(); ++i) {
      futureArray[i] = moveStateList.get(i).result;
    }
    CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(futureArray);
    try {
      allFuturesResult.get(config.getMoveTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("Container balancer is interrupted");
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      long timeoutCounts = cancelMovesThatExceedTimeoutDuration();
      LOGGER.warn("{} Container moves are canceled.", timeoutCounts);
      metrics.incrementNumContainerMovesTimeoutInLatestIteration(timeoutCounts);
    } catch (ExecutionException e) {
      LOGGER.error("Got exception while checkIterationMoveResults", e);
    }
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
    int numCancelled = 0;
    // iterate through all moves and cancel ones that aren't done yet
    for (ContainerBalancerTask.MoveState state : moveStateList) {
      CompletableFuture<MoveManager.MoveResult> future = state.result;
      if (!future.isDone()) {
        ContainerMoveSelection moveSelection = state.moveSelection;
        LOGGER.warn("Container move timed out for container {} from source {} to target {}.",
            moveSelection.getContainerID(),
            containerToSourceMap.get(moveSelection.getContainerID()).getUuidString(),
            moveSelection.getTargetNode().getUuidString()
        );

        future.cancel(true);
        numCancelled += 1;
      }
    }

    return numCancelled;
  }

  private void collectMetrics() {
    metrics.incrementNumDatanodesInvolvedInLatestIteration(currentState.datanodeCountUsedIteration);
    metrics.incrementNumContainerMovesScheduled(metrics.getNumContainerMovesScheduledInLatestIteration());
    metrics.incrementNumContainerMovesCompleted(metrics.getNumContainerMovesCompletedInLatestIteration());
    metrics.incrementNumContainerMovesTimeout(metrics.getNumContainerMovesTimeoutInLatestIteration());
    metrics.incrementDataSizeMovedGBInLatestIteration(currentState.sizeActuallyMovedInLatestIteration / OzoneConsts.GB);
    metrics.incrementDataSizeMovedGB(metrics.getDataSizeMovedGBInLatestIteration());
    metrics.incrementNumContainerMovesFailed(metrics.getNumContainerMovesFailedInLatestIteration());
    LOGGER.info("Iteration Summary. Number of Datanodes involved: {}. Size moved: {} ({} Bytes)." +
            "Number of Container moves completed: {}.",
        currentState.datanodeCountUsedIteration,
        StringUtils.byteDesc(currentState.sizeActuallyMovedInLatestIteration),
        currentState.sizeActuallyMovedInLatestIteration,
        metrics.getNumContainerMovesCompletedInLatestIteration());
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
    return getUnderUtilizedNodes();
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
    return getOverUtilizedNodes();
  }

  /**
   * Calculates the average utilization for the specified nodes.
   * Utilization is (capacity - remaining) divided by capacity.
   *
   * @param nodes List of DatanodeUsageInfo to find the average utilization for
   * @return Average utilization value
   */
  @VisibleForTesting
  public static double calculateAvgUtilization(List<DatanodeUsageInfo> nodes) {
    if (nodes.isEmpty()) {
      LOGGER.warn("No nodes to calculate average utilization for in ContainerBalancer.");
      return 0;
    }
    SCMNodeStat aggregatedStats = new SCMNodeStat(0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    long clusterCapacity = aggregatedStats.getCapacity().get();
    long clusterRemaining = aggregatedStats.getRemaining().get();

    return (clusterCapacity - clusterRemaining) / (double) clusterCapacity;
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

  public List<DatanodeUsageInfo> getOverUtilizedNodes() {
    return overUtilizedNodes;
  }

  public List<DatanodeUsageInfo> getUnderUtilizedNodes() {
    return underUtilizedNodes;
  }

  public List<DatanodeUsageInfo> getUnBalancedNodes() {
    return unBalancedNodes;
  }

  public ContainerBalancerTask.IterationResult getIterationResult() {
    return currentState.status;
  }

  public long getSizeScheduledForMoveInLatestIteration() {
    return currentState.sizeScheduledForMoveInLatestIteration;
  }

  public int getDatanodeCountUsedIteration() {
    return currentState.datanodeCountUsedIteration;
  }

  public Set<DatanodeDetails> getSelectedTargets() {
    return selectedTargets;
  }

  public Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return containerToTargetMap;
  }

  public Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return containerToSourceMap;
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  static class IterationState {
    public ContainerBalancerTask.IterationResult status = ContainerBalancerTask.IterationResult.ITERATION_COMPLETED;
    public int datanodeCountUsedIteration = 0;
    public long sizeScheduledForMoveInLatestIteration = 0;
    public long sizeActuallyMovedInLatestIteration = 0;

    public void reset() {
      status = ContainerBalancerTask.IterationResult.ITERATION_COMPLETED;
      datanodeCountUsedIteration = 0;
      sizeScheduledForMoveInLatestIteration = 0;
      sizeActuallyMovedInLatestIteration = 0;
    }
  }
}