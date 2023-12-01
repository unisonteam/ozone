package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  private final FindTargetStrategy findTargetStrategy;
  private final FindSourceStrategy findSourceStrategy;

  ContainerBalanceIteration(ContainerBalancerConfiguration config, StorageContainerManager scm,
                            List<DatanodeUsageInfo> datanodeUsageInfos) {
    findSourceStrategy = new FindSourceGreedy(scm.getScmNodeManager());
    findTargetStrategy = FindTargetStrategyFactory.create(scm, config.getNetworkTopologyEnable());
    double threshold = config.getThresholdAsRatio();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();
    this.totalNodesInCluster = datanodeUsageInfos.size();
    this.maxDatanodeCountToUseInIteration = calculateMaxDatanodeCountToUseInIteration(config, totalNodesInCluster);

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
  }

  private static int calculateMaxDatanodeCountToUseInIteration(ContainerBalancerConfiguration config,
                                                               int totalNodesInCluster)
  {
    return (int) (config.getMaxDatanodesRatioToInvolvePerIteration() * totalNodesInCluster);
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

  public boolean findOverAndUnderUtilizedNodes(ContainerBalancerTask task,
                                               ContainerBalancerMetrics metrics,
                                               List<DatanodeUsageInfo> datanodeUsageInfos) {
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
}