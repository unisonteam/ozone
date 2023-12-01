package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ContainerBalanceIteration {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBalanceIteration.class);

  public final int maxDatanodeCountToUseInIteration;
  public final long maxSizeToMovePerIteration;
  public final int totalNodesInCluster;
  public final double upperLimit;
  public final double lowerLimit;

  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;

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
}