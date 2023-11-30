package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;

import java.util.List;
import java.util.Set;

public class ContainerBalanceIteration {

  public final double maxDatanodesRatioToInvolvePerIteration;
  public final long maxSizeToMovePerIteration;
  public final int totalNodesInCluster;
//  public final double upperLimit;
//  public final double lowerLimit;

  private List<DatanodeUsageInfo> overUtilizedNodes;
  private List<DatanodeUsageInfo> underUtilizedNodes;

  private final FindTargetStrategy findTargetStrategy;
  private final FindSourceStrategy findSourceStrategy;

  ContainerBalanceIteration(ContainerBalancerConfiguration config, FindSourceStrategy findSourceStrategy,
                                 FindTargetStrategy findTargetStrategy, List<DatanodeUsageInfo> datanodeUsageInfos)
  {
    this.findSourceStrategy = findSourceStrategy;
    this.findTargetStrategy = findTargetStrategy;
    double threshold = config.getThresholdAsRatio();
    this.maxDatanodesRatioToInvolvePerIteration = config.getMaxDatanodesRatioToInvolvePerIteration();
    this.maxSizeToMovePerIteration = config.getMaxSizeToMovePerIteration();

    this.totalNodesInCluster = datanodeUsageInfos.size();
//
//    double clusterAvgUtilisation = calculateAvgUtilization(datanodeUsageInfos);
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Average utilization of the cluster is {}",
//          clusterAvgUtilisation);
//    }
//
//    // over utilized nodes have utilization(that is, used / capacity) greater
//    // than upper limit
//    this.upperLimit = clusterAvgUtilisation + threshold;
//    // under utilized nodes have utilization(that is, used / capacity) less
//    // than lower limit
//    this.lowerLimit = clusterAvgUtilisation - threshold;

//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Lower limit for utilization is {} and Upper limit for " +
//          "utilization is {}", lowerLimit, upperLimit);
//    }
  }
}
