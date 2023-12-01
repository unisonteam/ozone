package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import javax.annotation.Nonnull;

public class FindTargetStrategyFactory {
  public static FindTargetStrategy create(@Nonnull StorageContainerManager scm, boolean isNetworkTopologyEnabled) {
    if (isNetworkTopologyEnabled) {
      return new FindTargetGreedyByNetworkTopology(scm);
    } else {
      return new FindTargetGreedyByUsageInfo(scm);
    }
  }
}
