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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Find a target for a source datanode with greedy strategy.
 */
public abstract class AbstractFindTargetGreedy implements FindTargetStrategy {
  private final Logger logger;
  private final ContainerManager containerManager;
  private final PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private final Map<DatanodeDetails, Long> sizeEnteringNode;
  private final NodeManager nodeManager;
  private Collection<DatanodeUsageInfo> potentialTargets;

  protected AbstractFindTargetGreedy(@Nonnull StorageContainerManager scm, @Nonnull Class<?> findTargetClazz) {
    sizeEnteringNode = new HashMap<>();
    containerManager = scm.getContainerManager();
    placementPolicyValidateProxy = scm.getPlacementPolicyValidateProxy();
    nodeManager = scm.getScmNodeManager();
    logger = LoggerFactory.getLogger(findTargetClazz);
  }

  protected void setPotentialTargets(@Nonnull Collection<DatanodeUsageInfo> pt) {
    potentialTargets = pt;
  }

  protected int compareByUsage(@Nonnull DatanodeUsageInfo a, @Nonnull DatanodeUsageInfo b) {
    double currentUsageOfA = a.calculateUtilization(
        sizeEnteringNode.get(a.getDatanodeDetails()));
    double currentUsageOfB = b.calculateUtilization(
        sizeEnteringNode.get(b.getDatanodeDetails()));
    int ret = Double.compare(currentUsageOfA, currentUsageOfB);
    if (ret != 0) {
      return ret;
    }
    UUID uuidA = a.getDatanodeDetails().getUuid();
    UUID uuidB = b.getDatanodeDetails().getUuid();
    return uuidA.compareTo(uuidB);
  }

  @Override
  public ContainerMoveSelection findTargetForContainerMove(
      @Nonnull DatanodeDetails source,
      @Nonnull Set<ContainerID> candidateContainers,
      long maxSizeEnteringTarget,
      double upperLimit)
  {
    sortTargetForSource(source);
    for (DatanodeUsageInfo targetInfo : potentialTargets) {
      DatanodeDetails target = targetInfo.getDatanodeDetails();
      for (ContainerID container : candidateContainers) {
        Set<ContainerReplica> replicas;
        ContainerInfo containerInfo;
        try {
          replicas = containerManager.getContainerReplicas(container);
          containerInfo = containerManager.getContainer(container);
        } catch (ContainerNotFoundException e) {
          logger.warn("Could not get Container {} from Container Manager for " +
              "obtaining replicas in Container Balancer.", container, e);
          continue;
        }

        if (replicas.stream().noneMatch(replica -> replica.getDatanodeDetails().equals(target)) &&
            containerMoveSatisfiesPlacementPolicy(container, replicas, source, target) &&
            canSizeEnterTarget(target, containerInfo.getUsedBytes(), maxSizeEnteringTarget, upperLimit))
        {
          return new ContainerMoveSelection(target, container);
        }
      }
    }
    logger.info("Container Balancer could not find a target for " +
        "source datanode {}", source.getUuidString());
    return null;
  }

  /**
   * Checks if container being present in target instead of source satisfies
   * the placement policy.
   * @param containerID Container to be moved from source to target
   * @param replicas Set of replicas of the given container
   * @param source Source datanode for container move
   * @param target Target datanode for container move
   * @return true if placement policy is satisfied, otherwise false
   */
  private boolean containerMoveSatisfiesPlacementPolicy(
      @Nonnull ContainerID containerID,
      @Nonnull Set<ContainerReplica> replicas,
      @Nonnull DatanodeDetails source,
      @Nonnull DatanodeDetails target)
  {
    ContainerInfo containerInfo;
    try {
      containerInfo = containerManager.getContainer(containerID);
    } catch (ContainerNotFoundException e) {
      logger.warn("Could not get Container {} from Container Manager while " +
          "checking if container move satisfies placement policy in " +
          "Container Balancer.", containerID.toString(), e);
      return false;
    }
    List<DatanodeDetails> replicaList =
        replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .filter(datanodeDetails -> !datanodeDetails.equals(source))
            .collect(Collectors.toList());
    replicaList.add(target);
    ContainerPlacementStatus placementStatus = placementPolicyValidateProxy
        .validateContainerPlacement(replicaList, containerInfo);

    boolean isPolicySatisfied = placementStatus.isPolicySatisfied();
    if (!isPolicySatisfied) {
      logger.debug("Moving container {} from source {} to target {} will not " +
              "satisfy placement policy.", containerID, source.getUuidString(), target.getUuidString());
    }
    return isPolicySatisfied;
  }

  /**
   * Checks if specified size can enter specified target datanode
   * according to {@link ContainerBalancerConfiguration}
   * "size.entering.target.max".
   *
   * @param target target datanode in which size is entering
   * @param size   size in bytes
   * @return true if size can enter target, else false
   */
  private boolean canSizeEnterTarget(@Nonnull DatanodeDetails target,
                                     long size, long maxSizeEnteringTarget, double upperLimit)
  {
    if (sizeEnteringNode.containsKey(target)) {
      long sizeEnteringAfterMove = sizeEnteringNode.get(target) + size;
      //size can be moved into target datanode only when the following
      //two condition are met.
      //1 sizeEnteringAfterMove does not succeed the configured
      // MaxSizeEnteringTarget
      //2 current usage of target datanode plus sizeEnteringAfterMove
      // is smaller than or equal to upperLimit
      if (sizeEnteringAfterMove > maxSizeEnteringTarget) {
        logger.debug("{} bytes cannot enter datanode {} because 'size" +
                ".entering.target.max' limit is {} and {} bytes have already " +
                "entered.", size, target.getUuidString(),
            maxSizeEnteringTarget,
            sizeEnteringNode.get(target));
        return false;
      }
      if (Double.compare(nodeManager.getUsageInfo(target)
          .calculateUtilization(sizeEnteringAfterMove), upperLimit) > 0) {
        logger.debug("{} bytes cannot enter datanode {} because its " +
                "utilization will exceed the upper limit of {}.", size,
            target.getUuidString(), upperLimit);
        return false;
      }
      return true;
    }

    logger.warn("No record of how much size has entered datanode {}",
        target.getUuidString());
    return false;
  }

  @Override
  public void increaseSizeEntering(@Nonnull DatanodeDetails target, long size, long maxSizeEnteringTarget) {
    if (sizeEnteringNode.containsKey(target)) {
      long totalEnteringSize = sizeEnteringNode.get(target) + size;
      sizeEnteringNode.put(target, totalEnteringSize);
      potentialTargets.removeIf(
          c -> c.getDatanodeDetails().equals(target));
      if (totalEnteringSize < maxSizeEnteringTarget) {
        //reorder
        potentialTargets.add(nodeManager.getUsageInfo(target));
      }
      return;
    }
    logger.warn("Cannot find {} in the candidates target nodes",
        target.getUuid());
  }

  @Override
  public void reInitialize(@Nonnull List<DatanodeUsageInfo> potentialDataNodes) {
    sizeEnteringNode.clear();
    potentialTargets.clear();
    potentialDataNodes.forEach(datanodeUsageInfo -> {
      sizeEnteringNode.putIfAbsent(datanodeUsageInfo.getDatanodeDetails(), 0L);
      potentialTargets.add(datanodeUsageInfo);
    });
  }

  @VisibleForTesting
  public @Nonnull Collection<DatanodeUsageInfo> getPotentialTargets() {
    return potentialTargets;
  }

  /**
   * sort potentialTargets for specified source datanode according to
   * network topology if enabled.
   * @param source the specified source datanode
   */
  @VisibleForTesting
  public abstract void sortTargetForSource(@Nonnull DatanodeDetails source);

  /**
   * Resets the collection of target datanode usage info that will be
   * considered for balancing. Gets the latest usage info from node manager.
   * @param targets collection of target {@link DatanodeDetails} that
   *                containers can move to
   */
  @Override
  public final void resetPotentialTargets(@Nonnull Collection<DatanodeDetails> targets) {
    potentialTargets.clear();
    targets.forEach(datanodeDetails -> {
      DatanodeUsageInfo usageInfo = nodeManager.getUsageInfo(datanodeDetails);
      sizeEnteringNode.putIfAbsent(datanodeDetails, 0L);
      potentialTargets.add(usageInfo);
    });
  }
}
