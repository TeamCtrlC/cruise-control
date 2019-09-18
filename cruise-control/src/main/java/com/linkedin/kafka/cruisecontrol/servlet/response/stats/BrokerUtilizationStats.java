/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * Get broker level stats related to resource utilization in human readable format.
 */
public class BrokerUtilizationStats extends BrokerStats {
  private static final String HOST = "Host";
  private static final String HOSTS = "hosts";
  private final List<SingleBrokerUtilizationStats> _brokerStats;
  private final SortedMap<String, BasicUtilizationStats> _hostStats;

  public BrokerUtilizationStats(KafkaCruiseControlConfig config) {
    super(config);
    _brokerStats = new ArrayList<>();
    _hostStats = new ConcurrentSkipListMap<>();
  }

  public void addSingleBrokerUtilizationStats(String host, int id, Broker.State state, double diskUtil, double cpuUtil, double leaderBytesInRate,
                                   double followerBytesInRate, double bytesOutRate, double potentialBytesOutRate,
                                   int numReplicas, int numLeaders, boolean isEstimated, double capacity) {

    SingleBrokerUtilizationStats singleBrokerStats =
        new SingleBrokerUtilizationStats(host, id, state, diskUtil, cpuUtil, leaderBytesInRate, followerBytesInRate, bytesOutRate,
                              potentialBytesOutRate, numReplicas, numLeaders, isEstimated, capacity);
    _brokerStats.add(singleBrokerStats);
    _hostFieldLength = Math.max(_hostFieldLength, host.length());
    _hostStats.computeIfAbsent(host, h -> new BasicUtilizationStats(0.0, 0.0, 0.0, 0.0,
                                                         0.0, 0.0, 0, 0, 0.0))
              .addBasicStats(singleBrokerStats.basicUtilizationStats());
    _isBrokerStatsEstimated = _isBrokerStatsEstimated || isEstimated;
  }

  @Override
  protected void discardIrrelevantResponse() {
    // Discard irrelevant response.
    _brokerStats.clear();
    _hostStats.clear();
  }

  /**
   * Return an object that can be further be used to encode into JSON
   */
  @Override
  public Map<String, Object> getJsonStructure() {
    List<Map<String, Object>> hostStats = new ArrayList<>(_hostStats.size());

    // host level statistics
    for (Map.Entry<String, BasicUtilizationStats> entry : _hostStats.entrySet()) {
      Map<String, Object> hostEntry = entry.getValue().getJsonStructure();
      hostEntry.put(HOST, entry.getKey());
      hostStats.add(hostEntry);
    }

    // broker level statistics
    List<Map<String, Object>> brokerStats = new ArrayList<>(_brokerStats.size());
    for (SingleBrokerUtilizationStats stats : _brokerStats) {
      Map<String, Object> brokerEntry = stats.getJsonStructure();
      brokerStats.add(brokerEntry);
    }

    // consolidated statistics
    Map<String, Object> stats = new HashMap<>(2);
    stats.put(HOSTS, hostStats);
    stats.put(BROKERS, brokerStats);
    return stats;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // put host stats.
    sb.append(String.format("%n%" + _hostFieldLength + "s%26s%15s%25s%25s%20s%20s%20s%n",
                            "HOST", "DISK(MB)/_(%)_", "CPU(%)", "LEADER_NW_IN(KB/s)",
                            "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
    for (Map.Entry<String, BasicUtilizationStats> entry : _hostStats.entrySet()) {
      BasicUtilizationStats stats = entry.getValue();
      sb.append(String.format("%" + _hostFieldLength + "s,%19.3f/%05.2f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                              entry.getKey(),
                              stats.diskUtil(),
                              stats.diskUtilPct(),
                              stats.cpuUtil(),
                              stats.leaderBytesInRate(),
                              stats.followerBytesInRate(),
                              stats.bytesOutRate(),
                              stats.potentialBytesOutRate(),
                              stats.numLeaders(),
                              stats.numReplicas()));
    }

    // put broker stats.
    sb.append(String.format("%n%n%" + _hostFieldLength + "s%15s%26s%15s%25s%25s%20s%20s%20s%n",
                            "HOST", "BROKER", "DISK(MB)/_(%)_", "CPU(%)", "LEADER_NW_IN(KB/s)",
                            "FOLLOWER_NW_IN(KB/s)", "NW_OUT(KB/s)", "PNW_OUT(KB/s)", "LEADERS/REPLICAS"));
    for (SingleBrokerUtilizationStats stats : _brokerStats) {
      sb.append(String.format("%" + _hostFieldLength + "s,%14d,%19.3f/%05.2f,%14.3f,%24.3f,%24.3f,%19.3f,%19.3f,%14d/%d%n",
                              stats.host(),
                              stats.id(),
                              stats.basicUtilizationStats().diskUtil(),
                              stats.basicUtilizationStats().diskUtilPct(),
                              stats.basicUtilizationStats().cpuUtil(),
                              stats.basicUtilizationStats().leaderBytesInRate(),
                              stats.basicUtilizationStats().followerBytesInRate(),
                              stats.basicUtilizationStats().bytesOutRate(),
                              stats.basicUtilizationStats().potentialBytesOutRate(),
                              stats.basicUtilizationStats().numLeaders(),
                              stats.basicUtilizationStats().numReplicas()));
    }

    return sb.toString();
  }
}
