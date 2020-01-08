/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import java.util.HashMap;
import java.util.Map;


class BasicCapacityStats {
    private static final String DISK_CAPACITY = "diskCapacity";
    private static final String CPU_CAPACITY = "cpuCapacity";
    private static final String NW_IN_CAPACITY = "nwInCapacity";
    private static final String NW_OUT_CAPACITY = "nwOutCapacity";
    private double _diskCapacity;
    private double _cpuCapacity;
    private double _bytesInCapacity;
    private double _bytesOutCapacity;

  BasicCapacityStats(double diskCapacity, double cpuCapacity, double nwInCapacity, double nwOutCapacity) {
    _diskCapacity = diskCapacity < 0.0 ? 0.0 : diskCapacity;
    _cpuCapacity = cpuCapacity < 0.0 ? 0.0 : cpuCapacity;
    _bytesInCapacity = nwInCapacity < 0.0 ? 0.0 : nwInCapacity;
    _bytesOutCapacity = nwOutCapacity < 0.0 ? 0.0 : nwOutCapacity;
  }

  double diskCapacity() {
    return _diskCapacity;
  }

  double cpuCapacity() {
    return _cpuCapacity;
  }

  double bytesInCapacity() {
    return _bytesInCapacity;
  }

  double bytesOutCapacity() {
    return _bytesOutCapacity;
  }

  void addBasicStats(BasicCapacityStats basicStats) {
    _diskCapacity += basicStats.diskCapacity();
    _cpuCapacity += basicStats.cpuCapacity();
    _bytesInCapacity += basicStats.bytesInCapacity();
    _bytesOutCapacity += basicStats.bytesOutCapacity();
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = new HashMap<>(4);
    entry.put(DISK_CAPACITY, diskCapacity());
    entry.put(CPU_CAPACITY, cpuCapacity());
    entry.put(NW_IN_CAPACITY, bytesInCapacity());
    entry.put(NW_OUT_CAPACITY, bytesOutCapacity());
    return entry;
  }
}
