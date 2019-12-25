/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.SEC_TO_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector.METRIC_REGISTRY_NAME;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.METRIC_ANOMALY;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.BROKER_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;

@JsonResponseClass
public class AnomalyDetectorState {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorState.class);
  @JsonResponseField
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  @JsonResponseField
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  @JsonResponseField
  private static final String SELF_HEALING_ENABLED_RATIO = "selfHealingEnabledRatio";
  @JsonResponseField
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  @JsonResponseField
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  @JsonResponseField
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";
  @JsonResponseField
  private static final String RECENT_DISK_FAILURES = "recentDiskFailures";
  @JsonResponseField(required = false)
  private static final String ONGOING_SELF_HEALING_ANOMALY = "ongoingSelfHealingAnomaly";
  @JsonResponseField
  private static final String METRICS = "metrics";
  @JsonResponseField
  private static final String BALANCEDNESS_SCORE = "balancednessScore";
  static final String NUM_SELF_HEALING_STARTED = "numSelfHealingStarted";
  private static final long NO_ONGOING_ANOMALY_FLAG = -1L;

  // Recent anomalies with anomaly state by the anomaly type.
  private final Map<AnomalyType, Map<String, AnomalyState>> _recentAnomaliesByType;
  private Anomaly _ongoingSelfHealingAnomaly;
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;
  SelfHealingEnabledRatio _selfHealingEnabledRatio;
  // Maximum number of anomalies to keep in the anomaly detector state.
  private final int _numCachedRecentAnomalyStates;
  private AnomalyMetrics _metrics;
  // The detection time of the earliest ongoing anomaly. Expected to be cleared upon start of a proposal execution.
  private volatile long _ongoingAnomalyDetectionTimeMs;
  // The count during which there is at least one ongoing anomaly.
  private long _ongoingAnomalyCount;
  private double _ongoingAnomalyDurationSumForAverageMs;
  private final Time _time;
  private AtomicLong _numSelfHealingStarted;
  private final Map<AnomalyType, Meter> _anomalyRateByType;
  private double _balancednessScore;

  public AnomalyDetectorState(Time time,
                              Map<AnomalyType, Boolean> selfHealingEnabled,
                              int numCachedRecentAnomalyStates,
                              MetricRegistry dropwizardMetricRegistry) {
    _time = time;
    _numCachedRecentAnomalyStates = numCachedRecentAnomalyStates;
    _recentAnomaliesByType = new HashMap<>(KafkaAnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      _recentAnomaliesByType.put(anomalyType, new LinkedHashMap<String, AnomalyState>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, AnomalyState> eldest) {
          return this.size() > _numCachedRecentAnomalyStates;
        }
      });
    }
    _selfHealingEnabled = selfHealingEnabled;
    _selfHealingEnabledRatio = null;
    _ongoingSelfHealingAnomaly = null;
    _ongoingAnomalyDetectionTimeMs = NO_ONGOING_ANOMALY_FLAG;
    _ongoingAnomalyCount = 0L;
    _ongoingAnomalyDurationSumForAverageMs = 0;
    _numSelfHealingStarted = new AtomicLong(0L);

    Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs = new HashMap<>(KafkaAnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      meanTimeBetweenAnomaliesMs.put(anomalyType, 0.0);
    }
    _metrics = new AnomalyMetrics(meanTimeBetweenAnomaliesMs, 0.0, 0L, 0L);

    if (dropwizardMetricRegistry != null) {
      dropwizardMetricRegistry.register(MetricRegistry.name(METRIC_REGISTRY_NAME, "mean-time-to-start-fix-ms"),
                                        (Gauge<Double>) this::meanTimeToStartFixMs);
      dropwizardMetricRegistry.register(MetricRegistry.name(METRIC_REGISTRY_NAME, "number-of-self-healing-started"),
                                        (Gauge<Long>) this::numSelfHealingStarted);
      dropwizardMetricRegistry.register(MetricRegistry.name(METRIC_REGISTRY_NAME, "ongoing-anomaly-duration-ms"),
                                        (Gauge<Long>) this::ongoingAnomalyDurationMs);

      _anomalyRateByType = new HashMap<>(KafkaAnomalyType.cachedValues().size());
      _anomalyRateByType.put(BROKER_FAILURE,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "broker-failure-rate")));
      _anomalyRateByType.put(GOAL_VIOLATION,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "goal-violation-rate")));
      _anomalyRateByType.put(METRIC_ANOMALY,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "metric-anomaly-rate")));
      _anomalyRateByType.put(DISK_FAILURE,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(METRIC_REGISTRY_NAME, "disk-failure-rate")));
    } else {
      _anomalyRateByType = new HashMap<>(KafkaAnomalyType.cachedValues().size());
      KafkaAnomalyType.cachedValues().forEach(anomalyType -> _anomalyRateByType.put(anomalyType, new Meter()));
    }
  }

  /**
   * Mark the occurrence of an anomaly.
   *
   * @param anomalyType Type of anomaly.
   */
  void markAnomalyRate(AnomalyType anomalyType) {
    _anomalyRateByType.get(anomalyType).mark();
  }

  /**
   * Refresh the anomaly metrics.
   *
   * @param selfHealingEnabledRatio The ratio
   * @param balancednessScore A metric to quantify how well the load distribution on a cluster satisfies the anomaly
   * detection goals.
   */
  synchronized void refreshMetrics(Map<AnomalyType, Float> selfHealingEnabledRatio, double balancednessScore) {
    if (selfHealingEnabledRatio == null) {
      throw new IllegalArgumentException("Attempt to set selfHealingEnabledRatio with null.");
    }

    // Retrieve mean time between anomalies, record the time in ms.
    Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs = new HashMap<>(KafkaAnomalyType.cachedValues().size());
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      meanTimeBetweenAnomaliesMs.put(anomalyType, _anomalyRateByType.get(anomalyType).getMeanRate() * SEC_TO_MS);
    }

    _metrics = new AnomalyMetrics(meanTimeBetweenAnomaliesMs, meanTimeToStartFixMs(), _numSelfHealingStarted.get(), ongoingAnomalyDurationMs());
    _selfHealingEnabledRatio = new SelfHealingEnabledRatio(selfHealingEnabledRatio.size());
    selfHealingEnabledRatio.forEach((key, value) -> _selfHealingEnabledRatio.put(key, value));
    _balancednessScore = balancednessScore;
  }

  /**
   * @return Duration of the ongoing anomaly in ms if there is one, 0 otherwise. This method is intentionally not
   * thread-safe to avoid the synchronization latency; hence, it can potentially be stale.
   */
  private long ongoingAnomalyDurationMs() {
    return _ongoingAnomalyDetectionTimeMs != NO_ONGOING_ANOMALY_FLAG ? _time.milliseconds() - _ongoingAnomalyDetectionTimeMs : 0L;
  }

  /**
   * @return Mean time to start a fix in ms. This method is intentionally not thread-safe to avoid the synchronization
   * latency; hence, it can potentially be stale.
   */
  private double meanTimeToStartFixMs() {
    long fixedAnomalyDurations = _ongoingAnomalyDetectionTimeMs == NO_ONGOING_ANOMALY_FLAG ? _ongoingAnomalyCount
                                                                                           : _ongoingAnomalyCount - 1;

    return fixedAnomalyDurations == 0L ? 0 : _ongoingAnomalyDurationSumForAverageMs / fixedAnomalyDurations;
  }

  /**
   * If there is an ongoing anomaly, clear the earliest detection time to indicate start of an ongoing fix.
   */
  synchronized void maybeClearOngoingAnomalyDetectionTimeMs() {
    if (_ongoingAnomalyDetectionTimeMs != NO_ONGOING_ANOMALY_FLAG) {
      double elapsed = _time.milliseconds() - _ongoingAnomalyDetectionTimeMs;
      _ongoingAnomalyDurationSumForAverageMs += elapsed;
      // Clear ongoing anomaly detection time
      _ongoingAnomalyDetectionTimeMs = NO_ONGOING_ANOMALY_FLAG;
    }
  }

  /**
   * The {@link #_ongoingAnomalyDetectionTimeMs} is updated only when there is no earlier ongoing anomaly.
   * See {@link #maybeClearOngoingAnomalyDetectionTimeMs()} for clearing the ongoing anomaly detection time.
   */
  synchronized void maybeSetOngoingAnomalyDetectionTimeMs() {
    if (_ongoingAnomalyDetectionTimeMs == NO_ONGOING_ANOMALY_FLAG) {
      _ongoingAnomalyDetectionTimeMs = _time.milliseconds();
      _ongoingAnomalyCount++;
    }
  }

  /**
   * @return Number of anomaly fixes started by the anomaly detector for self healing.
   */
  long numSelfHealingStarted() {
    return _numSelfHealingStarted.get();
  }

  /**
   * Increment the number of self healing actions started successfully.
   */
  void incrementNumSelfHealingStarted() {
    _numSelfHealingStarted.incrementAndGet();
  }

  /**
   * Package private for testing
   */
  Map<String, Object> metrics() {
    return _metrics.getJsonStructure();
  }

  private SelfHealingEnabledRatio selfHealingEnabledRatio() {
    return _selfHealingEnabledRatio == null ? new SelfHealingEnabledRatio(0) : _selfHealingEnabledRatio;
  }

  /**
   * Update anomaly status once associated self-healing operation has finished.
   *
   * @param anomalyId Unique id of anomaly which triggered self-healing operation.
   */
  public synchronized void markSelfHealingFinished(String anomalyId) {
    if (_ongoingSelfHealingAnomaly == null || !_ongoingSelfHealingAnomaly.anomalyId().equals(anomalyId)) {
      throw new IllegalStateException(String.format("Anomaly %s is not marked as %s state in AnomalyDetector.",
                                                    anomalyId, AnomalyState.Status.FIX_STARTED));
    }
    _ongoingSelfHealingAnomaly = null;
  }

  /**
   * Add detected anomaly to the anomaly detector state.
   *
   * @param anomalyType Type of the detected anomaly.
   * @param anomaly The detected anomaly.
   */
  void addAnomalyDetection(AnomalyType anomalyType, Anomaly anomaly) {
    _recentAnomaliesByType.get(anomalyType).put(anomaly.anomalyId(), new AnomalyState(anomaly));
  }

  /**
   * Update state regarding how the anomaly has been handled.
   *
   * @param anomaly The anomaly to handle.
   * @param status A status information regarding how the anomaly was handled.
   */
  synchronized void onAnomalyHandle(Anomaly anomaly, AnomalyState.Status status) {
    AnomalyType anomalyType = anomaly.anomalyType();
    String anomalyId = anomaly.anomalyId();

    if (status == AnomalyState.Status.FIX_STARTED) {
      _ongoingSelfHealingAnomaly = anomaly;
    }

    AnomalyState recentAnomalyState = _recentAnomaliesByType.get(anomalyType).get(anomalyId);
    if (recentAnomalyState != null) {
      recentAnomalyState.setStatus(status);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Anomaly (type: {}, anomalyId: {}) is no longer in the anomaly detector state cache.", anomalyType, anomalyId);
    }
  }

  /**
   * Set self healing for the given anomaly type.
   *
   * @param anomalyType Type of anomaly.
   * @param isSelfHealingEnabled True if self healing is enabled, false otherwise.
   */
  public synchronized void setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    _selfHealingEnabled.put(anomalyType, isSelfHealingEnabled);
  }

  /**
   * Package private for unit tests.
   */
  Map<AnomalyType, Map<String, AnomalyState>> recentAnomaliesByType() {
    return _recentAnomaliesByType;
  }

  private Set<Map<String, Object>> recentAnomalies(AnomalyType anomalyType, boolean isJson) {
    Map<String, AnomalyState> anomaliesById = _recentAnomaliesByType.get(anomalyType);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>(_numCachedRecentAnomalyStates);
    for (Map.Entry<String, AnomalyState> entry: anomaliesById.entrySet()) {
      recentAnomalies.add(new AnomalyDetails(entry.getValue(), anomalyType, false, isJson).populateAnomalyDetails());
    }
    return recentAnomalies;
  }

  private Map<Boolean, Set<String>> getSelfHealingByEnableStatus() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = new HashMap<>(2);
    selfHealingByEnableStatus.put(true, new HashSet<>(KafkaAnomalyType.cachedValues().size()));
    selfHealingByEnableStatus.put(false, new HashSet<>(KafkaAnomalyType.cachedValues().size()));
    _selfHealingEnabled.forEach((key, value) -> {
      selfHealingByEnableStatus.get(value).add(key.toString());
    });
    return selfHealingByEnableStatus;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public synchronized Map<String, Object> getJsonStructure() {
    Map<String, Object> anomalyDetectorState = new HashMap<>(_recentAnomaliesByType.size() + (_ongoingSelfHealingAnomaly == null ? 5 : 6));
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    anomalyDetectorState.put(SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true));
    anomalyDetectorState.put(SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false));
    anomalyDetectorState.put(SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio().getJsonStructure());
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, true));
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, true));
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, true));
    anomalyDetectorState.put(RECENT_DISK_FAILURES, recentAnomalies(DISK_FAILURE, true));
    anomalyDetectorState.put(METRICS, metrics());
    if (_ongoingSelfHealingAnomaly != null) {
      anomalyDetectorState.put(ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly.anomalyId());
    }
    anomalyDetectorState.put(BALANCEDNESS_SCORE, _balancednessScore);
    return anomalyDetectorState;
  }

  @Override
  public synchronized String toString() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%.3f}%n",
                         SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true),
                         SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false),
                         SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio().getJsonStructure(),
                         RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, false),
                         RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, false),
                         RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, false),
                         RECENT_DISK_FAILURES, recentAnomalies(DISK_FAILURE, false),
                         METRICS, _metrics,
                         ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly == null
                                                       ? "None" : _ongoingSelfHealingAnomaly.anomalyId(),
                         BALANCEDNESS_SCORE, _balancednessScore);
  }
}
