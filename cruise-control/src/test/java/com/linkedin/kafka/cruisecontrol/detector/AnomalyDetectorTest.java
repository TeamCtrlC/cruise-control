/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


/**
 * Unit test class for anomaly detector.
 */
public class AnomalyDetectorTest {
  static private final long MOCK_ANOMALY_DETECTION_INTERVAL_MS = 3000L;
  static private final long MOCK_DELAY_CHECK_MS = 1000L;


  private static void startPeriodicDetectors(ScheduledExecutorService mockDetectorScheduler,
                                             GoalViolationDetector mockGoalViolationDetector,
                                             MetricAnomalyDetector mockMetricAnomalyDetector,
                                             ScheduledExecutorService executorService) {
    // Starting periodic goal violation detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockGoalViolationDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting periodic metric anomaly detection.
    EasyMock.expect(mockDetectorScheduler.scheduleAtFixedRate(EasyMock.eq(mockMetricAnomalyDetector),
                                                              EasyMock.anyLong(),
                                                              EasyMock.eq(MOCK_ANOMALY_DETECTION_INTERVAL_MS),
                                                              EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);

    // Starting anomaly handler
    EasyMock.expect(mockDetectorScheduler.submit(EasyMock.isA(AnomalyDetector.AnomalyHandlerTask.class)))
            .andDelegateTo(executorService);
  }

  @Test
  public void testDelayedCheck() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onBrokerFailure(EasyMock.isA(BrokerFailures.class)))
            .andReturn(AnomalyNotificationResult.check(MOCK_DELAY_CHECK_MS));

    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    // Schedule a delayed check
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(MOCK_DELAY_CHECK_MS),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    shutdownDetector(mockDetectorScheduler, executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(ExecutorState.noTaskInProgress(null, null), null, null,
                                              null, null));
    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      anomalies.add(new BrokerFailures(mockKafkaCruiseControl, Collections.singletonMap(0, 100L),
                                       false, true, true,
                                       Collections.emptyList()));
      while (anomalyDetector.numCheckedWithDelay() < 1) {
        // Wait for the anomaly to be checked with delay before attempting to shutdown the anomaly detector.
      }
      anomalyDetector.shutdown();
      assertEquals(0, anomalyDetector.numFixed());
      assertEquals(1, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
    } finally {
      executorService.shutdown();
    }
    EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
  }

  private static void shutdownDetector(ScheduledExecutorService mockDetectorScheduler,
                                       ScheduledExecutorService executorService) throws InterruptedException {
    mockDetectorScheduler.shutdown();
    EasyMock.expectLastCall().andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.awaitTermination(MOCK_ANOMALY_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS)).andDelegateTo(executorService);
    EasyMock.expect(mockDetectorScheduler.isTerminated()).andDelegateTo(executorService);
  }

  private static void replayMocks(AnomalyNotifier mockAnomalyNotifier,
                                  BrokerFailureDetector mockBrokerFailureDetector,
                                  GoalViolationDetector mockGoalViolationDetector,
                                  MetricAnomalyDetector mockMetricAnomalyDetector,
                                  ScheduledExecutorService mockDetectorScheduler,
                                  KafkaCruiseControl mockKafkaCruiseControl) {
    EasyMock.replay(mockAnomalyNotifier);
    EasyMock.replay(mockBrokerFailureDetector);
    EasyMock.replay(mockGoalViolationDetector);
    EasyMock.replay(mockMetricAnomalyDetector);
    EasyMock.replay(mockDetectorScheduler);
    EasyMock.replay(mockKafkaCruiseControl);
  }

  @Test
  public void testFix() throws InterruptedException, KafkaCruiseControlException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    EasyMock.expect(mockAnomalyNotifier.onGoalViolation(EasyMock.isA(GoalViolations.class)))
            .andReturn(AnomalyNotificationResult.fix());


    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    shutdownDetector(mockDetectorScheduler, executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(ExecutorState.noTaskInProgress(null, null), null, null,
                                              null, null));
    EasyMock.expect(mockKafkaCruiseControl.rebalance(EasyMock.eq(Collections.emptyList()),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(null),
                                                     EasyMock.anyObject(OperationProgress.class),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(null),
                                                     EasyMock.eq(null),
                                                     EasyMock.anyString(),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(false),
                                                     EasyMock.eq(true),
                                                     EasyMock.eq(Collections.emptySet())))
            .andReturn(null);
    EasyMock.expect(mockKafkaCruiseControl.meetCompletenessRequirements(Collections.emptyList())).andReturn(true);
    EasyMock.expect(mockDetectorScheduler.schedule(EasyMock.isA(Runnable.class),
                                                   EasyMock.eq(0L),
                                                   EasyMock.eq(TimeUnit.MILLISECONDS)))
            .andReturn(null);
    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      GoalViolations violations = new GoalViolations(mockKafkaCruiseControl, true,
                                                     true, true,
                                                     Collections.emptyList());
      violations.addViolation("RackAwareGoal", true);
      anomalies.add(violations);
      while (anomalyDetector.numFixed() < 1) {
        // Wait for the anomaly to be fixed before attempting to shutdown the anomaly detector.
      }
      anomalyDetector.shutdown();
      assertEquals(1, anomalyDetector.numFixed());
      assertEquals(0, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
      EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testExecutionInProgress() throws InterruptedException {
    LinkedBlockingDeque<Anomaly> anomalies = new LinkedBlockingDeque<>();
    AnomalyNotifier mockAnomalyNotifier = EasyMock.mock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    ScheduledExecutorService mockDetectorScheduler = EasyMock.mock(ScheduledExecutorService.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);

    startPeriodicDetectors(mockDetectorScheduler, mockGoalViolationDetector, mockMetricAnomalyDetector, executorService);
    shutdownDetector(mockDetectorScheduler, executorService);

    // The following state are used to test the delayed check when executor is idle.
    EasyMock.expect(mockKafkaCruiseControl.state(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new CruiseControlState(
                ExecutorState.operationInProgress(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                  null,
                                                  1,
                                                  1,
                                                  null,
                                                  null,
                                                  null),
                null, null, null, null));

    replayMocks(mockAnomalyNotifier, mockBrokerFailureDetector, mockGoalViolationDetector, mockMetricAnomalyDetector,
                mockDetectorScheduler, mockKafkaCruiseControl);

    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalies, MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl, mockAnomalyNotifier,
                                                          mockGoalViolationDetector, mockBrokerFailureDetector,
                                                          mockMetricAnomalyDetector, mockDetectorScheduler,
                                                          EasyMock.mock(LoadMonitor.class));

    try {
      anomalyDetector.startDetection();
      anomalies.add(new GoalViolations(mockKafkaCruiseControl, true,
                                       true, true,
                                       Collections.emptyList()));
      while (!anomalies.isEmpty()) {
        // Just wait for the anomalies to be drained.
      }
      anomalyDetector.shutdown();
      assertEquals(0, anomalyDetector.numFixed());
      assertEquals(0, anomalyDetector.numCheckedWithDelay());
      assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));
    } finally {
      executorService.shutdown();
    }
    EasyMock.verify(mockAnomalyNotifier, mockDetectorScheduler, mockKafkaCruiseControl);
  }

  @Test
  public void testShutdown() throws InterruptedException {
    AnomalyNotifier mockAnomalyNotifier = EasyMock.createNiceMock(AnomalyNotifier.class);
    BrokerFailureDetector mockBrokerFailureDetector = EasyMock.createNiceMock(BrokerFailureDetector.class);
    GoalViolationDetector mockGoalViolationDetector = EasyMock.createNiceMock(GoalViolationDetector.class);
    MetricAnomalyDetector mockMetricAnomalyDetector = EasyMock.createNiceMock(MetricAnomalyDetector.class);
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    ScheduledExecutorService detectorScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("AnomalyDetector", false, null));

    AnomalyDetector anomalyDetector = new AnomalyDetector(new LinkedBlockingDeque<>(), MOCK_ANOMALY_DETECTION_INTERVAL_MS, mockKafkaCruiseControl,
                                                          mockAnomalyNotifier, mockGoalViolationDetector,
                                                          mockBrokerFailureDetector, mockMetricAnomalyDetector,
                                                          detectorScheduler, EasyMock.mock(LoadMonitor.class));

    anomalyDetector.shutdown();
    Thread t = new Thread(anomalyDetector::shutdown);
    t.start();
    t.join(30000L);
    assertEquals(0, anomalyDetector.numFixed());
    assertEquals(0, anomalyDetector.numCheckedWithDelay());
    assertTrue(detectorScheduler.isTerminated());
  }
}
