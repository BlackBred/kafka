/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server;

import kafka.cluster.Partition;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionStateSummaryData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryParameters;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import scala.jdk.javaapi.OptionConverters;

/**
 * Advances {@code logStartOffset} of the partitions led by this broker to the position that the groups configured in
 * {@link org.apache.kafka.common.config.TopicConfig#RETENTION_CONSUMED_GROUPS_CONFIG} have consumed past. Records are
 * then deleted by the existing log start offset retention path once they have been consumed, instead of being retained
 * until {@code retention.ms} or {@code retention.bytes} is breached. Because segments below {@code logStartOffset} are
 * never copied to remote storage, consumed records are also never uploaded when tiered storage is enabled.
 *
 * <p>The consumed position of a share group is its share-partition start offset, which is read through the
 * {@link Persister}. Reading it from the durable share group state rather than from an in-memory share partition means
 * the position is available on a broker that has not served share fetches for the partition, and immediately after a
 * leadership change.
 *
 * <p>The component fails safe: whenever a consumed position cannot be established for every configured group of a
 * partition, that partition is left untouched and its records are retained under the ordinary retention settings.
 */
public class ConsumedRetentionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumedRetentionManager.class);

    private static final String METRICS_PACKAGE = "kafka.log";
    private static final String METRICS_TYPE = "ConsumedRetentionManager";
    private static final String TASK_NAME = "kafka-consumed-retention-check";

    // Visible for testing
    static final String ENABLED_PARTITION_COUNT_METRIC = "EnabledPartitionCount";
    static final String SKIPPED_PARTITION_COUNT_METRIC = "SkippedPartitionCount";
    static final String CHECK_FAILURES_METRIC = "CheckFailuresPerSec";

    private final Function<TopicPartition, Optional<Partition>> partitionLookup;
    private final Persister persister;
    private final Scheduler scheduler;
    private final long checkIntervalMs;

    /**
     * The partitions led by this broker that have consumption-driven retention configured. Only the identity is kept:
     * the {@link Partition}, its topic id and its configuration are all resolved again on every check, so that a
     * reassigned partition is not held on to and so that changes to the configured groups and lag are picked up without
     * a notification.
     */
    private final Set<TopicPartition> enabledPartitions = ConcurrentHashMap.newKeySet();

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(METRICS_PACKAGE, METRICS_TYPE);
    private final AtomicInteger enabledPartitionCount = new AtomicInteger();
    private final AtomicInteger skippedPartitionCount = new AtomicInteger();
    private final Meter checkFailures;

    /** Guards against overlapping checks, since the previous one may still be waiting on the share group state. */
    private final AtomicBoolean checkInFlight = new AtomicBoolean();
    private volatile ScheduledFuture<?> scheduledCheck;

    public ConsumedRetentionManager(Function<TopicPartition, Optional<Partition>> partitionLookup,
                                    Persister persister,
                                    Scheduler scheduler,
                                    long checkIntervalMs) {
        this.partitionLookup = partitionLookup;
        this.persister = persister;
        this.scheduler = scheduler;
        this.checkIntervalMs = checkIntervalMs;
        this.checkFailures = metricsGroup.newMeter(CHECK_FAILURES_METRIC, "failures", TimeUnit.SECONDS);
        metricsGroup.newGauge(ENABLED_PARTITION_COUNT_METRIC, enabledPartitionCount::get);
        metricsGroup.newGauge(SKIPPED_PARTITION_COUNT_METRIC, skippedPartitionCount::get);
    }

    public void startup() {
        scheduledCheck = scheduler.schedule(TASK_NAME, this::maybeAdvanceLogStartOffsets, checkIntervalMs, checkIntervalMs);
    }

    @Override
    public void close() {
        ScheduledFuture<?> task = scheduledCheck;
        if (task != null) {
            task.cancel(false);
        }
        metricsGroup.removeMetric(ENABLED_PARTITION_COUNT_METRIC);
        metricsGroup.removeMetric(SKIPPED_PARTITION_COUNT_METRIC);
        metricsGroup.removeMetric(CHECK_FAILURES_METRIC);
    }

    /**
     * Registers the partitions this broker became the leader of that have consumption-driven retention configured, and
     * unregisters the ones it became a follower of. A new leader whose configuration does not enable the feature is
     * unregistered rather than ignored, so that the topic configuration change hook can call this method
     * unconditionally and have it handle both enabling and disabling the feature.
     */
    public void onLeadershipChange(Set<Partition> partitionsBecomeLeader, Set<Partition> partitionsBecomeFollower) {
        if (partitionsBecomeLeader.isEmpty() && partitionsBecomeFollower.isEmpty()) {
            return;
        }
        List<TopicPartition> registered = new ArrayList<>();
        List<TopicPartition> unregistered = new ArrayList<>();
        for (Partition partition : partitionsBecomeFollower) {
            if (enabledPartitions.remove(partition.topicPartition())) {
                unregistered.add(partition.topicPartition());
            }
        }
        for (Partition partition : partitionsBecomeLeader) {
            TopicPartition topicPartition = partition.topicPartition();
            boolean enabled = OptionConverters.toJava(partition.leaderLogIfLocal())
                .map(log -> log.config().consumedRetentionEnabled())
                .orElse(false);
            if (enabled) {
                if (enabledPartitions.add(topicPartition)) {
                    registered.add(topicPartition);
                }
            } else if (enabledPartitions.remove(topicPartition)) {
                unregistered.add(topicPartition);
            }
        }
        if (!registered.isEmpty() || !unregistered.isEmpty()) {
            LOG.debug("Registered {} and unregistered {} for consumption-driven retention, {} partitions are registered now",
                registered, unregistered, enabledPartitions.size());
        }
    }

    /**
     * Unregisters partitions that this broker no longer hosts, such as deleted or reassigned ones.
     */
    public void stopPartitions(Set<TopicPartition> partitions) {
        partitions.forEach(enabledPartitions::remove);
    }

    // Visible for testing
    void maybeAdvanceLogStartOffsets() {
        if (!checkInFlight.compareAndSet(false, true)) {
            LOG.debug("Skipping consumed retention check because the previous check has not completed yet");
            return;
        }
        boolean checkFinished = true;
        try {
            CollectedTargets collected = collectTargets();
            Map<TopicIdPartition, ConsumedRetentionTarget> targets = collected.targets();
            enabledPartitionCount.set(targets.size() + collected.withoutTopicId());
            if (targets.isEmpty()) {
                skippedPartitionCount.set(collected.withoutTopicId());
                return;
            }
            CompletableFuture<Void> check = readConsumedPositions(targets, collected.withoutTopicId());
            check.whenComplete((ignored, error) -> {
                if (error != null) {
                    checkFailures.mark();
                    LOG.error("Consumed retention check failed", error);
                }
                checkInFlight.set(false);
            });
            checkFinished = false;
        } catch (Throwable e) {
            checkFailures.mark();
            LOG.error("Consumed retention check failed", e);
        } finally {
            if (checkFinished) {
                checkInFlight.set(false);
            }
        }
    }

    /**
     * The registered partitions that this broker still leads with consumption-driven retention configured. A partition
     * that no longer qualifies is unregistered here, so that a registration which was not followed by a leadership or
     * configuration change notification does not linger.
     */
    private CollectedTargets collectTargets() {
        Map<TopicIdPartition, ConsumedRetentionTarget> targets = new HashMap<>();
        int withoutTopicId = 0;
        for (TopicPartition topicPartition : enabledPartitions) {
            Optional<Partition> partition = partitionLookup.apply(topicPartition);
            Optional<LogConfig> config = partition
                .flatMap(p -> OptionConverters.toJava(p.leaderLogIfLocal()))
                .map(UnifiedLog::config);
            if (config.isEmpty() || !config.get().consumedRetentionEnabled()) {
                LOG.debug("Unregistering {} because it is no longer a leader partition with consumed retention configured",
                    topicPartition);
                enabledPartitions.remove(topicPartition);
                continue;
            }
            Optional<Uuid> topicId = OptionConverters.toJava(partition.get().topicId());
            if (topicId.isEmpty()) {
                // The share group state is keyed by topic id, so the partition has to stay registered and wait for a
                // later check rather than be unregistered.
                LOG.debug("Skipping consumed retention for {} because its topic id is not known yet", topicPartition);
                withoutTopicId++;
                continue;
            }
            targets.put(new TopicIdPartition(topicId.get(), topicPartition),
                new ConsumedRetentionTarget(partition.get(), config.get().retentionConsumedGroups, config.get().retentionConsumedLagMessages));
        }
        return new CollectedTargets(targets, withoutTopicId);
    }

    /**
     * Reads the consumed position of every configured group and applies the resulting floor. One request is issued per
     * group, covering all partitions that list that group.
     */
    private CompletableFuture<Void> readConsumedPositions(Map<TopicIdPartition, ConsumedRetentionTarget> targets,
                                                          int withoutTopicId) {
        Map<String, List<TopicIdPartition>> partitionsByGroup = new HashMap<>();
        targets.forEach((topicIdPartition, target) -> target.groups().forEach(group ->
            partitionsByGroup.computeIfAbsent(group, ignored -> new ArrayList<>()).add(topicIdPartition)));

        // The lowest consumed position across all groups of a partition, and the partitions for which at least one group
        // has no usable position. The latter must not be advanced at all, because a group that has not consumed the
        // records yet would lose them.
        Map<TopicIdPartition, Long> consumedPositions = new ConcurrentHashMap<>();
        Set<TopicIdPartition> withoutPosition = ConcurrentHashMap.newKeySet();

        List<CompletableFuture<Void>> reads = new ArrayList<>(partitionsByGroup.size());
        partitionsByGroup.forEach((group, groupPartitions) ->
            reads.add(readGroup(group, groupPartitions, consumedPositions, withoutPosition)));

        return CompletableFuture.allOf(reads.toArray(new CompletableFuture<?>[0]))
            .thenRun(() -> advance(targets, consumedPositions, withoutPosition, withoutTopicId));
    }

    private CompletableFuture<Void> readGroup(String group,
                                              List<TopicIdPartition> groupPartitions,
                                              Map<TopicIdPartition, Long> consumedPositions,
                                              Set<TopicIdPartition> withoutPosition) {
        // The persister response carries topic ids and partition indexes but no topic names, so responses are resolved
        // back to the requested TopicIdPartition rather than reconstructed.
        Map<Uuid, Map<Integer, TopicIdPartition>> requested = new HashMap<>();
        groupPartitions.forEach(topicIdPartition -> requested
            .computeIfAbsent(topicIdPartition.topicId(), ignored -> new HashMap<>())
            .put(topicIdPartition.partition(), topicIdPartition));

        ReadShareGroupStateSummaryRequestData request = new ReadShareGroupStateSummaryRequestData().setGroupId(group);
        requested.forEach((topicId, partitions) -> request.topics().add(
            new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(topicId)
                .setPartitions(partitions.keySet().stream()
                    .map(partition -> new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(partition))
                    .toList())));

        return persister.readSummary(ReadShareGroupStateSummaryParameters.from(request))
            .handle((result, error) -> {
                if (error != null || result == null || result.topicsData() == null) {
                    // Without the group state there is no safe floor, so every partition of this group is left alone.
                    // Its records are still deleted by the size and time retention settings.
                    checkFailures.mark();
                    LOG.warn("Could not read the consumed position of group {}, leaving {} partitions to ordinary retention",
                        group, groupPartitions.size(), error);
                    withoutPosition.addAll(groupPartitions);
                } else {
                    mergeGroupResult(group, requested, result, consumedPositions, withoutPosition);
                }
                return null;
            });
    }

    private void mergeGroupResult(String group,
                                  Map<Uuid, Map<Integer, TopicIdPartition>> requested,
                                  ReadShareGroupStateSummaryResult result,
                                  Map<TopicIdPartition, Long> consumedPositions,
                                  Set<TopicIdPartition> withoutPosition) {
        Set<TopicIdPartition> answered = new HashSet<>();
        for (TopicData<PartitionStateSummaryData> topicData : result.topicsData()) {
            Map<Integer, TopicIdPartition> requestedPartitions = requested.get(topicData.topicId());
            if (requestedPartitions == null) {
                continue;
            }
            for (PartitionStateSummaryData partitionData : topicData.partitions()) {
                TopicIdPartition topicIdPartition = requestedPartitions.get(partitionData.partition());
                if (topicIdPartition == null) {
                    continue;
                }
                answered.add(topicIdPartition);
                if (partitionData.errorCode() != Errors.NONE.code()) {
                    LOG.warn("Group {} returned {} for {}, leaving it to ordinary retention",
                        group, Errors.forCode(partitionData.errorCode()), topicIdPartition);
                    withoutPosition.add(topicIdPartition);
                } else if (partitionData.startOffset() == PartitionFactory.UNINITIALIZED_START_OFFSET) {
                    // The group has not consumed anything from this partition yet, so nothing may be deleted.
                    LOG.debug("Group {} has no consumed position for {} yet", group, topicIdPartition);
                    withoutPosition.add(topicIdPartition);
                } else {
                    consumedPositions.merge(topicIdPartition, partitionData.startOffset(), Math::min);
                }
            }
        }
        // A partition the group was asked about but did not answer for has no known position either.
        requested.values().forEach(partitions -> partitions.values().forEach(topicIdPartition -> {
            if (!answered.contains(topicIdPartition)) {
                LOG.warn("Group {} returned no consumed position for {}, leaving it to ordinary retention", group, topicIdPartition);
                withoutPosition.add(topicIdPartition);
            }
        }));
    }

    private void advance(Map<TopicIdPartition, ConsumedRetentionTarget> targets,
                         Map<TopicIdPartition, Long> consumedPositions,
                         Set<TopicIdPartition> withoutPosition,
                         int withoutTopicId) {
        int skipped = withoutTopicId;
        for (Map.Entry<TopicIdPartition, ConsumedRetentionTarget> entry : targets.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            ConsumedRetentionTarget target = entry.getValue();
            Long consumedPosition = consumedPositions.get(topicIdPartition);
            if (withoutPosition.contains(topicIdPartition) || consumedPosition == null) {
                skipped++;
                continue;
            }
            // The lag keeps a replay window below the consumed position. It also breaks any feedback between
            // logStartOffset and a share partition start offset that follows it, because the floor then stays strictly
            // below the position it was derived from.
            if (target.lagMessages() >= consumedPosition) {
                continue;
            }
            long floor = consumedPosition - target.lagMessages();
            try {
                if (target.partition().advanceLogStartOffsetForConsumedRetention(floor)) {
                    LOG.debug("Advanced logStartOffset of {} to {} because the group state reports consumption up to {}",
                        topicIdPartition, floor, consumedPosition);
                }
            } catch (Throwable e) {
                checkFailures.mark();
                LOG.warn("Could not advance logStartOffset of {} to {}", topicIdPartition, floor, e);
            }
        }
        skippedPartitionCount.set(skipped);
    }

    private record ConsumedRetentionTarget(Partition partition, List<String> groups, long lagMessages) {
    }

    /**
     * What one pass over the registered partitions found: the partitions a request can be issued for, and how many are
     * still waiting for their topic id and are therefore left to ordinary retention by this check.
     */
    private record CollectedTargets(Map<TopicIdPartition, ConsumedRetentionTarget> targets, int withoutTopicId) {
    }
}
