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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionStateSummaryData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryParameters;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumedRetentionManagerTest {

    private static final String GROUP = "share-group";
    private static final String OTHER_GROUP = "other-share-group";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("consumed-retention-topic", 0);

    private ConsumedRetentionManager manager;

    @AfterEach
    public void tearDown() {
        if (manager != null) {
            manager.close();
            manager = null;
        }
    }

    @Test
    public void testAdvancesLogStartOffsetToConsumedPositionMinusLag() {
        Partition partition = partition(consumedRetentionConfig(10L, GROUP));
        Persister persister = persisterReturning(Map.of(GROUP, summary(100L)));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition).advanceLogStartOffsetForConsumedRetention(90L);
    }

    @Test
    public void testAdvancesToTheLowestConsumedPositionAcrossGroups() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP, OTHER_GROUP));
        Persister persister = persisterReturning(Map.of(GROUP, summary(100L), OTHER_GROUP, summary(40L)));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition).advanceLogStartOffsetForConsumedRetention(40L);
    }

    @Test
    public void testDoesNotAdvanceWhenOneGroupHasNotConsumedAnythingYet() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP, OTHER_GROUP));
        Persister persister = persisterReturning(Map.of(
            GROUP, summary(100L),
            OTHER_GROUP, summary(PartitionFactory.UNINITIALIZED_START_OFFSET)));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testDoesNotAdvanceWhenOneGroupReturnsAnError() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP, OTHER_GROUP));
        Persister persister = persisterReturning(Map.of(
            GROUP, summary(100L),
            OTHER_GROUP, errorSummary(Errors.COORDINATOR_NOT_AVAILABLE)));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testDoesNotAdvanceWhenTheGroupStateCannotBeRead() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP));
        Persister persister = mock(Persister.class);
        when(persister.readSummary(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testDoesNotAdvanceWhenTheGroupDoesNotAnswerForThePartition() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP));
        Persister persister = mock(Persister.class);
        when(persister.readSummary(any())).thenReturn(CompletableFuture.completedFuture(
            new ReadShareGroupStateSummaryResult.Builder().setTopicsData(List.of()).build()));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testIgnoresPartitionsWithoutConsumedRetentionConfigured() {
        Partition partition = partition(new LogConfig(new Properties()));
        Persister persister = mock(Persister.class);

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(persister, never()).readSummary(any());
        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testIgnoresPartitionWhoseTopicIdIsNotKnownYet() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP));
        when(partition.topicId()).thenReturn(OptionConverters.toScala(Optional.<Uuid>empty()));
        Persister persister = mock(Persister.class);

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(persister, never()).readSummary(any());
    }

    @Test
    public void testDoesNotAdvanceWhenTheLagCoversTheWholeConsumedRange() {
        Partition partition = partition(consumedRetentionConfig(100L, GROUP));
        Persister persister = persisterReturning(Map.of(GROUP, summary(100L)));

        newManager(persister, partition).maybeAdvanceLogStartOffsets();

        verify(partition, never()).advanceLogStartOffsetForConsumedRetention(anyLong());
    }

    @Test
    public void testDoesNotStartAnOverlappingCheck() {
        Partition partition = partition(consumedRetentionConfig(0L, GROUP));
        CompletableFuture<ReadShareGroupStateSummaryResult> pending = new CompletableFuture<>();
        Persister persister = mock(Persister.class);
        when(persister.readSummary(any())).thenReturn(pending);

        ConsumedRetentionManager consumedRetentionManager = newManager(persister, partition);
        consumedRetentionManager.maybeAdvanceLogStartOffsets();
        consumedRetentionManager.maybeAdvanceLogStartOffsets();

        verify(persister, times(1)).readSummary(any());

        // Once the pending read completes the next check runs again.
        pending.complete(new ReadShareGroupStateSummaryResult.Builder()
            .setTopicsData(List.of(new TopicData<>(TOPIC_ID, List.of(summary(100L))))).build());
        consumedRetentionManager.maybeAdvanceLogStartOffsets();

        verify(persister, times(2)).readSummary(any());
    }

    /**
     * A share partition adopts {@code logStartOffset} as its start offset when it is initialized, so the position this
     * component reads can follow the offset it just moved. The lag keeps the floor strictly below the position it was
     * derived from, so the two cannot chase each other: the log start offset settles at the real consumed position
     * minus the lag instead of walking forward on its own.
     */
    @Test
    public void testFloorDoesNotFeedBackIntoTheConsumedPosition() {
        long lag = 10L;
        long consumedPosition = 100L;
        AtomicLong logStartOffset = new AtomicLong(0L);
        List<Long> requestedFloors = new ArrayList<>();

        Partition partition = partition(consumedRetentionConfig(lag, GROUP));
        when(partition.advanceLogStartOffsetForConsumedRetention(anyLong())).thenAnswer(invocation -> {
            long floor = invocation.getArgument(0);
            requestedFloors.add(floor);
            // Mirrors Partition: the log start offset only ever moves forward.
            if (floor <= logStartOffset.get()) {
                return false;
            }
            logStartOffset.set(floor);
            return true;
        });

        Persister persister = mock(Persister.class);
        // Mirrors SharePartition, whose start offset is raised to logStartOffset when it falls behind the log.
        when(persister.readSummary(any())).thenAnswer(invocation -> CompletableFuture.completedFuture(
            new ReadShareGroupStateSummaryResult.Builder()
                .setTopicsData(List.of(new TopicData<>(TOPIC_ID,
                    List.of(summary(Math.max(consumedPosition, logStartOffset.get()))))))
                .build()));

        ConsumedRetentionManager consumedRetentionManager = newManager(persister, partition);
        for (int i = 0; i < 5; i++) {
            consumedRetentionManager.maybeAdvanceLogStartOffsets();
        }

        assertEquals(consumedPosition - lag, logStartOffset.get());
        assertTrue(requestedFloors.stream().allMatch(floor -> floor <= consumedPosition - lag),
            "no requested floor may exceed the consumed position minus the lag, but got " + requestedFloors);
    }

    private ConsumedRetentionManager newManager(Persister persister, Partition... partitions) {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.leaderPartitionsIterator())
            .thenAnswer(invocation -> CollectionConverters.asScala(List.of(partitions).iterator()));
        manager = new ConsumedRetentionManager(replicaManager, persister, mock(Scheduler.class), 30_000L);
        return manager;
    }

    private static Partition partition(LogConfig logConfig) {
        UnifiedLog log = mock(UnifiedLog.class);
        when(log.config()).thenReturn(logConfig);

        Partition partition = mock(Partition.class);
        when(partition.topicPartition()).thenReturn(TOPIC_PARTITION);
        when(partition.topicId()).thenReturn(OptionConverters.toScala(Optional.of(TOPIC_ID)));
        when(partition.leaderLogIfLocal()).thenReturn(OptionConverters.toScala(Optional.of(log)));
        return partition;
    }

    private static LogConfig consumedRetentionConfig(long lagMessages, String... groups) {
        Properties props = new Properties();
        props.put(TopicConfig.RETENTION_CONSUMED_GROUPS_CONFIG, String.join(",", groups));
        props.put(TopicConfig.RETENTION_CONSUMED_LAG_MESSAGES_CONFIG, Long.toString(lagMessages));
        return new LogConfig(props);
    }

    /**
     * A persister that answers each group with its own summary, so a test can give different groups different positions.
     */
    private static Persister persisterReturning(Map<String, PartitionStateSummaryData> summaryByGroup) {
        Persister persister = mock(Persister.class);
        when(persister.readSummary(any())).thenAnswer(invocation -> {
            ReadShareGroupStateSummaryParameters parameters = invocation.getArgument(0);
            String group = parameters.groupTopicPartitionData().groupId();
            PartitionStateSummaryData summary = summaryByGroup.get(group);
            return CompletableFuture.completedFuture(new ReadShareGroupStateSummaryResult.Builder()
                .setTopicsData(List.of(new TopicData<>(TOPIC_ID, List.of(summary))))
                .build());
        });
        return persister;
    }

    private static PartitionStateSummaryData summary(long startOffset) {
        return PartitionFactory.newPartitionStateSummaryData(TOPIC_PARTITION.partition(),
            PartitionFactory.DEFAULT_STATE_EPOCH, startOffset, 0, PartitionFactory.DEFAULT_LEADER_EPOCH,
            Errors.NONE.code(), Errors.NONE.message());
    }

    private static PartitionStateSummaryData errorSummary(Errors error) {
        return PartitionFactory.newPartitionStateSummaryData(TOPIC_PARTITION.partition(),
            PartitionFactory.DEFAULT_STATE_EPOCH, PartitionFactory.UNINITIALIZED_START_OFFSET, 0,
            PartitionFactory.DEFAULT_LEADER_EPOCH, error.code(), error.message());
    }
}
