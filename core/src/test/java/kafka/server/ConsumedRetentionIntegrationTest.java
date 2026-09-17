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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves end to end that records consumed by the configured share group are deleted from the local log because they
 * were consumed. The topic is created with retention settings that can never delete anything, so any deletion observed
 * here can only come from the consumption-driven log start offset.
 */
@Timeout(300)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.min.isr", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "share.coordinator.state.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
        @ClusterConfigProperty(key = "log.retention.check.interval.ms", value = "200"),
        @ClusterConfigProperty(key = "log.retention.consumed.check.interval.ms", value = "200")
    }
)
public class ConsumedRetentionIntegrationTest {

    private static final String TOPIC = "consumed-retention-topic";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
    private static final String GROUP = "consumed-retention-group";

    private static final int RECORD_COUNT = 60;
    private static final int RECORD_SIZE = 64 * 1024;
    private static final int SEGMENT_BYTES = 1024 * 1024;
    private static final long LAG_MESSAGES = 10L;
    private static final int ACKNOWLEDGED_PREFIX = 20;

    private static final Pattern SEGMENT_FILE_PATTERN =
        Pattern.compile("\\d+" + Pattern.quote(LogFileUtils.LOG_FILE_SUFFIX) + ".*");

    @ClusterTest
    public void testConsumedRecordsAreDeletedFromDisk(ClusterInstance cluster) throws Exception {
        createTopic(cluster, LAG_MESSAGES);
        alterShareAutoOffsetResetToEarliest(cluster);
        produceRecords(cluster);

        UnifiedLog log = log(cluster);
        assertTrue(log.logSegments().size() > 1, "the topic must span several segments for this test to be meaningful");

        // The group has not consumed anything yet, so the check has to leave the partition to ordinary retention. A
        // non-zero skipped count proves that a check ran to completion and deliberately did not advance the offset.
        TestUtils.waitForCondition(() -> gauge("SkippedPartitionCount") == 1,
            "the consumed retention check did not report the partition as skipped");
        assertEquals(0L, log.logStartOffset(), "nothing may be deleted before the group has consumed anything");

        consumeAndAcknowledge(cluster, RECORD_COUNT);

        long expectedLogStartOffset = RECORD_COUNT - LAG_MESSAGES;
        TestUtils.waitForCondition(() -> log.logStartOffset() == expectedLogStartOffset,
            () -> "the log start offset stopped at " + log.logStartOffset() + " instead of " + expectedLogStartOffset);

        long firstSegmentBaseOffset = log.logSegments().get(0).baseOffset();
        assertTrue(firstSegmentBaseOffset > 0, "no segment was deleted, the log still starts at offset 0");
        TestUtils.waitForCondition(() -> baseOffsetsOnDisk(log).stream().allMatch(base -> base >= firstSegmentBaseOffset),
            () -> "the consumed segments are still on disk: " + baseOffsetsOnDisk(log));
    }

    @ClusterTest
    public void testUnacknowledgedRecordsAreRetainedUntilTheyAreAcknowledged(ClusterInstance cluster) throws Exception {
        createTopic(cluster, 0L);
        alterShareAutoOffsetResetToEarliest(cluster);
        produceRecords(cluster);

        consumeAndAcknowledge(cluster, ACKNOWLEDGED_PREFIX);

        UnifiedLog log = log(cluster);
        TestUtils.waitForCondition(() -> log.logStartOffset() == ACKNOWLEDGED_PREFIX,
            () -> "the log start offset stopped at " + log.logStartOffset() + " instead of " + ACKNOWLEDGED_PREFIX);
        assertEquals(RECORD_COUNT, log.logEndOffset(),
            "the records the group released instead of acknowledging must still be in the log");

        consumeAndAcknowledge(cluster, RECORD_COUNT - ACKNOWLEDGED_PREFIX);

        TestUtils.waitForCondition(() -> log.logStartOffset() == RECORD_COUNT,
            () -> "the log start offset stopped at " + log.logStartOffset() + " instead of " + RECORD_COUNT);
    }

    private void createTopic(ClusterInstance cluster, long lagMessages) throws InterruptedException {
        cluster.createTopic(TOPIC, 1, (short) 1, Map.of(
            TopicConfig.RETENTION_CONSUMED_GROUPS_CONFIG, GROUP,
            TopicConfig.RETENTION_CONSUMED_LAG_MESSAGES_CONFIG, Long.toString(lagMessages),
            TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(SEGMENT_BYTES),
            // Neither time nor size retention can ever delete a record of this topic.
            TopicConfig.RETENTION_MS_CONFIG, "-1",
            TopicConfig.RETENTION_BYTES_CONFIG, "-1",
            TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "1"));
    }

    private void alterShareAutoOffsetResetToEarliest(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            admin.incrementalAlterConfigs(Map.of(
                new ConfigResource(ConfigResource.Type.GROUP, GROUP),
                List.of(new AlterConfigOp(
                    new ConfigEntry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest"), AlterConfigOp.OpType.SET))
            )).all().get();
        }
    }

    private void produceRecords(ClusterInstance cluster) throws Exception {
        byte[] value = new byte[RECORD_SIZE];
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                producer.send(new ProducerRecord<>(TOPIC, TOPIC_PARTITION.partition(), null, value));
            }
            producer.flush();
        }
        UnifiedLog log = log(cluster);
        TestUtils.waitForCondition(() -> log.highWatermark() == RECORD_COUNT,
            () -> "the high watermark stopped at " + log.highWatermark() + " instead of " + RECORD_COUNT);
    }

    /**
     * Acknowledges the first {@code count} records and releases the rest, so that the share partition start offset
     * settles exactly at the first record the group did not acknowledge.
     */
    private void consumeAndAcknowledge(ClusterInstance cluster, int count) throws Exception {
        Map<String, Object> configs = Map.of(
            ConsumerConfig.GROUP_ID_CONFIG, GROUP,
            ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        try (ShareConsumer<byte[], byte[]> shareConsumer = cluster.shareConsumer(configs)) {
            shareConsumer.subscribe(Set.of(TOPIC));
            int acknowledged = 0;
            long deadline = System.currentTimeMillis() + TestUtils.DEFAULT_MAX_WAIT_MS;
            while (acknowledged < count) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("the group acknowledged only " + acknowledged + " of " + count + " records");
                }
                for (ConsumerRecord<byte[], byte[]> record : shareConsumer.poll(Duration.ofSeconds(1))) {
                    if (acknowledged < count) {
                        shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        acknowledged++;
                    } else {
                        shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                    }
                }
            }
            shareConsumer.commitSync();
        }
    }

    private UnifiedLog log(ClusterInstance cluster) {
        KafkaBroker broker = cluster.brokers().values().iterator().next();
        return broker.logManager().getLog(TOPIC_PARTITION).orElseThrow(
            () -> new AssertionError("the broker does not host " + TOPIC_PARTITION));
    }

    /**
     * The base offsets of the segment files in the partition directory, including the ones that are already renamed for
     * deletion, so that a segment which is only scheduled for deletion does not count as removed.
     */
    private static List<Long> baseOffsetsOnDisk(UnifiedLog log) {
        List<Long> baseOffsets = new ArrayList<>();
        for (File file : Objects.requireNonNull(log.dir().listFiles())) {
            if (SEGMENT_FILE_PATTERN.matcher(file.getName()).matches()) {
                baseOffsets.add(LogFileUtils.offsetFromFileName(file.getName()));
            }
        }
        return baseOffsets;
    }

    private static long gauge(String name) {
        for (Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            if (entry.getKey().getMBeanName().equals("kafka.log:type=ConsumedRetentionManager,name=" + name)) {
                return ((Number) ((Gauge<?>) entry.getValue()).value()).longValue();
            }
        }
        return -1L;
    }
}
