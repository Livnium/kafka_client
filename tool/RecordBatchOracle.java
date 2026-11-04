// Copyright Livnium.
// SPDX-License-Identifier: Apache-2.0
// Project test harness using separately installed Apache Kafka APIs.
// No Kafka implementation code or JAR is bundled by this helper.

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/** Broker-free Kafka 4.0.2 record-batch oracle with fixed, synthetic inputs. */
public final class RecordBatchOracle {
    private static String bytes(ByteBuffer buffer) {
        if (buffer == null) return "null";
        byte[] data = new byte[buffer.remaining()];
        buffer.duplicate().get(data);
        return bytes(data);
    }

    private static String bytes(byte[] data) {
        return data == null ? "null" : "\"" + Base64.getEncoder().encodeToString(data) + "\"";
    }

    private static void output(String name, MemoryRecords records) {
        StringBuilder expected = new StringBuilder();
        long lastOffset = -1;
        boolean firstRecord = true;
        for (var batch : records.batches()) {
            batch.ensureValid();
            lastOffset = batch.lastOffset();
            for (var record : batch) {
                record.ensureValid();
                if (batch.isControlBatch()) continue;
                if (!firstRecord) expected.append(',');
                firstRecord = false;
                // Decimal strings preserve all signed INT64 values in JSON.
                expected.append("{\"offset\":\"").append(record.offset())
                    .append("\",\"timestamp\":\"").append(record.timestamp())
                    .append("\",\"key\":").append(bytes(record.key()))
                    .append(",\"value\":").append(bytes(record.value()))
                    .append(",\"headers\":[");
                boolean firstHeader = true;
                for (Header header : record.headers()) {
                    if (!firstHeader) expected.append(',');
                    firstHeader = false;
                    expected.append('[')
                        .append(bytes(header.key().getBytes(StandardCharsets.UTF_8)))
                        .append(',').append(bytes(header.value())).append(']');
                }
                expected.append("]}");
            }
        }
        // Case names are fixed ASCII literals, not caller-supplied JSON text.
        System.out.println("{\"name\":\"" + name + "\",\"wire\":" + bytes(records.buffer())
            + ",\"lastBatchOffset\":\"" + lastOffset + "\",\"records\":[" + expected + "]}");
    }

    private static MemoryRecordsBuilder builder(long baseOffset, TimestampType type,
                                                long logAppendTime, long deleteHorizon) {
        return new MemoryRecordsBuilder(
            new ByteBufferOutputStream(ByteBuffer.allocate(4096)), (byte) 2,
            Compression.NONE, type, baseOffset, logAppendTime,
            -1, (short) -1, -1, false, false, 7, 4096, deleteHorizon);
    }

    public static void main(String[] args) {
        if (args.length != 0) throw new IllegalArgumentException("No arguments supported");

        var batch = builder(100, TimestampType.CREATE_TIME, -1, -1);
        batch.appendWithOffset(100, 1700000000100L, new byte[]{0, -1}, new byte[]{1, 2},
            new Header[]{new RecordHeader("π🙂", null), new RecordHeader("π🙂", new byte[0]),
                new RecordHeader("", new byte[]{-1})});
        batch.appendWithOffset(101, 1700000000000L, (byte[]) null, (byte[]) null);
        batch.appendWithOffset(102, 1700000000200L, new byte[0], new byte[0]);
        output("create-time-binary-headers-negative-delta", batch.build());

        batch = builder(200, TimestampType.LOG_APPEND_TIME, 1700000999000L, -1);
        batch.appendWithOffset(200, 1700000000000L, (byte[]) null, new byte[]{1});
        batch.appendWithOffset(201, 1700000000010L, (byte[]) null, new byte[]{2});
        output("log-append-time", batch.build());

        batch = builder(300, TimestampType.CREATE_TIME, -1, -1);
        batch.appendWithOffset(305, 1700000000000L, new byte[]{1}, new byte[]{2});
        batch.appendWithOffset(310, 1700000000100L, new byte[]{3}, new byte[]{4});
        batch.overrideLastOffset(320);
        output("compacted-sparse-prefix-tail", batch.build());

        batch = builder(400, TimestampType.CREATE_TIME, -1, 1700100000000L);
        batch.appendWithOffset(405, 1700000000000L, new byte[]{1}, null);
        batch.appendWithOffset(410, 1700000000100L, new byte[]{2}, new byte[]{3});
        batch.overrideLastOffset(420);
        output("delete-horizon-sparse", batch.build());

        ByteBuffer empty = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(empty, (byte) 2, 55, (short) 3, 7, 500, 525,
            3, TimestampType.CREATE_TIME, 1700000000000L, false, false);
        empty.flip();
        output("compacted-empty", MemoryRecords.readableRecords(empty));

        output("commit-control", MemoryRecords.withEndTransactionMarker(
            600, 1700000000000L, 1, 55, (short) 3,
            new EndTransactionMarker(ControlRecordType.COMMIT, 3)));
        output("abort-control", MemoryRecords.withEndTransactionMarker(
            601, 1700000000000L, 1, 55, (short) 3,
            new EndTransactionMarker(ControlRecordType.ABORT, 3)));
        output("transactional-visible-read-uncommitted", MemoryRecords.withTransactionalRecords(
            700, Compression.NONE, 55, (short) 3, 7, 1,
            new SimpleRecord(1700000000000L, new byte[]{1}, new byte[]{2})));

        batch = builder(9007199254740993L, TimestampType.CREATE_TIME, -1, -1);
        batch.appendWithOffset(9007199254740993L, 9007199254740993L,
            (byte[]) null, new byte[]{1});
        output("above-js-safe-integer", batch.build());

        batch = builder(800, TimestampType.CREATE_TIME, -1, -1);
        batch.appendWithOffset(800, -1, (byte[]) null, new byte[]{1});
        output("no-timestamp-sentinel", batch.build());
    }
}
