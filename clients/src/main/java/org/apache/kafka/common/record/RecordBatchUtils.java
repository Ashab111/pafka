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
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import java.util.ArrayList;
import java.util.List;

public class RecordBatchUtils {
    public static boolean shouldSkipForMagic(RecordBatch batch, byte toMagic) {
        return toMagic < RecordBatch.MAGIC_VALUE_V2 && batch.isControlBatch();
    }

    public static void validateCompressionType(RecordBatch batch) {
        if (batch.compressionType() == CompressionType.ZSTD) {
            throw new UnsupportedCompressionTypeException("Down-conversion of zstandard-compressed batches is not supported");
        }
    }

    public static boolean needsDownConversion(RecordBatch batch, byte toMagic) {
        return batch.magic() > toMagic;
    }

    public static List<Record> getRecordsForDownConversion(RecordBatch batch, byte toMagic, long firstOffset) {
        List<Record> records = new ArrayList<>();
        for (Record record : batch) {
            if (toMagic > RecordBatch.MAGIC_VALUE_V1 || batch.isCompressed() || record.offset() >= firstOffset) {
                records.add(record);
            }
        }
        return records;
    }

    public static long calculateBaseOffset(RecordBatch batch, byte toMagic, List<Record> records) {
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && toMagic >= RecordBatch.MAGIC_VALUE_V2) {
            return batch.baseOffset();
        } else {
            return records.get(0).offset();
        }
    }

    public static int estimateSizeInBytes(RecordBatch batch, byte toMagic, List<Record> records) {
        long baseOffset = calculateBaseOffset(batch, toMagic, records);
        return AbstractRecords.estimateSizeInBytes(toMagic, baseOffset, batch.compressionType(), records);
    }
}
