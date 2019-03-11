/*
 *  Copyright 2018 acoder2013
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http:www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.song.yama.common.storage.impl;

import com.song.yama.common.storage.KeyValueStorage;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ChecksumType;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

@Slf4j
public class RocksDBKeyValueStorage implements KeyValueStorage {

    private final RocksDB rocksDB;

    private final WriteOptions optionSync;
    private final WriteOptions optionDontSync;

    private final ReadOptions optionCache;
    private final ReadOptions optionDontCache;

    private final WriteBatch emptyBatch;

    public RocksDBKeyValueStorage(String path, boolean readOnly) throws IOException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable throwable) {
            throw new IOException("Failed to load RocksDB jni library.", throwable);
        }

        this.optionSync = new WriteOptions();
        this.optionDontSync = new WriteOptions();
        this.optionCache = new ReadOptions();
        this.optionDontCache = new ReadOptions();
        this.emptyBatch = new WriteBatch();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.LZ4_COMPRESSION);
            options.setWriteBufferSize(64 * 1024 * 1024);
            options.setMaxWriteBufferNumber(4);
            options.setLevelZeroFileNumCompactionTrigger(4);
            options.setMaxBytesForLevelBase(256 * 1024 * 1024);
            options.setMaxBackgroundCompactions(16);
            options.setMaxBackgroundFlushes(16);
            options.setIncreaseParallelism(32);
            options.setMaxTotalWalSize(512 * 1024 * 1024);
            options.setMaxOpenFiles(-1);
            options.setTargetFileSizeBase(64 * 1024 * 1024);
            options.setDeleteObsoleteFilesPeriodMicros(TimeUnit.HOURS.toMicros(1));

            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setBlockSize(64 * 1024);
            tableOptions.setBlockCacheSize(256 * 1024 * 1024);
            tableOptions.setFormatVersion(2);
            tableOptions.setChecksumType(ChecksumType.kxxHash);
            tableOptions.setFilter(new BloomFilter(10, false));

            // Options best suited for HDDs
            tableOptions.setCacheIndexAndFilterBlocks(true);
            options.setLevelCompactionDynamicLevelBytes(true);

            options.setTableFormatConfig(tableOptions);

            options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);

            try {
                if (readOnly) {
                    rocksDB = RocksDB.openReadOnly(options, path);
                } else {
                    rocksDB = RocksDB.open(options, path);
                }
            } catch (RocksDBException e) {
                throw new IOException("Failed to open RocksDB.", e);
            }
        }

        optionSync.setSync(true);
        optionDontSync.setSync(false);

        optionCache.setFillCache(true);
        optionDontCache.setFillCache(false);

    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            this.rocksDB.put(optionSync, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to put into RocksDB.", e);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        try {
            return this.rocksDB.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get from RocksDB.", e);
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            this.rocksDB.delete(optionDontSync, key);
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete from RocksDB.", e);
        }
    }

    @Override
    public void sync() throws IOException {
        try {
            this.rocksDB.write(optionSync, emptyBatch);
        } catch (RocksDBException e) {
            throw new IOException("Failed to sync RocksDB.", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.rocksDB.close();
        optionSync.close();
        optionDontSync.close();
        optionCache.close();
        optionDontCache.close();
        emptyBatch.close();
    }
}
