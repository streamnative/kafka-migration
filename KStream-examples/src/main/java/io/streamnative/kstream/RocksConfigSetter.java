package io.streamnative.kstream;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Configuration setter to inject our specific configuration into RocksDB.
 *
 * The default rocks and kafka configurations are too aggressive for the smallish instance types
 * we're using
 */
public class RocksConfigSetter implements RocksDBConfigSetter {
    private static final Logger logger = LoggerFactory.getLogger(RocksConfigSetter.class);
        // The settings below affect the memory usage of RocksDB
        // To estimate the required memory, use the following formula:
        //
        // StoreMemory = WRITE_BUFFER_SIZE * WRITE_BUFFER_COUNT (default 3) +  BLOCK_CACHE_SIZE
        // TotalMemory = StoreMemory * PartitionCount * WindowStoreSegments (3)
        // See: https://docs.confluent.io/current/streams/sizing.html#troubleshooting
    private static final long BLOCK_SIZE = 32 * 1024L;
    private static final int STATS_DUMP_SECS = 60;
    // TODO: Turning this on results in a SIGSEGV -- find out why
    private static final boolean ENABLE_ROCKSDB_LOGGING = false;

    public RocksConfigSetter() {
        logger.info("Customized RocksDB Config Setter");
    }

    /**
     * Overrides Kafka Streams / RocksDB settings with those appropriate to our workload
     *
     * @param s       Store name
     * @param options Existing rocksDB options
     * @param map     Streams config
     */
    @Override
    public void setConfig(String s, Options options, Map<String, Object> map) {
        logger.info("Set RocksDB configuration for store " + s);
        long writeBufferSize = 64 * 1024 * 1024;
        logger.info("Write buffer size: " + writeBufferSize);
        options.setWriteBufferSize(writeBufferSize);
        if (ENABLE_ROCKSDB_LOGGING) {
            logger.info("Setting Rocksdb stats dump frequency: " + STATS_DUMP_SECS);
            options.setStatsDumpPeriodSec(STATS_DUMP_SECS);
            logger.info("Creating rocksdb logger");
            options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
            // Have the logger call back into Java
            final org.rocksdb.Logger rocksDbLogger = new org.rocksdb.Logger(options) {
                @Override
                protected void log(InfoLogLevel infoLogLevel, String s) {
                    // The messages out of RocksDB are a mess so distinguish them with a prefix
                    s = "ROCKSDB: " + s;
                    switch (infoLogLevel) {
                        case DEBUG_LEVEL:
                            logger.debug(s);
                            break;
                        case ERROR_LEVEL:
                        case FATAL_LEVEL:
                            logger.error(s);
                            break;
                        case INFO_LEVEL:
                            logger.info(s);
                            break;
                        case WARN_LEVEL:
                            logger.warn(s);
                            break;
                        default:
                            logger.debug(s);
                    }
                }
            };
            options.setLogger(rocksDbLogger);
        }
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        long blockCacheBytes = 16 * 1024L * 1024L;
        logger.info("Block cache size: " + blockCacheBytes);
        tableConfig.setBlockCacheSize(blockCacheBytes);
        logger.info("Block size: " + BLOCK_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);

        // options.setTableFormatConfig(tableConfig);
    }
}