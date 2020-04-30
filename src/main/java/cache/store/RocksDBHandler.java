package cache.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * It handles RocksDB instances and keeps them in thread-safe map by their path to the database.
 * <br>
 * It provides default {@link Options} which will be used for all new RocksDB instances, {@link
 * #defaultOptions} can be changed using {@link #setDefaultOptions(Options)}. Only one instance of
 * RocksDBHandler can be created(singleton) and it keeps only one instance of {@link RocksDB} per
 * path to the database to prevent blocking issue(https://rocksdb.org/blog/2014/05/14/lock.html,
 * https://github.com/facebook/rocksdb/issues/908).
 */
public class RocksDBHandler {
  private static final Logger LOG = LogManager.getLogger(RocksDBHandler.class);
  private static final RocksDBHandler ROCK_DB_HANDLER = new RocksDBHandler();
  private final Map<String, RocksDB> rocksDBByPath = new ConcurrentHashMap<>();
  private Options defaultOptions = new Options().setCreateIfMissing(true);

  private RocksDBHandler() {
    RocksDB.loadLibrary();
  }

  public static RocksDBHandler getInstance() {
    return ROCK_DB_HANDLER;
  }

  public synchronized RocksDB getRocksDB(String path) {
    if (!rocksDBByPath.containsKey(path)) {
      try {
        LOG.info("Opening a RocksDB instance with a path:[{}] to the database", path);
        rocksDBByPath.put(path, RocksDB.open(defaultOptions(), path));
      } catch (RocksDBException e) {
        LOG.error(
            "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
            e.getCause(),
            e.getMessage(),
            e.getStackTrace());
      }
    }
    return rocksDBByPath.get(path);
  }

  public Options defaultOptions() {
    return this.defaultOptions;
  }

  public void setDefaultOptions(Options options) {
    defaultOptions.close();
    this.defaultOptions = options;
  }
}
