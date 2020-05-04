import static org.junit.Assert.assertTrue;

import cache.store.RocksDBRepositoryImpl;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBRepositoryImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBRepositoryImplTest.class);
  private static final File DB_DIR = FileUtils.getTempDirectory();
  private static final String ENTRY = "Some text information";

  @Test
  public void testSave() throws IOException {

    RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(DB_DIR.toString(), true);
    rocksDBRepository.save("Key1", new ByteArrayInputStream(ENTRY.getBytes()));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

    Assert.assertEquals(ENTRY, new String(outputStream.toByteArray()));
  }

  @Test
  public void testSaveWhenOverwriteExistingModeDisabled() throws IOException {
    // Disabling overwriteExisting mode
    RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(DB_DIR.getPath(), false);
    rocksDBRepository.save("Key1", new ByteArrayInputStream(ENTRY.getBytes()));

    // Save new entry with existing key
    rocksDBRepository.save("Key1", new ByteArrayInputStream("New text".getBytes()));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

    Assert.assertEquals(
        "The previous entry should not be overwritten, because overwriteExisting mode disabled.",
        ENTRY,
        new String(outputStream.toByteArray()));
  }

  @Test
  public void testDelete() throws IOException {

    RocksDBRepositoryImpl rocksDBRepository = new RocksDBRepositoryImpl(DB_DIR.toString(), true);
    rocksDBRepository.save("Key1", new ByteArrayInputStream(ENTRY.getBytes()));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.copy(rocksDBRepository.find("Key1"), outputStream);

    rocksDBRepository.delete("Key1");

    Assert.assertNull(rocksDBRepository.find("Key1"));
  }

  // ********************* Multithreading tests *******************************

  @Test
  public void testConcurrentMultithreadedSave() throws InterruptedException {
    List<Runnable> runnables =
        Stream.generate(
                () ->
                    (Runnable)
                        (() -> {
                          RocksDBRepositoryImpl rocksDBRepository =
                              new RocksDBRepositoryImpl(DB_DIR.toString(), true);
                          String key = UUID.randomUUID().toString();
                          String entry = ENTRY + key;
                          rocksDBRepository.save(key, new ByteArrayInputStream(entry.getBytes()));

                          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                          try {
                            IOUtils.copy(rocksDBRepository.find(key), outputStream);
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }

                          Assert.assertEquals(entry, new String(outputStream.toByteArray()));
                        }))
            .limit(100)
            .collect(Collectors.toList());

    executeConcurrent(runnables);
  }

  @Test
  public void testConcurrentMultithreadedDelete() throws InterruptedException {

    List<Runnable> runnables =
        Stream.generate(
                () ->
                    (Runnable)
                        (() -> {
                          RocksDBRepositoryImpl rocksDBRepository =
                              new RocksDBRepositoryImpl(DB_DIR.toString(), true);
                          String key = UUID.randomUUID().toString();
                          String entry = ENTRY + key;

                          rocksDBRepository.save(key, new ByteArrayInputStream(entry.getBytes()));

                          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                          try {
                            IOUtils.copy(rocksDBRepository.find(key), outputStream);
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }

                          rocksDBRepository.delete(key);

                          Assert.assertNull(rocksDBRepository.find(key));
                        }))
            .limit(100)
            .collect(Collectors.toList());

    executeConcurrent(runnables);
  }

  /** It allows to start all threads at once and monitor process of execution */
  private static void executeConcurrent(final List<? extends Runnable> threads)
      throws InterruptedException {
    final int numThreads = threads.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());

    final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
    final CountDownLatch afterInitBlocker = new CountDownLatch(1);
    final CountDownLatch allDone = new CountDownLatch(numThreads);

    for (final Runnable runnable : threads) {
      new Thread(
              () -> {
                allExecutorThreadsReady.countDown();
                try {
                  afterInitBlocker.await();
                  LOG.info("Thread: [{}] start", Thread.currentThread().getName());
                  runnable.run();
                } catch (Exception e) {
                  exceptions.add(e);
                } finally {
                  LOG.info("Thread: [{}] finished", Thread.currentThread().getName());
                  allDone.countDown();
                }
              })
          .start();
    }
    allExecutorThreadsReady.await();
    LOG.info("Threads ready");
    afterInitBlocker.countDown();
    allDone.await();
    LOG.info("Threads complete");
    assertTrue(exceptions.isEmpty());
  }
}
