/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends MetaStoreCompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private long cleanerCheckInterval = 0;

  private ReplChangeManager replChangeManager;
  private ExecutorService executorService;

  private final int KEEPALIVE_SECONDS = 60;
  private Lock rsLock = new ReentrantLock();

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws Exception {
    super.init(stop, looped);
    replChangeManager = ReplChangeManager.getInstance(conf);
    int coreThreads = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_CORE_CLEANER_THREADS);
    int maxThreads = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_MAX_CLEANER_THREADS);
    assert coreThreads > 0;
    assert maxThreads > 0;
    assert maxThreads >= coreThreads;
    BlockingQueue<Runnable> cleanTaks = new PriorityBlockingQueue<Runnable>(coreThreads,
        Comparator.comparing((cw) -> {
          try {
            return ((CleanWork)((FutureTask)cw).get()).getPriority();
          } catch (InterruptedException | ExecutionException e) {
            LOG.info("Exception in priority queue: " + e.getMessage());
          }
          return 0;
        }));
    executorService = new ThreadPoolExecutor(coreThreads, maxThreads, KEEPALIVE_SECONDS, TimeUnit.SECONDS, cleanTaks);
  }

  @Override
  public void run() {
    if (cleanerCheckInterval == 0) {
      cleanerCheckInterval = conf.getTimeVar(
          HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    do {
      // This is solely for testing.  It checks if the test has set the looped value to false,
      // and if so remembers that and then sets it to true at the end.  We have to check here
      // first to make sure we go through a complete iteration of the loop before resetting it.
      boolean setLooped = !looped.get();
      TxnStore.MutexAPI.LockHandle handle = null;
      long startedAt = -1;
      // Make sure nothing escapes this run method and kills the metastore at large,
      // so wrap it in a big catch Throwable statement.
      try {
        handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Cleaner.name());
        startedAt = System.currentTimeMillis();
        long minOpenTxnId = txnHandler.findMinOpenTxnId();
        Collection<Callable<Void>> calls = new ArrayList<>();
        List<CompactionInfo> cis = txnHandler.findReadyToClean();
        LOG.info("Found " + cis.size() + " potentials compations to clean");
        for(CompactionInfo compactionInfo : cis) {
          calls.add(new CleanWork(compactionInfo, minOpenTxnId, rsLock));
        }
        // We have to wait now for all the Callables to finish before proceeding
        // because otherwise we could start again the same cleaning work. When the
        // cleaning work finishes markCleaned will be called and rows will be removed
        // from TXN_COMPONENTS that should prevent this.
        // TODO: optimize this so we don't have this constraint.
        // Maybe adding a new state besides READY_FOR_CLEANING, like CLEANING_RUNNING
        executorService.invokeAll(calls);

      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor cleaner, " +
            StringUtils.stringifyException(t));
      }
      finally {
        if (handle != null) {
          handle.releaseLocks();
        }
      }
      if (setLooped) {
        looped.set(true);
      }
      // Now, go back to bed until it's time to do this again
      long elapsedTime = System.currentTimeMillis() - startedAt;
      if (elapsedTime >= cleanerCheckInterval || stop.get())  {
        continue;
      } else {
        try {
          Thread.sleep(cleanerCheckInterval - elapsedTime);
        } catch (InterruptedException ie) {
          // What can I do about it?
        }
      }
    } while (!stop.get());
  }

  private static String idWatermark(CompactionInfo ci) {
    return " id=" + ci.id;
  }

  /**
   * In the first one we scan all the directories and delete files of
   * transaction that were aborted before addDynamicPartitions is called.
   * The second one does the "regular" clean and removes files as a result
   * of compaction and other kinds of aborted transactions.
   */
  private class CleanWork implements Callable<Void> {
    final CompactionInfo ci;
    final long minOpenTxnGLB;
    final Lock rsLock;
    /**
     * Contructor that corresponds to the second kind of clean work.
     * @param ci compaction info.
     */
    CleanWork(CompactionInfo ci, long minOpenTxnGLB, Lock rsLock) {
      this.ci = ci;
      this.minOpenTxnGLB = minOpenTxnGLB;
      this.rsLock = rsLock;
    }

    @Override
    public Void call() {
      try {
        clean();
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor cleaner, " +
            StringUtils.stringifyException(t));
      }
      return null;
    }

    void clean() throws MetaException {
      if (ci.isCleanAbortedCompaction()) {
        cleanAborted();
      } else {
        cleanRegular();
      }
    }

    private void cleanAborted() throws MetaException {
      if (ci.writeIds == null || ci.writeIds.size() == 0) {
        LOG.warn("Attempted cleaning aborted transaction with empty writeId list");
        return;
      }
      LOG.info("Starting abort cleaning for table " + ci.getFullTableName()
          + ". This will scan all the partition directories.");
      try {
        Table t = syncResolveTable();
        if (t == null) {
          // The table was dropped before we got around to cleaning it.
          LOG.info("Unable to find table " + ci.getFullTableName() + ", assuming it was dropped." +
              idWatermark(ci));
          txnHandler.markCleaned(ci);
          return;
        }

        StorageDescriptor sd = resolveStorageDescriptor(t, null);

        if (runJobAsSelf(ci.runAs)) {
          rmFilesClean(sd.getLocation(), ci);
        } else {
          LOG.info("Cleaning as user " + ci.runAs + " for " + ci.getFullPartitionName());
          UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
              UserGroupInformation.getLoginUser());
          ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              rmFilesClean(sd.getLocation(), ci);
              return null;
            }
          });
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException exception) {
            LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
                ci.getFullPartitionName() + idWatermark(ci), exception);
          }
        }
        txnHandler.markCleaned(ci);
      } catch (Exception e) {
        LOG.error("Caught exception when cleaning, unable to complete cleaning of " + ci + " " +
            StringUtils.stringifyException(e));
        txnHandler.markFailed(ci);
      }
    }

    private Table syncResolveTable() throws MetaException {
      try {
        rsLock.lock();
        return resolveTable(ci);
      } finally {
        rsLock.unlock();
      }
    }

    private Database syncGetDatabase() throws NoSuchObjectException {
      try {
        rsLock.lock();
        return rs.getDatabase(getDefaultCatalog(conf), ci.dbname);
      } finally {
        rsLock.unlock();
      }
    }

    private Partition syncResolvePartition() throws Exception {
      try {
        rsLock.lock();
        return resolvePartition(ci);
      } finally {
        rsLock.unlock();
      }
    }

    private void cleanRegular() throws MetaException {
      LOG.info("Starting cleaning for " + ci);
      try {
        Table t = syncResolveTable();
        if (t == null) {
          // The table was dropped before we got around to cleaning it.
          LOG.info("Unable to find table " + ci.getFullTableName() + ", assuming it was dropped." +
              idWatermark(ci));
          txnHandler.markCleaned(ci);
          return;
        }
        Partition p = null;
        if (ci.partName != null) {
          p = syncResolvePartition();
          if (p == null) {
            // The partition was dropped before we got around to cleaning it.
            LOG.info("Unable to find partition " + ci.getFullPartitionName() +
                ", assuming it was dropped." + idWatermark(ci));
            txnHandler.markCleaned(ci);
            return;
          }
        }
        StorageDescriptor sd = resolveStorageDescriptor(t, p);
        final String location = sd.getLocation();
        ValidTxnList validTxnList =
            TxnUtils.createValidTxnListForCleaner(txnHandler.getOpenTxns(), minOpenTxnGLB);
        //save it so that getAcidState() sees it
        conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
        /**
         * {@code validTxnList} is capped by minOpenTxnGLB so if
         * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} sees a base/delta
         * produced by a compactor, that means every reader that could be active right now see it
         * as well.  That means if this base/delta shadows some earlier base/delta, the it will be
         * used in favor of any files that it shadows.  Thus the shadowed files are safe to delete.
         *
         *
         * The metadata about aborted writeIds (and consequently aborted txn IDs) cannot be deleted
         * above COMPACTION_QUEUE.CQ_HIGHEST_WRITE_ID.
         * See {@link TxnStore#markCleaned(CompactionInfo)} for details.
         * For example given partition P1, txnid:150 starts and sees txnid:149 as open.
         * Say compactor runs in txnid:160, but 149 is still open and P1 has the largest resolved
         * writeId:17.  Compactor will produce base_17_c160.
         * Suppose txnid:149 writes delta_18_18
         * to P1 and aborts.  Compactor can only remove TXN_COMPONENTS entries
         * up to (inclusive) writeId:17 since delta_18_18 may be on disk (and perhaps corrupted) but
         * not visible based on 'validTxnList' capped at minOpenTxn so it will not not be cleaned by
         * {@link #removeFiles(String, ValidWriteIdList, CompactionInfo)} and so we must keep the
         * metadata that says that 18 is aborted.
         * In a slightly different case, whatever txn created delta_18 (and all other txn) may have
         * committed by the time cleaner runs and so cleaner will indeed see delta_18_18 and remove
         * it (since it has nothing but aborted data).  But we can't tell which actually happened
         * in markCleaned() so make sure it doesn't delete meta above CG_CQ_HIGHEST_WRITE_ID.
         *
         * We could perhaps make cleaning of aborted and obsolete and remove all aborted files up
         * to the current Min Open Write Id, this way aborted TXN_COMPONENTS meta can be removed
         * as well up to that point which may be higher than CQ_HIGHEST_WRITE_ID.  This could be
         * useful if there is all of a sudden a flood of aborted txns.  (For another day).
         */
        List<String> tblNames = Collections.singletonList(
            TableName.getDbTable(t.getDbName(), t.getTableName()));
        GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tblNames);
        rqst.setValidTxnList(validTxnList.writeToString());
        GetValidWriteIdsResponse rsp = txnHandler.getValidWriteIds(rqst);
        //we could have no write IDs for a table if it was never written to but
        // since we are in the Cleaner phase of compactions, there must have
        // been some delta/base dirs
        assert rsp != null && rsp.getTblValidWriteIdsSize() == 1;
        //Creating 'reader' list since we are interested in the set of 'obsolete' files
        ValidReaderWriteIdList validWriteIdList =
            TxnCommonUtils.createValidReaderWriteIdList(rsp.getTblValidWriteIds().get(0));

        if (runJobAsSelf(ci.runAs)) {
          rmFilesRegular(location, validWriteIdList, ci);
        } else {
          LOG.info("Cleaning as user " + ci.runAs + " for " + ci.getFullPartitionName());
          UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
              UserGroupInformation.getLoginUser());
          ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              rmFilesRegular(location, validWriteIdList, ci);
              return null;
            }
          });
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException exception) {
            LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
                ci.getFullPartitionName() + idWatermark(ci), exception);
          }
        }
        txnHandler.markCleaned(ci);
      } catch (Exception e) {
        LOG.error("Caught exception when cleaning, unable to complete cleaning of " + ci + " " +
            StringUtils.stringifyException(e));
        txnHandler.markFailed(ci);
      }
    }

    private void rmFilesClean(String rootLocation, CompactionInfo ci) throws IOException, NoSuchObjectException {
      List<FileStatus> deleted = AcidUtils.deleteDeltaDirectories(new Path(rootLocation), conf, ci.writeIds);

      if (deleted.size() == 0) {
        LOG.info("No files were deleted in the clean abort compaction: " + idWatermark(ci));
        return;
      }

      FileSystem fs = deleted.get(0).getPath().getFileSystem(conf);
      Database db = syncGetDatabase();
      Boolean isSourceOfRepl = ReplChangeManager.isSourceOfReplication(db);

      for (FileStatus dead : deleted) {
        Path deadPath = dead.getPath();
        LOG.debug("Going to delete path " + deadPath.toString());
        if (isSourceOfRepl) {
          replChangeManager.recycle(deadPath, ReplChangeManager.RecycleType.MOVE, true);
        }
        fs.delete(deadPath, true);
      }
    }

    private void rmFilesRegular(String location, ValidWriteIdList writeIdList, CompactionInfo ci)
        throws IOException, NoSuchObjectException {
      Path locPath = new Path(location);
      AcidUtils.Directory dir = AcidUtils.getAcidState(locPath, conf, writeIdList);
      List<FileStatus> obsoleteDirs = dir.getObsolete();
      /**
       * add anything in 'dir'  that only has data from aborted transactions - no one should be
       * trying to read anything in that dir (except getAcidState() that only reads the name of
       * this dir itself)
       * So this may run ahead of {@link CompactionInfo#highestWriteId} but it's ok (suppose there
       * are no active txns when cleaner runs).  The key is to not delete metadata about aborted
       * txns with write IDs > {@link CompactionInfo#highestWriteId}.
       * See {@link TxnStore#markCleaned(CompactionInfo)}
       */
      obsoleteDirs.addAll(dir.getAbortedDirectories());
      List<Path> filesToDelete = new ArrayList<>(obsoleteDirs.size());
      StringBuilder extraDebugInfo = new StringBuilder("[");
      for (FileStatus stat : obsoleteDirs) {
        filesToDelete.add(stat.getPath());
        extraDebugInfo.append(stat.getPath().getName()).append(",");
        if(!FileUtils.isPathWithinSubtree(stat.getPath(), locPath)) {
          LOG.info(idWatermark(ci) + " found unexpected file: " + stat.getPath());
        }
      }
      extraDebugInfo.setCharAt(extraDebugInfo.length() - 1, ']');
      LOG.info(idWatermark(ci) + " About to remove " + filesToDelete.size() +
          " obsolete directories from " + location + ". " + extraDebugInfo.toString());
      if (filesToDelete.size() < 1) {
        LOG.warn("Hmm, nothing to delete in the cleaner for directory " + location +
            ", that hardly seems right.");
        return;
      }

      FileSystem fs = filesToDelete.get(0).getFileSystem(conf);
      Database db = syncGetDatabase();
      Boolean isSourceOfRepl = ReplChangeManager.isSourceOfReplication(db);

      for (Path dead : filesToDelete) {
        LOG.debug("Deleted path " + dead.toString());
        if (isSourceOfRepl) {
          replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, true);
        }
        fs.delete(dead, true);
      }
    }

    /**
     * Gives higher priority to regular clean tasks. Lower value
     * means more priority
     * @return priority.
     */
    int getPriority() {
      if (ci.isCleanAbortedCompaction()) {
        return 1;
      } else {
        return 2;
      }
    }
  }
}
