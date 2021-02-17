package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ MediumTests.class, RegionServerTests.class})
public class TestAssignAlreadyOpened {
  private static Logger LOG =
    LoggerFactory.getLogger(TestAssignAlreadyOpened.class.getName());

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAssignAlreadyOpened.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("testAbort");

  private static CountDownLatch waitUntilAbort = new CountDownLatch(1);
  private static CountDownLatch waitWithAbort = new CountDownLatch(1);
  private static AtomicReference<JVMClusterUtil.RegionServerThread> rsThreadToAbort = new AtomicReference<>();
  private static AtomicBoolean rsAbortNextTime = new AtomicBoolean(false);

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                                       CrashAfterSecondOpeningCP.class.getName());
    UTIL.startMiniCluster(1); // create a single RS, for the meta and namespace tables
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void updateRSThread() {
    for (JVMClusterUtil.RegionServerThread t : UTIL.getMiniHBaseCluster()
      .getLiveRegionServerThreads()) {
      if (!t.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        rsThreadToAbort.set(t);
        return;
      }
    }
    fail("unable to find RS thread");
  }

  private void dumpMeta(String comment) throws IOException {
    Table t = UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      RegionInfo info = MetaTableAccessor.getRegionInfo(result);
      if (info == null) {
        continue;
      }
      LOG.info("META DUMP - {} regioninfo: {}", comment, Bytes.toStringBinary(result.getRow()) + info);

      byte[] stateBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
      if (stateBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} state: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(stateBytes));

      byte[] serverBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      if (serverBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} server: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(serverBytes));
    }
    s.close();
    t.close();
  }


  @Test
  public void test() throws Exception {
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME); // first opening attempt
    RegionInfo testRegion = UTIL.getConnection().getRegionLocator(TABLE_NAME).getAllRegionLocations().get(0).getRegion();
    dumpMeta("after table create");

    // creating a new region server
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    ServerName secondRS = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1).getRegionServer().getServerName();

    // configuring the coprocessor to kill the second RS on the next open attempt
    rsAbortNextTime.set(true);
    rsThreadToAbort.set(UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1));

    // move the region to the second RS, this will cause abortion
    dumpMeta("before table move");
    UTIL.getAdmin().move(testRegion.getEncodedNameAsBytes(), secondRS);
    dumpMeta("after table move");

    // wait until abort successful
    assertTrue(waitUntilAbort.await(10, TimeUnit.SECONDS));
    UTIL.getMiniHBaseCluster().getMaster().stop("stopped master by the test");
    waitWithAbort.countDown();

    // wait until RS really done
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor aborted the RS");
    dumpMeta("after RS abortion");


    UTIL.getMiniHBaseCluster().startMaster();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(10000);
    dumpMeta("after master started");

    // restart the second RS
    UTIL.getMiniHBaseCluster().startRegionServer(secondRS.getHostname(), secondRS.getPort());
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    dumpMeta("after RS started again");



    // the master still thinks the region is in transition
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    UTIL.waitFor(10000, () -> am.getRegionsInTransition().stream().anyMatch(r -> r.getTable().equals(TABLE_NAME)));


    // UTIL.getAdmin().disableTable(TABLE_NAME);

    // dumpMeta("before enable");
    // UTIL.getAdmin().enableTableAsync(TABLE_NAME); // this should trigger the second table opening, which should cause the RS to abort before answering to Master


    //JVMClusterUtil.shutdown(Collections.emptyList(), asList(currentRSThread.get()));

    updateRSThread();
    rsThreadToAbort.get().getRegionServer().abort("test: aborting RS");

    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("RS stopped");
    dumpMeta("after RS stopped");

    // restarting RS, the second open attempt should abort the RS
    UTIL.getMiniHBaseCluster().startRegionServer();
    //UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);

    // wait until abort successful
    assertTrue(waitUntilAbort.await(10, TimeUnit.SECONDS));

    // wait until RS restarted
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor aborted the RS");

    UTIL.getMiniHBaseCluster().startRegionServer();
    LOG.info("region server restarted");


    // wait until RS restarted
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    updateRSThread();
    dumpMeta("after RS restarted");

    // wait until region is back on the RS and serving requests
    UTIL.waitFor(15000, () -> rsThreadToAbort.get().getRegionServer().getRegions().stream().anyMatch(h -> h.getTableDescriptor().getTableName().equals(TABLE_NAME)));
    UTIL.loadRandomRows(UTIL.getConnection().getTable(TABLE_NAME), CF, 10, 10);
    UTIL.flush(TABLE_NAME);
    dumpMeta("after region is online");


    //UTIL.waitUntilNoRegionsInTransition(30000);


    /*
    // find the rs and hri of the table
    HRegionServer rs = rsThread.getRegionServer();
    RegionInfo hri = rs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    TransitRegionStateProcedure moveRegionProcedure = TransitRegionStateProcedure.reopen(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(), hri);
    RegionStateNode regionNode = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getOrCreateRegionStateNode(hri);
    regionNode.setProcedure(moveRegionProcedure);
    UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
      .submitProcedure(moveRegionProcedure);
    countDownLatch.await();
    UTIL.getMiniHBaseCluster().stopMaster(0);
    UTIL.getMiniHBaseCluster().startMaster();
    // wait until master initialized
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster() != null &&
      UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    Assert.assertTrue("Should be 3 RS after master restart",
                      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 3);

     */

  }

  @Test
  public void test2() throws Exception {
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME); // first opening attempt
    RegionInfo testRegion = UTIL.getConnection().getRegionLocator(TABLE_NAME).getAllRegionLocations().get(0).getRegion();
    dumpMeta("after table create");

    // creating a new region server
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    ServerName secondRS = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1).getRegionServer().getServerName();

    // move the region to the second RS
    dumpMeta("before table move");
    UTIL.moveRegionAndWait(testRegion, secondRS);
    dumpMeta("after table move");

    UTIL.unassignRegion(testRegion.getRegionName());
    dumpMeta("after unassign");

    // configuring the coprocessor to kill the second RS on the next open attempt
    rsAbortNextTime.set(true);
    rsThreadToAbort.set(UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1));

    UTIL.getAdmin().assign(testRegion.getEncodedNameAsBytes());

    // wait until abort successful
    assertTrue(waitUntilAbort.await(10, TimeUnit.SECONDS));
    dumpMeta("after coprocessor triggered on second RS");
    waitWithAbort.countDown();

    // wait until first RS really done
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor aborted the second RS");
    dumpMeta("after coprocessor aborted the second RS");


    // restart the second RS
    UTIL.getMiniHBaseCluster().startRegionServer(secondRS.getHostname(), secondRS.getPort());
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor restarted the second RS");
    dumpMeta("after restarted the second RS");

    // the master still thinks the region is in transition
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    UTIL.waitFor(10000, () -> am.getRegionsInTransition().stream().anyMatch(r -> r.getTable().equals(TABLE_NAME)));


    // UTIL.getAdmin().disableTable(TABLE_NAME);

    // dumpMeta("before enable");
    // UTIL.getAdmin().enableTableAsync(TABLE_NAME); // this should trigger the second table opening, which should cause the RS to abort before answering to Master


    //JVMClusterUtil.shutdown(Collections.emptyList(), asList(currentRSThread.get()));

    updateRSThread();
    rsThreadToAbort.get().getRegionServer().abort("test: aborting RS");

    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("RS stopped");
    dumpMeta("after RS stopped");

    // restarting RS, the second open attempt should abort the RS
    UTIL.getMiniHBaseCluster().startRegionServer();
    //UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);

    // wait until abort successful
    assertTrue(waitUntilAbort.await(10, TimeUnit.SECONDS));

    // wait until RS restarted
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor aborted the RS");

    UTIL.getMiniHBaseCluster().startRegionServer();
    LOG.info("region server restarted");


    // wait until RS restarted
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 2);
    updateRSThread();
    dumpMeta("after RS restarted");

    // wait until region is back on the RS and serving requests
    UTIL.waitFor(15000, () -> rsThreadToAbort.get().getRegionServer().getRegions().stream().anyMatch(h -> h.getTableDescriptor().getTableName().equals(TABLE_NAME)));
    UTIL.loadRandomRows(UTIL.getConnection().getTable(TABLE_NAME), CF, 10, 10);
    UTIL.flush(TABLE_NAME);
    dumpMeta("after region is online");


    //UTIL.waitUntilNoRegionsInTransition(30000);


    /*
    // find the rs and hri of the table
    HRegionServer rs = rsThread.getRegionServer();
    RegionInfo hri = rs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    TransitRegionStateProcedure moveRegionProcedure = TransitRegionStateProcedure.reopen(
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(), hri);
    RegionStateNode regionNode = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getOrCreateRegionStateNode(hri);
    regionNode.setProcedure(moveRegionProcedure);
    UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
      .submitProcedure(moveRegionProcedure);
    countDownLatch.await();
    UTIL.getMiniHBaseCluster().stopMaster(0);
    UTIL.getMiniHBaseCluster().startMaster();
    // wait until master initialized
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster() != null &&
      UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    Assert.assertTrue("Should be 3 RS after master restart",
                      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 3);

     */

  }



  @Test
  public void test3() throws Exception {
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME); // first opening attempt
    RegionInfo testRegion = UTIL.getConnection().getRegionLocator(TABLE_NAME).getAllRegionLocations().get(0).getRegion();
    dumpMeta("after table create");

    ServerName rs = UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer().getServerName();

    UTIL.unassignRegion(testRegion.getRegionName());
    dumpMeta("after unassign");

    // configuring the coprocessor to kill the  RS on the next open attempt
    rsAbortNextTime.set(true);
    rsThreadToAbort.set(UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(0));


    UTIL.getAdmin().assign(testRegion.getRegionName()); // should cause the coprocessor to trigger abortion

    // wait until abort successful
    assertTrue(waitUntilAbort.await(10, TimeUnit.SECONDS));
    waitWithAbort.countDown();

    System.err.println("aaaa");


    // wait until first RS really done
    System.err.println("bbbb");
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 0);
    System.err.println("cccc");
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    System.err.println("dddd");
    LOG.info("coprocessor aborted the RS");
    //dumpMeta("after coprocessor aborted the RS");
    System.err.println("eeee");


    // restart the RS
    //UTIL.getMiniHBaseCluster().startRegionServer(rs.getHostname(), rs.getPort());
    UTIL.getMiniHBaseCluster().startRegionServer();
    System.err.println("ffff");
    UTIL.waitFor(15000, () -> UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == 1);
    Thread.sleep(5000); // make sure the aborted RS totally dead and all threads exited
    LOG.info("coprocessor restarted the RS");
    dumpMeta("after restarted the RS");

    // the master still thinks the region is in transition
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    UTIL.waitFor(10000, () -> am.getRegionsInTransition().stream().anyMatch(r -> r.getTable().equals(TABLE_NAME)));


  }



  public static class CrashAfterSecondOpeningCP implements RegionCoprocessor, RegionObserver {

    static AtomicInteger regionOpenAttempts = new AtomicInteger(0);

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
      if (c.getEnvironment().getRegion().getRegionInfo().getTable().equals(TABLE_NAME)) {

        int openAttempts = regionOpenAttempts.incrementAndGet();
        LOG.info("CrashAfterSecondOpeningCP: region opening attempt {}", openAttempts);

        if(rsAbortNextTime.getAndSet(false)) {
          //updateRSThread();
          //UTIL.getMiniHBaseCluster().killRegionServer(rsThreadToAbort.get().getRegionServer().getServerName());

          waitUntilAbort.countDown();
          try {
            waitWithAbort.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          rsThreadToAbort.get().getRegionServer().abort("CrashAfterSecondOpeningCP: aborting RS");
          LOG.info("CrashAfterSecondOpeningCP: RS shutdown called");
        }
      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }
}
