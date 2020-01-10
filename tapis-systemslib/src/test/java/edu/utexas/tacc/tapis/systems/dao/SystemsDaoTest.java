package edu.utexas.tacc.tapis.systems.dao;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMethod;

// TODO Update tests to check "tags" value
// TODO Update tests to check "notes" value

/**
 * Test the SystemsDao class against a DB running locally
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;

  // Test data
  private static final String tenantName = "tenant1";
  private static List<TransferMethod> txfrMethods1 = new ArrayList<>(List.of(TransferMethod.SFTP, TransferMethod.S3));
  private static List<TransferMethod> txfrMethodsEmpty = new ArrayList<>();
  private static final Protocol prot1 = new Protocol(AccessMethod.ACCESS_KEY, txfrMethods1, 0, false, "", 0);
  private static final Protocol prot2 = new Protocol(AccessMethod.PKI_KEYS, txfrMethods1, 22, false, "",0);
  private static final Protocol prot3 = new Protocol(AccessMethod.CERT, txfrMethods1, 23, true, "localhost",22);
  private static final Protocol prot4 = new Protocol(AccessMethod.CERT, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot5 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, null,-1);
  private static final Protocol prot6 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot7 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final String tags = "{\"key1\":\"a\", \"key2\":\"b\"}";
  private static final String notes = "{\"project\":\"myproj1\", \"testdata\":\"abc\"}";
  private static final String[] sys1 = {tenantName, "Dsys1", "description 1", SystemType.LINUX.name(), "owner1", "host1", "effUser1", "bucket1", "/root1",
      "jobLocalWorkDir1", "jobLocalArchDir1", "jobRemoteArchSystem1", "jobRemoteArchDir1", tags, notes, "{}"};
  private static final String[] sys2 = {tenantName, "Dsys2", "description 2", SystemType.LINUX.name(), "owner2", "host2", "effUser2", "bucket2", "/root2",
       "jobLocalWorkDir2", "jobLocalArchDir2", "jobRemoteArchSystem2", "jobRemoteArchDir2", tags, notes, "{}"};
  private static final String[] sys3 = {tenantName, "Dsys3", "description 3", SystemType.OBJECT_STORE.name(), "owner3", "host3", "effUser3", "bucket3", "/root3",
       "jobLocalWorkDir3", "jobLocalArchDir3", "jobRemoteArchSystem3", "jobRemoteArchDir3", tags, notes, "{}"};
  private static final String[] sys4 = {tenantName, "Dsys4", "description 4", SystemType.LINUX.name(), "owner4", "host4", "effUser4", "bucket4", "/root4",
       "jobLocalWorkDir4", "jobLocalArchDir4", "jobRemoteArchSystem4", "jobRemoteArchDir4", tags, notes, "{}"};
  private static final String[] sys5 = {tenantName, "Dsys5", "description 5", SystemType.LINUX.name(), "owner5", "host5", "effUser5", "bucket5", "/root5",
       "jobLocalWorkDir5", "jobLocalArchDir5", "jobRemoteArchSystem5", "jobRemoteArchDir5", tags, notes, "{}"};
  private static final String[] sys6 = {tenantName, "Dsys6", "description 6", SystemType.LINUX.name(), "owner6", "host6", "effUser6", "bucket6", "/root6",
       "jobLocalWorkDir6", "jobLocalArchDir6", "jobRemoteArchSystem6", "jobRemoteArchDir6", tags, notes, "{}"};
  private static final String[] sys7 = {tenantName, "Dsys7", "description 7", SystemType.LINUX.name(), "owner7", "host7", "effUser7", "bucket7", "/root7",
       "jobLocalWorkDir7", "jobLocalArchDir7", "jobRemoteArchSystem7", "jobRemoteArchDir7", tags, notes, "{}"};

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDaoImpl();
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    String[] sys0 = sys1;
    Protocol prot0 = prot1;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
                                   prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
                                   prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
                                   false, sys0[9], sys0[10], sys0[11], sys0[12], null,
                                    sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByName() throws Exception {
    String[] sys0 = sys2;
    Protocol prot0 = prot2;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0[3]);
    Assert.assertEquals(tmpSys.getOwner(), sys0[4]);
    Assert.assertEquals(tmpSys.getHost(), sys0[5]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0[6]);
    Assert.assertEquals(tmpSys.getBucketName(), sys0[7]);
    Assert.assertEquals(tmpSys.getRootDir(), sys0[8]);
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0[9]);
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0[10]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0[11]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0[12]);
    System.out.println("Found tags: " + tmpSys.getTags());
    System.out.println("Found notes: " + tmpSys.getNotes());
    Assert.assertEquals(tmpSys.getAccessMethod(), prot0.getAccessMethod());
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    Assert.assertTrue(tMethodsList.contains(TransferMethod.S3), "List of transfer methods did not contain: " + TransferMethod.S3.name());
    Assert.assertTrue(tMethodsList.contains(TransferMethod.SFTP), "List of transfer methods did not contain: " + TransferMethod.SFTP.name());
  }

  // Test retrieving all system names
  @Test(enabled=true)
  public void testGetSystemNames() throws Exception {
    String[] sys0 = sys3;
    Protocol prot0 = prot3;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    prot0 = prot4;
    itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = dao.getTSystemNames(tenantName);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(sys3[1]), "List of systems did not contain system name: " + sys3[1]);
    Assert.assertTrue(systemNames.contains(sys4[1]), "List of systems did not contain system name: " + sys4[1]);
  }

  // Test retrieving all systems
  @Test(enabled=true)
  public void testGetSystems() throws Exception {
    String[] sys0 = sys5;
    Protocol prot0 = prot5;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = dao.getTSystems(tenantName);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    String[] sys0 = sys6;
    Protocol prot0 = prot6;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], prot0.getTransferMethodsAsStr(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.deleteTSystem(sys0[0], sys0[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys0[1]);
  }

  // Test create and get for a single item with no transfer methods supported
  @Test(enabled=true)
  public void testNoTxfr() throws Exception
  {
    String[] sys0 = sys7;
    Protocol prot0 = prot7;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6],
            prot0.getAccessMethod().name(), sys0[7], sys0[8], null,
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[9], sys0[10], sys0[11], sys0[12], null,
            sys0[13], sys0[14], sys0[15]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0[3]);
    Assert.assertEquals(tmpSys.getOwner(), sys0[4]);
    Assert.assertEquals(tmpSys.getHost(), sys0[5]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0[6]);
    Assert.assertEquals(tmpSys.getBucketName(), sys0[7]);
    Assert.assertEquals(tmpSys.getRootDir(), sys0[8]);
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0[9]);
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0[10]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0[11]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0[12]);
    System.out.println("Found tags: " + tmpSys.getTags());
    System.out.println("Found notes: " + tmpSys.getNotes());
    Assert.assertEquals(tmpSys.getAccessMethod(), prot0.getAccessMethod());
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    List<TransferMethod> txfrMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(txfrMethodsList);
    Assert.assertEquals(txfrMethodsList.size(), 0);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.deleteTSystem(sys1[0], sys1[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys1[0], sys1[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys1[1]);
    dao.deleteTSystem(sys2[0], sys2[1]);
    dao.deleteTSystem(sys3[0], sys3[1]);
    dao.deleteTSystem(sys4[0], sys4[1]);
    dao.deleteTSystem(sys5[0], sys5[1]);
    dao.deleteTSystem(sys7[0], sys7[1]);
  }
}