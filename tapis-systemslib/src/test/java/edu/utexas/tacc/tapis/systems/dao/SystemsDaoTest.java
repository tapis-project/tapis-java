package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;

/**
 * Test the SystemsDao class against a running DB
 * System objects need a valid Protocol so create those in setup
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;
  private ProtocolDaoImpl protocolDao;

  // Test data
  private static String mechsStr = "{SFTP,S3}";
  private static final String tenant = "tenant1";
  private static final String[] sys1 = {tenant, "sys1", "description 1", "owner1", "host1", "bucket1", "/root1",
      "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", "fakePassword1"};
  private static final String[] sys2 = {tenant, "sys2", "description 2", "owner2", "host2", "bucket2", "/root2",
      "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", "fakePassword2"};
  private static final String[] sys3 = {tenant, "sys3", "description 3", "owner3", "host3", "bucket3", "/root3",
      "jobInputDir3", "jobOutputDir3", "workDir3", "scratchDir3", "effUser3", "fakePassword3"};
  private static final String[] sys4 = {tenant, "sys4", "description 4", "owner4", "host4", "bucket4", "/root4",
    "jobInputDir4", "jobOutputDir4", "workDir4", "scratchDir4", "effUser4", "fakePassword4"};
  private static final String[] sys5 = {tenant, "sys5", "description 5", "owner5", "host5", "bucket5", "/root5",
    "jobInputDir5", "jobOutputDir5", "workDir5", "scratchDir5", "effUser5", "fakePassword5"};
  private static final String[] sys6 = {tenant, "sys6", "description 6", "owner6", "host6", "bucket6", "/root6",
    "jobInputDir6", "jobOutputDir6", "workDir6", "scratchDir6", "effUser6", "fakePassword6"};
  private static final String[] sys7 = {tenant, "sys7", "description 7", "owner7", "host7", "bucket7", "/root7",
    "jobInputDir7", "jobOutputDir7", "workDir7", "scratchDir7", "effUser7", "fakePassword7"};

  int protId1, protId2;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDaoImpl();
    protocolDao = new ProtocolDaoImpl();
    // Use port number different from values in other tests since other tests may be running in parallel. Cleanup in other tests
    //  can fail if protocols are referenced in the systems created here.
    protId1 = protocolDao.create(AccessMechanism.NONE.name(), mechsStr, 1001, false, "", 0);
    protId2 = protocolDao.create(AccessMechanism.SSH_PASSWORD.name(), null, 1002, true,
                                 "localhost", 2222);
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    String[] sys0 = sys1;
    int numRows = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    Assert.assertEquals(numRows, 1);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByName() throws Exception {
    String[] sys0 = sys2;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    TSystem tmpSys = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getOwner(), sys0[3]);
    Assert.assertEquals(tmpSys.getHost(), sys0[4]);
    Assert.assertEquals(tmpSys.getBucketName(), sys0[5]);
    Assert.assertEquals(tmpSys.getRootDir(), sys0[6]);
    Assert.assertEquals(tmpSys.getJobInputDir(), sys0[7]);
    Assert.assertEquals(tmpSys.getJobOutputDir(), sys0[8]);
    Assert.assertEquals(tmpSys.getWorkDir(), sys0[9]);
    Assert.assertEquals(tmpSys.getScratchDir(), sys0[10]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0[11]);
    Assert.assertNotNull(tmpSys.getProtocol(), "Protocol was null for system name: " + sys0[1]);
    Assert.assertEquals(tmpSys.getProtocol().getAccessMechanism(), AccessMechanism.NONE.name());
    Assert.assertEquals(tmpSys.getProtocol().getPort(), 1001);
    Assert.assertEquals(tmpSys.getProtocol().isUseProxy(), false);
    Assert.assertEquals(tmpSys.getProtocol().getProxyHost(), "");
    Assert.assertEquals(tmpSys.getProtocol().getProxyPort(), 0);
    List<Protocol.TransferMechanism> tmechsList = tmpSys.getProtocol().getTransferMechanisms();
    Assert.assertNotNull(tmechsList);
    Assert.assertTrue(tmechsList.contains(Protocol.TransferMechanism.S3), "List of transfer mechanisms did not contain: " + Protocol.TransferMechanism.S3.name());
    Assert.assertTrue(tmechsList.contains(Protocol.TransferMechanism.SFTP), "List of transfer mechanisms did not contain: " + Protocol.TransferMechanism.SFTP.name());
  }

  // Test retrieving all system names
  @Test(enabled=true)
  public void testGetSystemNames() throws Exception {
    String[] sys0 = sys3;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    sys0 = sys4;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    List<String> systemNames = dao.getTSystemNames(tenant);
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
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    List<TSystem> systems = dao.getTSystems(tenant);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    String[] sys0 = sys6;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId1);
    dao.deleteTSystem(sys0[0], sys0[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys0[1]);
  }

  // Test create and get for a single item with no transfer mechanisms supported
  @Test(enabled=true)
  public void testNoTxfr() throws Exception
  {
    String[] sys0 = sys7;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId2);
    TSystem tmpSys = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getOwner(), sys0[3]);
    Assert.assertEquals(tmpSys.getHost(), sys0[4]);
    Assert.assertEquals(tmpSys.getBucketName(), sys0[5]);
    Assert.assertEquals(tmpSys.getRootDir(), sys0[6]);
    Assert.assertEquals(tmpSys.getJobInputDir(), sys0[7]);
    Assert.assertEquals(tmpSys.getJobOutputDir(), sys0[8]);
    Assert.assertEquals(tmpSys.getWorkDir(), sys0[9]);
    Assert.assertEquals(tmpSys.getScratchDir(), sys0[10]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0[11]);
    Assert.assertNotNull(tmpSys.getProtocol(), "Protocol was null for system name: " + sys0[1]);
    Assert.assertEquals(tmpSys.getProtocol().getAccessMechanism(), AccessMechanism.SSH_PASSWORD.name());
    Assert.assertEquals(tmpSys.getProtocol().getPort(), 1002);
    Assert.assertEquals(tmpSys.getProtocol().isUseProxy(), true);
    Assert.assertEquals(tmpSys.getProtocol().getProxyHost(), "localhost");
    Assert.assertEquals(tmpSys.getProtocol().getProxyPort(), 2222);
    List<Protocol.TransferMechanism> tmechsList = tmpSys.getProtocol().getTransferMechanisms();
    Assert.assertNotNull(tmechsList);
    Assert.assertEquals(tmechsList.size(), 0);
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
    dao.deleteTSystem(sys5[0], sys7[1]);
    protocolDao.delete(protId1);
    protocolDao.delete(protId2);
  }
}