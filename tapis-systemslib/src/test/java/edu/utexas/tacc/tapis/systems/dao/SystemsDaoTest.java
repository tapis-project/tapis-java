package edu.utexas.tacc.tapis.systems.dao;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;


/**
 * Test the SystemsDao class against a running DB
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;

  // Test data
  private static final String tenant = "tenant1";
  private static String mechsStr = "{SFTP,S3}";
  private static List<TransferMechanism> mechs = new ArrayList<>(List.of(TransferMechanism.SFTP, TransferMechanism.S3));
  private static String mechsStrEmpty = "{}";
  private static List<TransferMechanism> mechsEmpty = new ArrayList<>();
  private static final Protocol prot1 = new Protocol(AccessMechanism.NONE, mechs, 0, false, "", 0);
  private static final Protocol prot2 = new Protocol(AccessMechanism.ANONYMOUS, mechs, 22, false, "",0);
  private static final Protocol prot3 = new Protocol(AccessMechanism.SSH_CERT, mechs, 23, true, "localhost",22);
  private static final Protocol prot4 = new Protocol(AccessMechanism.SSH_CERT, mechsEmpty, -1, false, "",-1);
  private static final Protocol prot5 = new Protocol(AccessMechanism.SSH_PASSWORD, mechsEmpty, -1, false, null,-1);
  private static final Protocol prot6 = new Protocol(AccessMechanism.SSH_PASSWORD, mechsEmpty, -1, false, "",-1);
  private static final Protocol prot7 = new Protocol(AccessMechanism.SSH_PASSWORD, mechsEmpty, -1, false, "",-1);
  private static final String[] sys1 = {tenant, "sys1a", "description 1", "owner1", "host1", "bucket1", "/root1",
      "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", "fakePassword1"};
  private static final String[] sys2 = {tenant, "sys2a", "description 2", "owner2", "host2", "bucket2", "/root2",
      "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", "fakePassword2"};
  private static final String[] sys3 = {tenant, "sys3a", "description 3", "owner3", "host3", "bucket3", "/root3",
      "jobInputDir3", "jobOutputDir3", "workDir3", "scratchDir3", "effUser3", "fakePassword3"};
  private static final String[] sys4 = {tenant, "sys4a", "description 4", "owner4", "host4", "bucket4", "/root4",
    "jobInputDir4", "jobOutputDir4", "workDir4", "scratchDir4", "effUser4", "fakePassword4"};
  private static final String[] sys5 = {tenant, "sys5a", "description 5", "owner5", "host5", "bucket5", "/root5",
    "jobInputDir5", "jobOutputDir5", "workDir5", "scratchDir5", "effUser5", "fakePassword5"};
  private static final String[] sys6 = {tenant, "sys6a", "description 6", "owner6", "host6", "bucket6", "/root6",
    "jobInputDir6", "jobOutputDir6", "workDir6", "scratchDir6", "effUser6", "fakePassword6"};
  private static final String[] sys7 = {tenant, "sys7a", "description 7", "owner7", "host7", "bucket7", "/root7",
    "jobInputDir7", "jobOutputDir7", "workDir7", "scratchDir7", "effUser7", "fakePassword7"};

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
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByName() throws Exception {
    String[] sys0 = sys2;
    Protocol prot0 = prot2;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
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
    Assert.assertEquals(tmpSys.getAccessMechanism(), prot0.getAccessMechanism());
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    List<TransferMechanism> tmechsList = tmpSys.getTransferMechanisms();
    Assert.assertNotNull(tmechsList);
    Assert.assertTrue(tmechsList.contains(TransferMechanism.S3), "List of transfer mechanisms did not contain: " + TransferMechanism.S3.name());
    Assert.assertTrue(tmechsList.contains(TransferMechanism.SFTP), "List of transfer mechanisms did not contain: " + TransferMechanism.SFTP.name());
  }

  // Test retrieving all system names
  @Test(enabled=true)
  public void testGetSystemNames() throws Exception {
    String[] sys0 = sys3;
    Protocol prot0 = prot3;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    prot0 = prot4;
    itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
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
    Protocol prot0 = prot5;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = dao.getTSystems(tenant);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    String[] sys0 = sys6;
    Protocol prot0 = prot6;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.deleteTSystem(sys0[0], sys0[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys0[1]);
  }

  // Test create and get for a single item with no transfer mechanisms supported
  @Test(enabled=true)
  public void testNoTxfr() throws Exception
  {
    String[] sys0 = sys7;
    Protocol prot0 = prot7;
    int itemId = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11],
                                    prot0.getAccessMechanism().name(), null, prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort());
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
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
    Assert.assertEquals(tmpSys.getAccessMechanism(), prot0.getAccessMechanism());
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    List<TransferMechanism> tmechsList = tmpSys.getTransferMechanisms();
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
    dao.deleteTSystem(sys7[0], sys7[1]);
  }
}