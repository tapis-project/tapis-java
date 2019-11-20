package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;

// TODO Update tests to check "tags" value
// TODO Update tests to check "notes" value

/**
 * Test the SystemsService implementation class against a DB running locally
 */
@Test(groups={"integration"})
public class SystemsServiceTest
{
  private SystemsService svc;

  // Test data
  private static final String tenant = "dev";
  private static final String apiUserId = "testuser1";
  private static final List<TransferMechanism> mechList = new ArrayList<>(List.of(TransferMechanism.SFTP,TransferMechanism.S3));
  private static final Protocol prot1 = new Protocol(AccessMechanism.NONE, mechList, -1, false, "",-1);
  private static final String prot1AccessMechName = prot1.getAccessMechanism().name();
  private static final String prot1TxfMechs = prot1.getTransferMechanismsAsStr();
  private static final String tags = "{\"key1\":\"a\", \"key2\":\"b\"}";
  private static final String notes = "{\"project\":\"myproj1\", \"testdata\":\"abc\"}";

  private static final String[] sys1 = {tenant, "Ssys1", "description 1", "owner1", "host1", "bucket1", "/root1",
    "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", tags, notes, "fakePassword1", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys2 = {tenant, "Ssys2", "description 2", "owner2", "host2", "bucket2", "/root2",
    "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", tags, notes, "fakePassword2", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys3 = {tenant, "Ssys3", "description 3", "owner3", "host3", "bucket3", "/root3",
    "jobInputDir3", "jobOutputDir3", "workDir3", "scratchDir3", "effUser3", tags, notes, "fakePassword3", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys4 = {tenant, "Ssys4", "description 4", "owner4", "host4", "bucket4", "/root4",
    "jobInputDir4", "jobOutputDir4", "workDir4", "scratchDir4", "effUser4", tags, notes, "fakePassword4", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys5 = {tenant, "Ssys5", "description 5", "owner5", "host5", "bucket5", "/root5",
    "jobInputDir5", "jobOutputDir5", "workDir5", "scratchDir5", "effUser5", tags, notes, "fakePassword5", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys6 = {tenant, "Ssys6", "description 6", "owner6", "host6", "bucket6", "/root6",
    "jobInputDir6", "jobOutputDir6", "workDir6", "scratchDir6", "effUser6", tags, notes, "fakePassword6", prot1AccessMechName, prot1TxfMechs};


  @BeforeSuite
  public void setUp()
  {
    System.out.println("Executing BeforeSuite setup method");
    svc = new SystemsServiceImpl();
  }

  @Test
  public void testCreateSystem() throws Exception
  {
    String[] sys0 = sys1;
    int itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  sys0[15], sys0[16],
                                  prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  @Test
  public void testGetSystemByName() throws Exception
  {
    String[] sys0 = sys2;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                    sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                    prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                    prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystemByName(sys0[0], sys0[1], false);
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
    System.out.println("Found tags: " + tmpSys.getTags());
    System.out.println("Found notes: " + tmpSys.getNotes());
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

  @Test
  public void testGetSystemNames() throws Exception
  {
    String[] sys0 = sys3;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                   sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                   prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                   prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    prot0 = prot1;
    itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                   sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                   prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                   prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = svc.getSystemNames(tenant);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(sys3[1]), "List of systems did not contain system name: " + sys3[1]);
    Assert.assertTrue(systemNames.contains(sys4[1]), "List of systems did not contain system name: " + sys4[1]);
  }

  @Test
  public void testGetSystems() throws Exception
  {
    String[] sys0 = sys5;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                   sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                   prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                   prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = svc.getSystems(tenant);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  @Test
  public void testDelete() throws Exception
  {
    // Create the system
    String[] sys0 = sys6;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUserId, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                   sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                   prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                   prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);

    // Delete the system
    svc.deleteSystemByName(sys0[0], sys0[1]);
    TSystem tmpSys2 = svc.getSystemByName(sys0[0], sys0[1], false);
    Assert.assertNull(tmpSys2, "System not deleted. System name: " + sys0[1]);
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    svc.deleteSystemByName(sys1[0], sys1[1]);
    TSystem tmpSys = svc.getSystemByName(sys1[0], sys1[1], false);
    Assert.assertNull(tmpSys, "System not deleted. System name: " + sys1[1]);
    svc.deleteSystemByName(sys2[0], sys2[1]);
    svc.deleteSystemByName(sys3[0], sys3[1]);
    svc.deleteSystemByName(sys4[0], sys4[1]);
    svc.deleteSystemByName(sys5[0], sys5[1]);
    svc.deleteSystemByName(sys6[0], sys6[1]);
  }
}