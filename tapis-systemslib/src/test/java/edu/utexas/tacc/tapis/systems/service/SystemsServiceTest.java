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
  private static final String tenantName = "dev";
  private static final String apiUser = "testuser1";
  private static final String testUser2 = "testuser2";
  private static final List<TransferMechanism> mechList = new ArrayList<>(List.of(TransferMechanism.SFTP,TransferMechanism.S3));
  private static final Protocol prot1 = new Protocol(AccessMechanism.NONE, mechList, -1, false, "",-1);
  private static final String prot1AccessMechName = prot1.getAccessMechanism().name();
  private static final String accessMechNameSSH_CERT = AccessMechanism.SSH_CERT.name();
  private static final String prot1TxfMechs = prot1.getTransferMechanismsAsStr();
  private static final String tags = "{\"key1\":\"a\", \"key2\":\"b\"}";
  private static final String notes = "{\"project\":\"myproj1\", \"testdata\":\"abc\"}";
  private static final List<String> testPerms = new ArrayList<>(List.of(TSystem.Permissions.READ.name(),TSystem.Permissions.MODIFY.name(),
                                                                        TSystem.Permissions.DELETE.name()));

  private static final String[] sys1 = {tenantName, "Ssys1", "description 1", "owner1", "host1", "bucket1", "/root1",
    "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", tags, notes, "fakePassword1", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys2 = {tenantName, "Ssys2", "description 2", "owner2", "host2", "bucket2", "/root2",
    "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", tags, notes, "fakePassword2", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys3 = {tenantName, "Ssys3", "description 3", "owner3", "host3", "bucket3", "/root3",
    "jobInputDir3", "jobOutputDir3", "workDir3", "scratchDir3", "effUser3", tags, notes, "fakePassword3", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys4 = {tenantName, "Ssys4", "description 4", "owner4", "host4", "bucket4", "/root4",
    "jobInputDir4", "jobOutputDir4", "workDir4", "scratchDir4", "effUser4", tags, notes, "fakePassword4", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys5 = {tenantName, "Ssys5", "description 5", "owner5", "host5", "bucket5", "/root5",
    "jobInputDir5", "jobOutputDir5", "workDir5", "scratchDir5", "effUser5", tags, notes, "fakePassword5", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys6 = {tenantName, "Ssys6", "description 6", "owner6", "host6", "bucket6", "/root6",
    "jobInputDir6", "jobOutputDir6", "workDir6", "scratchDir6", "effUser6", tags, notes, "fakePassword6", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys7 = {tenantName, "Ssys7", "description 7", "owner7", "host7", "bucket7", "/root7",
    "jobInputDir7", "jobOutputDir7", "workDir7", "scratchDir7", "effUser7", tags, notes, "fakePassword7", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys8 = {tenantName, "Ssys8", "description 8", "${apiUserId}", "host8", "bucket8-${tenant}-${apiUserId}",
    "/root8/${tenant}", "jobInputDir8/home/${apiUserId}/input", "jobOutputDir8/home/${apiUserId}/output", "workDir8/home/${apiUserId}/tapis/data",
    "scratchDir8/home/${owner}/${tenant}/${apiUserId}", "${owner}", tags, notes, "fakePassword8", prot1AccessMechName, prot1TxfMechs};
  private static final String[] sys9 = {tenantName, "Ssys9", "description 9", "owner9", "host9", "bucket9", "/root9",
    "jobInputDir9", "jobOutputDir9", "workDir9", "scratchDir9", "effUser9", tags, notes, "fakePassword9", accessMechNameSSH_CERT, prot1TxfMechs};
  private static final String[] sysA = {tenantName, "SsysA", "description A", "ownerA", "hostA", "bucketA", "/rootA",
    "jobInputDirA", "jobOutputDirA", "workDirA", "scratchDirA", "effUserA", tags, notes, "fakePasswordA", prot1AccessMechName, prot1TxfMechs};


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
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
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
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystemByName(sys0[0], sys0[1], apiUser, false);
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

  // Check that when a system is created variable substitution is correct for:
  //   owner, bucketName, rootDir, jobInputDir, jobOutputDir, workDir, scratchDir
  // And when system is retrieved effectiveUserId is resolved
  @Test
  public void testGetSystemByNameWithVariables() throws Exception
  {
    // TODO
    String[] sys0 = sys8;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystemByName(sys0[0], sys0[1], apiUser, false);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);

// sys8 = {tenant, "Ssys8", "description 8", "${apiUserId}", "host8", "bucket8-${tenant}-${apiUserId}",
//   "/root8/${tenant}", "jobInputDir8/home/${apiUserId}/input", "jobOutputDir8/home/${apiUserId}/output", "workDir8/home/${apiUserId}/tapis/data",
//   "scratchDir8/home/${owner}/${tenant}/${apiUserId}", "${owner}", tags, notes, "fakePassword8", prot1AccessMechName, prot1TxfMechs};
    String owner = apiUser;
    String bucketName = "bucket8-" + tenantName + "-" + apiUser;
    String rootDir = "/root8/" + tenantName;
    String jobInputDir = "jobInputDir8/home/" + apiUser + "/input";
    String jobOutputDir = "jobOutputDir8/home/" + apiUser + "/output";
    String workDir = "workDir8/home/" + apiUser + "/tapis/data";
    String scratchDir = "scratchDir8/home/" + owner + "/" + tenantName + "/" + apiUser;
    String effectiveUserId = owner;
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getOwner(), owner);
    Assert.assertEquals(tmpSys.getHost(), sys0[4]);
    Assert.assertEquals(tmpSys.getBucketName(), bucketName);
    Assert.assertEquals(tmpSys.getRootDir(), rootDir);
    Assert.assertEquals(tmpSys.getJobInputDir(), jobInputDir);
    Assert.assertEquals(tmpSys.getJobOutputDir(), jobOutputDir);
    Assert.assertEquals(tmpSys.getWorkDir(), workDir);
    Assert.assertEquals(tmpSys.getScratchDir(), scratchDir);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), effectiveUserId);
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
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    prot0 = prot1;
    itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                              sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                              prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                              prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = svc.getSystemNames(tenantName);
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
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = svc.getSystems(tenantName, apiUser);
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
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);

    // Delete the system
    svc.deleteSystemByName(sys0[0], sys0[1]);
    TSystem tmpSys2 = svc.getSystemByName(sys0[0], sys0[1], apiUser, false);
    Assert.assertNull(tmpSys2, "System not deleted. System name: " + sys0[1]);
  }

  @Test
  public void testSystemExists() throws Exception
  {
    // If system not there we should get false
    Assert.assertFalse(svc.checkForSystemByName(sys7[0], sys7[1]));
    // After creating system we should get true
    String[] sys0 = sys7;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(sys7[0], sys7[1]));
  }

  // Check that if systems already exists we get an IllegalStateException
  @Test(expectedExceptions = {IllegalStateException.class})
  public void testSystemAlreadyExists() throws Exception
  {
    // Create the system
    String[] sys0 = sys9;
    Protocol prot0 = prot1;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                                  prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(sys9[0], sys9[1]));
    // Now attempt to create again, should get IllegalStateException
    svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                     sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                     prot0.getAccessMechanism().name(), prot0.getTransferMechanismsAsStr(), prot0.getPort(),
                     prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), "");
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    String[] sys0 = sysA;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                                  sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], sys0[12], sys0[13], sys0[14],
                                  sys0[15], sys0[16],
                                  prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(), "");
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Create user perms for the system
    svc.grantUserPermissions(sys0[0], sys0[1], testUser2, testPerms);
    // Get the system perms for the user and make sure permissions are there
    List<String> userPerms = svc.getUserPermissions(sys0[0], sys0[1], testUser2);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPerms.size(), "Incorrect number of perms returned.");
    for (String perm: testPerms) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm); }
    // Remove perms for the user
    svc.revokeUserPermissions(sys0[0], sys0[1], testUser2, testPerms);
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(sys0[0], sys0[1], testUser2);
    for (String perm: testPerms) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm); }
  }


  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    svc.deleteSystemByName(sys1[0], sys1[1]);
    TSystem tmpSys = svc.getSystemByName(sys1[0], sys1[1], apiUser, false);
    Assert.assertNull(tmpSys, "System not deleted. System name: " + sys1[1]);
    svc.deleteSystemByName(sys2[0], sys2[1]);
    svc.deleteSystemByName(sys3[0], sys3[1]);
    svc.deleteSystemByName(sys4[0], sys4[1]);
    svc.deleteSystemByName(sys5[0], sys5[1]);
    svc.deleteSystemByName(sys6[0], sys6[1]);
    svc.deleteSystemByName(sys7[0], sys7[1]);
    svc.deleteSystemByName(sys8[0], sys8[1]);
    svc.deleteSystemByName(sys9[0], sys9[1]);
    svc.deleteSystemByName(sysA[0], sysA[1]);
  }
}