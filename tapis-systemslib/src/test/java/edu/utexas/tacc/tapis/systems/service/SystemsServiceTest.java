package edu.utexas.tacc.tapis.systems.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.Credential;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMethod;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the SystemsService implementation class against a DB running locally
 * Note that this test has the following dependencies running locally or in dev
 *    Database - typically local
 *    Tenants service - typically dev
 *    Tokens service - typically dev and obtained from tenants service
 *    Security Kernel service - typically dev and obtained from tenants service
 */
@Test(groups={"integration"})
public class SystemsServiceTest
{
  private SystemsService svc;

  // Test data
  private static final String tenantName = "dev";
  private static final String apiUser = "testuser1";
  private static final String testUser2 = "testuser2";
  private static final List<TransferMethod> txfrMethodsList = new ArrayList<>(List.of(TransferMethod.SFTP, TransferMethod.S3));
  private static final Protocol prot1 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, -1, false, "",-1);
  private static final String prot1AccessMethName = prot1.getAccessMethod().name();
  private static final String accessMethName_CERT = AccessMethod.CERT.name();
  private static final String prot1TxfrMethods = prot1.getTransferMethodsAsString();
  private static final String tags = "{\"key1\": \"a\", \"key2\": \"b\"}";
  private static final String notes = "{\"project\": \"myproj1\", \"testdata\": \"abc\"}";
  private static final List<String> testPerms = new ArrayList<>(List.of(Permission.READ.name(), Permission.MODIFY.name(),
                                                                        Permission.DELETE.name()));
  private static final String[] sys1 = {tenantName, "Ssys1", "description 1", SystemType.LINUX.name(), "owner1", "host1",
          "effUser1", prot1AccessMethName, "fakePassword1", "bucket1", "/root1", prot1TxfrMethods,
          "jobLocalWorkDir1", "jobLocalArchDir1", "jobRemoteArchSystem1", "jobRemoteArchDir1", tags, notes, "{}"};
  private static final String[] sys2 = {tenantName, "Ssys2", "description 2", SystemType.LINUX.name(), "owner2", "host2",
          "effUser2", prot1AccessMethName, "fakePassword2", "bucket2", "/root2", prot1TxfrMethods,
          "jobLocalWorkDir2", "jobLocalArchDir2", "jobRemoteArchSystem2", "jobRemoteArchDir2", tags, notes, "{}"};
  private static final String[] sys3 = {tenantName, "Ssys3", "description 3", SystemType.LINUX.name(), "owner3", "host3",
          "effUser3", prot1AccessMethName, "fakePassword3", "bucket3", "/root3", prot1TxfrMethods,
          "jobLocalWorkDir3", "jobLocalArchDir3", "jobRemoteArchSystem3", "jobRemoteArchDir3", tags, notes, "{}"};
  private static final String[] sys4 = {tenantName, "Ssys4", "description 4", SystemType.LINUX.name(), "owner4", "host4",
          "effUser4", prot1AccessMethName, "fakePassword4", "bucket4", "/root4", prot1TxfrMethods,
          "jobLocalWorkDir4", "jobLocalArchDir4", "jobRemoteArchSystem4", "jobRemoteArchDir4", tags, notes, "{}"};
  private static final String[] sys5 = {tenantName, "Ssys5", "description 5", SystemType.LINUX.name(), "owner5", "host5",
          "effUser5", prot1AccessMethName, "fakePassword5", "bucket5", "/root5", prot1TxfrMethods,
          "jobLocalWorkDir5", "jobLocalArchDir5", "jobRemoteArchSystem5", "jobRemoteArchDir5", tags, notes, "{}"};
  private static final String[] sys6 = {tenantName, "Ssys6", "description 6", SystemType.LINUX.name(), "owner6", "host6",
          "effUser6", prot1AccessMethName, "fakePassword6", "bucket6", "/root6", prot1TxfrMethods,
          "jobLocalWorkDir6", "jobLocalArchDir6", "jobRemoteArchSystem6", "jobRemoteArchDir6", tags, notes, "{}"};
  private static final String[] sys7 = {tenantName, "Ssys7", "description 7", SystemType.LINUX.name(), "owner7", "host7",
          "effUser7", prot1AccessMethName, "fakePassword7", "bucket7", "/root7", prot1TxfrMethods,
          "jobLocalWorkDir7", "jobLocalArchDir7", "jobRemoteArchSystem7", "jobRemoteArchDir7", tags, notes, "{}"};
  private static final String[] sys8 = {tenantName, "Ssys8", "description 8", SystemType.LINUX.name(), "${apiUserId}", "host8",
          "${owner}", prot1AccessMethName, "fakePassword8", "bucket8-${tenant}-${apiUserId}", "/root8/${tenant}", prot1TxfrMethods,
          "jobLocalWorkDir8/${owner}/${tenant}/${apiUserId}", "jobLocalArchDir8/${apiUserId}", "jobRemoteArchSystem8",
          "jobRemoteArchDir8${owner}${tenant}${apiUserId}", tags, notes, "{}"};
  private static final String[] sys9 = {tenantName, "Ssys9", "description 9", SystemType.LINUX.name(), "owner9", "host9",
          "effUser9", accessMethName_CERT, "fakePassword", "bucket9", "/root9", prot1TxfrMethods,
          "jobLocalWorkDir9", "jobLocalArchDir9", "jobRemoteArchSystem9", "jobRemoteArchDir9", tags, notes, "{}"};
  private static final String[] sysA = {tenantName, "SsysA", "description A", SystemType.LINUX.name(), "ownerA", "hostA",
          "effUserA", prot1AccessMethName, "fakePasswordA", "bucketA", "/rootA", prot1TxfrMethods,
          "jobLocalWorkDirA", "jobLocalArchDirA", "jobRemoteArchSystemA", "jobRemoteArchDirA", tags, notes, "{}"};
  private static final String[] sysB = {tenantName, "SsysB", "description B", SystemType.LINUX.name(), "ownerB", "hostB",
          "effUserB", prot1AccessMethName, "fakePasswordB", "bucketB", "/rootB", prot1TxfrMethods,
          "jobLocalWorkDirB", "jobLocalArchDirB", "jobRemoteArchSystemB", "jobRemoteArchDirB", tags, notes, "{}"};

  private static final Capability capA1 = new Capability(Category.SCHEDULER, "Type", "Slurm");
  private static final Capability capB1 = new Capability(Category.HARDWARE, "CoresPerNode", "4");
  private static final Capability capC1 = new Capability(Category.SOFTWARE, "OpenMP", "4.5");
  private static final Capability capA2 = new Capability(Category.SCHEDULER, "Type", "Slurm");
  private static final Capability capB2 = new Capability(Category.HARDWARE, "CoresPerNode", "4");
  private static final Capability capC2 = new Capability(Category.SOFTWARE, "OpenMP", "4.5");
  private static final Capability capD2 = new Capability(Category.CONTAINER, "Singularity", null);
  private static final List<Capability> cap1List = new ArrayList<>(List.of(capA1, capB1, capC1));
  private static final List<Capability> cap2List = new ArrayList<>(List.of(capA2, capB2, capC2, capD2));

  @BeforeSuite
  public void setUp() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    // Initialize TenantManager and services
    String url = RuntimeParameters.getInstance().getTenantsSvcURL();
    TenantManager.getInstance(url).getTenants();
    svc = new SystemsServiceImpl();
    // Cleanup anything leftover from previous failed run
    tearDown();
  }

  @Test
  public void testCreateSystem() throws Exception
  {
    String[] sys0 = sys1;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], cap1List, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  @Test
  public void testGetSystemByName() throws Exception
  {
    String[] sys0 = sys2;
    Protocol prot0 = prot1;
    Credential cred0 = new Credential(sys0[8], "fakePrivateKey", "fakePublicKey",
            "fakeCert","fakeAccessKey", "fakeAccessSecret");
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], cap2List, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Retrieve the system including the credential using the default access method defined for the system
    TSystem tmpSys = svc.getSystemByName(sys0[0], sys0[1], apiUser, true, null);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0[3]);
    Assert.assertEquals(tmpSys.getOwner(), sys0[4]);
    Assert.assertEquals(tmpSys.getHost(), sys0[5]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0[6]);
    Assert.assertEquals(tmpSys.getDefaultAccessMethod().name(), sys0[7]);
    Assert.assertEquals(tmpSys.getBucketName(), sys0[9]);
    Assert.assertEquals(tmpSys.getRootDir(), sys0[10]);
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0[12]);
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0[13]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0[14]);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0[15]);
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    // Verify transfer methods
    List<TransferMethod> txfrMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(txfrMethodsList);
    Assert.assertTrue(txfrMethodsList.contains(TransferMethod.S3), "List of transfer methods did not contain: " + TransferMethod.S3.name());
    Assert.assertTrue(txfrMethodsList.contains(TransferMethod.SFTP), "List of transfer methods did not contain: " + TransferMethod.SFTP.name());
    // Verify credentials. Only cred for default accessMethod is returned. In this case PKI_KEYS.
    Assert.assertEquals(tmpSys.getAccessCredential().getPublicKey(), cred0.getPublicKey());
    Assert.assertEquals(tmpSys.getAccessCredential().getPrivateKey(), cred0.getPrivateKey());
    // Verify capabilities
    List<Capability> jobCaps = tmpSys.getJobCapabilities();
    Assert.assertNotNull(jobCaps);
    Assert.assertEquals(jobCaps.size(), cap2List.size());
    var capNamesFound = new ArrayList<String>();
    for (Capability capFound : jobCaps) {capNamesFound.add(capFound.getName());}
    for (Capability capSeed : cap2List)
    {
      Assert.assertTrue(capNamesFound.contains(capSeed.getName()), "List of capabilities did not contain a capability named: " + capSeed.getName());
    }
    // Retrieve tags, verify keys and values
    String tagsStr = tmpSys.getTags();
    System.out.println("Found tags: " + tagsStr);
    String notesStr = tmpSys.getNotes();
    System.out.println("Found notes: " + notesStr);
    Assert.assertFalse(StringUtils.isBlank(tagsStr), "Tags string not found");
    Assert.assertFalse(StringUtils.isBlank(notesStr), "Notes string not found");
    JsonObject obj = null;
    if (!StringUtils.isBlank(tagsStr)) obj = JsonParser.parseString(tagsStr).getAsJsonObject();
    Assert.assertNotNull(obj, "Tags value not found");
    Assert.assertTrue(obj.has("key1"));
    Assert.assertEquals(obj.get("key1").getAsString(), "a");
    Assert.assertTrue(obj.has("key2"));
    Assert.assertEquals(obj.get("key2").getAsString(), "b");
    // Retrieve notes, verify elements
    if (!StringUtils.isBlank(notesStr)) obj = JsonParser.parseString(notesStr).getAsJsonObject();
    Assert.assertNotNull(obj, "Notes value not found");
    Assert.assertTrue(obj.has("project"));
    Assert.assertEquals(obj.get("project").getAsString(), "myproj1");
    Assert.assertTrue(obj.has("testdata"));
    Assert.assertEquals(obj.get("testdata").getAsString(), "abc");
  }

  // Check that when a system is created variable substitution is correct for:
  //   owner, bucketName, rootDir, jobInputDir, jobOutputDir, workDir, scratchDir
  // And when system is retrieved effectiveUserId is resolved
  @Test
  public void testGetSystemByNameWithVariables() throws Exception
  {
    String[] sys0 = sys8;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystemByName(sys0[0], sys0[1], apiUser, false, null);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);

// sys8 = {tenantName, "Ssys8", "description 8", SystemType.LINUX.name(), "${apiUserId}", "host8",
//         "${owner}", prot1AccessMethName, "fakePassword8", "bucket8-${tenant}-${apiUserId}", "/root8/${tenant}", prot1TxfrMethods,
//         "jobLocalWorkDir8/${owner}/${tenant}/${apiUserId}", "jobLocalArchDir8/${apiUserId}", "jobRemoteArchSystem8",
//         "jobRemoteArchDir8${owner}${tenant}${apiUserId}", tags, notes, "{}"};
    String owner = apiUser;
    String effectiveUserId = owner;
    String bucketName = "bucket8-" + tenantName + "-" + apiUser;
    String rootDir = "/root8/" + tenantName;
    String jobLocalWorkingDir = "jobLocalWorkDir8/" + owner + "/" + tenantName + "/" + apiUser;
    String jobLocalArchiveDir = "jobLocalArchDir8/" + apiUser;
    String jobRemoteArchiveDir = "jobRemoteArchDir8" + owner + tenantName + apiUser;
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
    Assert.assertEquals(tmpSys.getDescription(), sys0[2]);
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0[3]);
    Assert.assertEquals(tmpSys.getOwner(), owner);
    Assert.assertEquals(tmpSys.getHost(), sys0[5]);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), effectiveUserId);
    Assert.assertEquals(tmpSys.getDefaultAccessMethod().name(), sys0[7]);
    Assert.assertEquals(tmpSys.getBucketName(), bucketName);
    Assert.assertEquals(tmpSys.getRootDir(), rootDir);
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), jobLocalWorkingDir);
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), jobLocalArchiveDir);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), jobRemoteArchiveDir);
    System.out.println("Found tags: " + tmpSys.getTags());
    System.out.println("Found notes: " + tmpSys.getNotes());
    Assert.assertEquals(tmpSys.getPort(), prot0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), prot0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), prot0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), prot0.getProxyPort());
    List<TransferMethod> txfrMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(txfrMethodsList);
    Assert.assertTrue(txfrMethodsList.contains(TransferMethod.S3), "List of transfer methods did not contain: " + TransferMethod.S3.name());
    Assert.assertTrue(txfrMethodsList.contains(TransferMethod.SFTP), "List of transfer methods did not contain: " + TransferMethod.SFTP.name());
  }

  @Test
  public void testGetSystemNames() throws Exception
  {
    String[] sys0 = sys3;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    prot0 = prot1;
    itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
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
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
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
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);

    // Delete the system
    svc.deleteSystemByName(sys0[0], sys0[1]);
    TSystem tmpSys2 = svc.getSystemByName(sys0[0], sys0[1], apiUser, false, null);
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
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(sys7[0], sys7[1]));
  }

  // Check that if systems already exists we get an IllegalStateException when attempting to create
  @Test(expectedExceptions = {IllegalStateException.class})
  public void testCreateSystemAlreadyExists() throws Exception
  {
    // Create the system
    String[] sys0 = sys9;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(sys9[0], sys9[1]));
    // Now attempt to create again, should get IllegalStateException
    svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    String[] sys0 = sysA;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
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

  // Test creating, reading and deleting user credentials for a system
  @Test
  public void testUserCredentials() throws Exception
  {
    // Create a system
    String[] sys0 = sysB;
    Protocol prot0 = prot1;
    Credential cred0 = null;
    int itemId = svc.createSystem(sys0[0], apiUser, sys0[1], sys0[2], sys0[3], sys0[4], sys0[5], true, sys0[6], AccessMethod.valueOf(sys0[7]),
            cred0, sys0[9], sys0[10], sys0[11], prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),
            false, sys0[12], sys0[13], sys0[14], sys0[15], null, sys0[16], sys0[17], sys0[18]);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    cred0 = new Credential(sys0[8], "fakePrivateKey", "fakePublicKey",
            "fakeCert","fakeAccessKey", "fakeAccessSecret");
    // Store and retrieve multiple secret types: password, ssh keys, access key and secret
    svc.createUserCredential(sys0[0], sys0[1], testUser2, cred0);
    Credential cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.PASSWORD);
    // Verify credentials
    Assert.assertEquals(cred1.getPassword(), cred0.getPassword());
    cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.PKI_KEYS);
    Assert.assertEquals(cred1.getPublicKey(), cred0.getPublicKey());
    Assert.assertEquals(cred1.getPrivateKey(), cred0.getPrivateKey());
    cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Delete credentials and verify they were destroyed
    svc.deleteUserCredential(sys0[0], sys0[1], testUser2);
    cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.PASSWORD);
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0[1] + " User name: " + testUser2);

    // Attempt to delete again, should not throw an exception
    svc.deleteUserCredential(sys0[0], sys0[1], testUser2);

    // Set just ACCESS_KEY only and test
    cred0 = new Credential(null, null, null, null,"fakeAccessKey2", "fakeAccessSecret2");
    svc.createUserCredential(sys0[0], sys0[1], testUser2, cred0);
    cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Attempt to retrieve secret that has not been set
    cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.PKI_KEYS);
    Assert.assertNull(cred1, "Credential was non-null for missing secret. System name: " + sys0[1] + " User name: " + testUser2);
    // Delete credentials and verify they were destroyed
    svc.deleteUserCredential(sys0[0], sys0[1], testUser2);
    try {
      cred1 = svc.getUserCredential(sys0[0], sys0[1], testUser2, AccessMethod.ACCESS_KEY);
    } catch (TapisClientException tce) {
      cred1 = null;
    }
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0[1] + " User name: " + testUser2);
  }

  // Test various cases when system is missing
  //  - get owner with no system
  //  - get perm with no system
  //  - grant perm with no system
  //  - revoke perm with no system
  //  - get credential with no system
  //  - create credential with no system
  //  - delete credential with no system
  @Test
  public void testMissingSystem() throws Exception
  {
    String fakeSystemName = "AMissingSystemName";
    String fakeUserName = "AMissingUserName";
    // Make sure system does not exist
    Assert.assertFalse(svc.checkForSystemByName(tenantName, fakeSystemName));
    // Get owner with no system should return null
    String owner = svc.getSystemOwner(tenantName, fakeSystemName);
    Assert.assertNull(owner, "Owner not null for non-existent system");
    // Get perm with no system should return empty list
    List<String> perms = svc.getUserPermissions(tenantName, fakeSystemName, fakeUserName);
    Assert.assertNotNull(perms, "Perms list was null for non-existent system");
    Assert.assertTrue(perms.isEmpty(), "Perms list not empty non-existent system");
    // Grant perm with no system should TODO TBD - handle this on front end.
    // TODO Currently systemslib does not check that system exists, it simple creates the permSpec entries in SK
    // TODO svc.grantUserPermissions(tenantName, fakeSystemName, fakeUserName, testPerms);

    // TODO revoke perm with no system

    // TODO/TBD: If system does not exist getCred should return null.
    Credential credential = svc.getUserCredential(tenantName, fakeSystemName, fakeUserName, AccessMethod.PKI_KEYS);
    Assert.assertNull(credential, "Credential was non-null for missing system. System name: " + fakeSystemName + " User name: " + fakeUserName);

    // Create credential with no system should TODO TBD - handle this on front end.
    // TODO Currently systemslib does not check that system exists, it simple creates the credential in SK
    credential = new Credential(null, null, null, null,"fakeAccessKey2", "fakeAccessSecret2");
    // TODO svc.createUserCredential(tenantName, fakeSystemName, fakeUserName, credential);

    // TODO delete credential with no system

  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    svc.deleteSystemByName(sys1[0], sys1[1]);
    TSystem tmpSys = svc.getSystemByName(sys1[0], sys1[1], apiUser, false, null);
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
    svc.deleteSystemByName(sysB[0], sysB[1]);
  }
}