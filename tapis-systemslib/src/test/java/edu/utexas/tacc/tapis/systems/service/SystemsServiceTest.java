package edu.utexas.tacc.tapis.systems.service;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.Protocol;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.Credential;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
  private AuthenticatedUser authenticatedUser, authenticatedService;
  // Test data
  private static final String tenantName = "dev";
  private static final String apiUser = "owner1";
  private static final String masterTenantName = "master";
  private static final String filesSvcName = "files";
  private static final String testUser2 = "testuser2";
  private static final List<TransferMethod> txfrMethodsList = new ArrayList<>(List.of(TransferMethod.SFTP, TransferMethod.S3));
  private static final List<TransferMethod> txfrMethodsEmpty = new ArrayList<>();
  private static final Protocol prot1 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, -1, false, "",-1);
  private static final Protocol prot2 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, 22, false, "",0);
  private static final Protocol prot3 = new Protocol(AccessMethod.ACCESS_KEY, txfrMethodsList, 23, true, "localhost",22);
  private static final Protocol prot4 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot5 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, null,-1);
  private static final Protocol prot6 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot7 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot8 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, -1, false, "",-1);
  private static final Protocol prot9 = new Protocol(AccessMethod.CERT, txfrMethodsList, -1, false, "",-1);
  private static final Protocol protA = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, -1, false, "",-1);
  private static final Protocol protB = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, -1, false, "",-1);
  private static final Protocol protC = new Protocol(AccessMethod.PKI_KEYS, null, -1, false, null,-1);
  private static final Set<Permission> testPerms = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  private static final String[] tags = {"value1", "value2", "a",
      "a long tag with spaces and numbers (1 3 2) and special characters [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  private static final String notes = "{\"project\": \"myproj1\", \"testdata\": \"abc\"}";
  private static final String scrubbedJson = "{}";
  private static JsonObject notesJO = TapisGsonUtils.getGson().fromJson(notes, JsonObject.class);
  TSystem sys1 = new TSystem(-1, tenantName, "Ssys1", "description 1", SystemType.LINUX, "owner1", "host1", true,
          "effUser1", prot1.getAccessMethod(), null,"bucket1", "/root1", prot1.getTransferMethods(),
          prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),false,
          "jobLocalWorkDir1", "jobLocalArchDir1", "jobRemoteArchSystem1","jobRemoteArchDir1",
          null, tags, notesJO, null, null);
  TSystem sys2 = new TSystem(-1, tenantName, "Ssys2", "description 2", SystemType.LINUX, "owner1", "host2", true,
          "effUser2", prot2.getAccessMethod(), null,"bucket2", "/root2", prot2.getTransferMethods(),
          prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),false,
          "jobLocalWorkDir2", "jobLocalArchDir2", "jobRemoteArchSystem2","jobRemoteArchDir2",
          null, tags, notesJO, null, null);
  TSystem sys3 = new TSystem(-1, tenantName, "Ssys3", "description 3", SystemType.OBJECT_STORE, "owner1", "host3", true,
          "effUser3", prot3.getAccessMethod(), null,"bucket3", "/root3", prot3.getTransferMethods(),
          prot3.getPort(), prot3.isUseProxy(), prot3.getProxyHost(), prot3.getProxyPort(),false,
          "jobLocalWorkDir3", "jobLocalArchDir3", "jobRemoteArchSystem3","jobRemoteArchDir3",
          null, tags, notesJO, null, null);
  TSystem sys4 = new TSystem(-1, tenantName, "Ssys4", "description 4", SystemType.LINUX, "owner1", "host4", true,
          "effUser4", prot4.getAccessMethod(), null,"bucket4", "/root4", prot4.getTransferMethods(),
          prot4.getPort(), prot4.isUseProxy(), prot4.getProxyHost(), prot4.getProxyPort(),false,
          "jobLocalWorkDir4", "jobLocalArchDir4", "jobRemoteArchSystem4","jobRemoteArchDir4",
          null, tags, notesJO, null, null);
  TSystem sys5 = new TSystem(-1, tenantName, "Ssys5", "description 5", SystemType.LINUX, "owner1", "host5", true,
          "effUser5", prot5.getAccessMethod(), null,"bucket5", "/root5", prot5.getTransferMethods(),
          prot5.getPort(), prot5.isUseProxy(), prot5.getProxyHost(), prot5.getProxyPort(),false,
          "jobLocalWorkDir5", "jobLocalArchDir5", "jobRemoteArchSystem5","jobRemoteArchDir5",
          null, tags, notesJO, null, null);
  TSystem sys6 = new TSystem(-1, tenantName, "Ssys6", "description 6", SystemType.LINUX, "owner1", "host6", true,
          "effUser6", prot6.getAccessMethod(), null,"bucket6", "/root6", prot6.getTransferMethods(),
          prot6.getPort(), prot6.isUseProxy(), prot6.getProxyHost(), prot6.getProxyPort(),false,
          "jobLocalWorkDir6", "jobLocalArchDir6", "jobRemoteArchSystem6","jobRemoteArchDir6",
          null, tags, notesJO, null, null);
  TSystem sys7 = new TSystem(-1, tenantName, "Ssys7", "description 7", SystemType.LINUX, "owner1", "host7", true,
          "effUser7", prot7.getAccessMethod(), null,"bucket7", "/root7", prot7.getTransferMethods(),
          prot7.getPort(), prot7.isUseProxy(), prot7.getProxyHost(), prot7.getProxyPort(),false,
          "jobLocalWorkDir7", "jobLocalArchDir7", "jobRemoteArchSystem7","jobRemoteArchDir7",
          null, tags, notesJO, null, null);
  TSystem sys8 = new TSystem(-1, tenantName, "Ssys8", "description 8", SystemType.LINUX, "${apiUserId}", "host8", false,
          "${owner}", prot8.getAccessMethod(), null,"bucket8-${tenant}-${apiUserId}", "/root8/${tenant}",
          prot8.getTransferMethods(), prot8.getPort(), prot8.isUseProxy(), prot8.getProxyHost(), prot8.getProxyPort(),false,
          "jobLocalWorkDir8/${owner}/${tenant}/${apiUserId}", "jobLocalArchDir8/${apiUserId}",
          "jobRemoteArchSystem8","jobRemoteArchDir8${owner}${tenant}${apiUserId}",
          null, tags, notesJO, null, null);
  TSystem sys9 = new TSystem(-1, tenantName, "Ssys9", "description 9", SystemType.LINUX, "owner1", "host9", true,
          "owner1", prot9.getAccessMethod(), null,"bucket9", "/root9", prot9.getTransferMethods(),
          prot9.getPort(), prot9.isUseProxy(), prot9.getProxyHost(), prot9.getProxyPort(),false,
          "jobLocalWorkDir9", "jobLocalArchDir9", "jobRemoteArchSystem9","jobRemoteArchDir9",
          null, tags, notesJO, null, null);
  TSystem sysA = new TSystem(-1, tenantName, "SsysA", "description A", SystemType.LINUX, "owner1", "hostA", true,
          "effUserA", protA.getAccessMethod(), null,"bucketA", "/rootA", protA.getTransferMethods(),
          protA.getPort(), protA.isUseProxy(), protA.getProxyHost(), protA.getProxyPort(),false,
          "jobLocalWorkDirA", "jobLocalArchDirA", "jobRemoteArchSystemA","jobRemoteArchDirA",
          null, tags, notesJO, null, null);
  TSystem sysB = new TSystem(-1, tenantName, "SsysB", "description B", SystemType.LINUX, "owner1", "hostB", true,
          "effUserB", protB.getAccessMethod(), null,"bucketB", "/rootB", protB.getTransferMethods(),
          protB.getPort(), protB.isUseProxy(), protB.getProxyHost(), protB.getProxyPort(),false,
          "jobLocalWorkDirB", "jobLocalArchDirB", "jobRemoteArchSystemB","jobRemoteArchDirB",
          null, tags, notesJO, null, null);
  TSystem sysC = new TSystem(-1, tenantName, "SsysC", null, SystemType.LINUX, null, "hostC", true,
          null, protC.getAccessMethod(), null,null, null, protC.getTransferMethods(),
          protC.getPort(), protC.isUseProxy(), protC.getProxyHost(), protC.getProxyPort(),false,
          null, null, null, null,
          null, null, null, null, null);

//  private static final Capability capA1 = new Capability(Category.SCHEDULER, "Type", "Slurm");
//  private static final Capability capB1 = new Capability(Category.HARDWARE, "CoresPerNode", "4");
//  private static final Capability capC1 = new Capability(Category.SOFTWARE, "OpenMP", "4.5");
//  private static final List<Capability> cap1List = new ArrayList<>(List.of(capA1, capB1, capC1));
  private static final Capability capA2 = new Capability(Category.SCHEDULER, "Type", "Slurm");
  private static final Capability capB2 = new Capability(Category.HARDWARE, "CoresPerNode", "4");
  private static final Capability capC2 = new Capability(Category.SOFTWARE, "OpenMP", "4.5");
  private static final Capability capD2 = new Capability(Category.CONTAINER, "Singularity", null);
  private static final List<Capability> cap2List = new ArrayList<>(List.of(capA2, capB2, capC2, capD2));

  @BeforeSuite
  public void setUp() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    // Setup for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure() {
        bind(SystemsServiceImpl.class).to(SystemsService.class);
        bind(SystemsDaoImpl.class).to(SystemsDao.class);
        bindFactory(SystemsServiceJWTFactory.class).to(ServiceJWT.class);
        bind(SKClient.class).to(SKClient.class);
      }
    });
    locator.inject(this);

    // Initialize TenantManager and services
    String url = RuntimeParameters.getInstance().getTenantsSvcURL();
    TenantManager.getInstance(url).getTenants();

    // Initialize services
    svc = locator.getService(SystemsService.class);

    // Initialize authenticated user and service
    authenticatedUser = new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(), null, apiUser, tenantName, null, null);
    authenticatedService = new AuthenticatedUser(filesSvcName, masterTenantName, TapisThreadContext.AccountType.service.name(), null, apiUser, tenantName, null, null);

    // Cleanup anything leftover from previous failed run
    tearDown();
  }

  @Test
  public void testCreateSystem() throws Exception
  {
    TSystem sys0 = sys1;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Create a system using minimal attributes:
  //   name, systemType, host, defaultAccessMethod, jobCanExec
  @Test
  public void testCreateSystemMinimal() throws Exception
  {
    TSystem sys0 = sysC;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a system including default access method
  //   and test retrieving for specified access method.
  @Test
  public void testGetSystemByName() throws Exception
  {
    TSystem sys0 = sys2;
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAccessCredential(cred0);
    sys0.setJobCapabilities(cap2List);
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Retrieve the system including the credential using the default access method defined for the system
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    TSystem tmpSys = svc.getSystemByName(authenticatedService, sys0.getName(), true, null);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getName());
    System.out.println("Found item: " + sys0.getName());
    Assert.assertEquals(tmpSys.getName(), sys0.getName());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getDefaultAccessMethod().name(), sys0.getDefaultAccessMethod().name());
    Assert.assertEquals(tmpSys.isEnabled(), sys0.isEnabled());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0.getJobLocalWorkingDir());
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0.getJobLocalArchiveDir());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0.getJobRemoteArchiveSystem());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0.getJobRemoteArchiveDir());
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    // Verify credentials. Only cred for default accessMethod is returned. In this case PKI_KEYS.
    Credential cred = tmpSys.getAccessCredential();
    Assert.assertNotNull(cred, "AccessCredential should not be null");
    Assert.assertEquals(cred.getPrivateKey(), cred0.getPrivateKey());
    Assert.assertEquals(cred.getPublicKey(), cred0.getPublicKey());
    Assert.assertNull(cred.getPassword(), "AccessCredential password should be null");
    Assert.assertNull(cred.getAccessKey(), "AccessCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AccessCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AccessCredential certificate should be null");
    // Verify transfer methods
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    for (TransferMethod txfrMethod : sys0.getTransferMethods())
    {
      Assert.assertTrue(tMethodsList.contains(txfrMethod), "List of transfer methods did not contain: " + txfrMethod.name());
    }
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
    // Verify tags
    String[] tmpTags = tmpSys.getTags();
    Assert.assertNotNull(tmpTags, "Tags value was null");
    var tagsList = Arrays.asList(tmpTags);
    Assert.assertEquals(tmpTags.length, tags.length, "Wrong number of tags.");
    for (String tagStr : tags)
    {
      Assert.assertTrue(tagsList.contains(tagStr));
      System.out.println("Found tag: " + tagStr);
    }
    // Verify notes
    JsonObject obj = tmpSys.getNotes();
    String notesStr = obj.toString();
    System.out.println("Found notes: " + notesStr);
    Assert.assertFalse(StringUtils.isBlank(notesStr), "Notes string not found.");
    Assert.assertNotNull(obj, "Error parsing Notes string");
    Assert.assertTrue(obj.has("project"));
    Assert.assertEquals(obj.get("project").getAsString(), "myproj1");
    Assert.assertTrue(obj.has("testdata"));
    Assert.assertEquals(obj.get("testdata").getAsString(), "abc");

    // Test retrieval using specified access method
    tmpSys = svc.getSystemByName(authenticatedService, sys0.getName(), true, AccessMethod.PASSWORD);
    System.out.println("Found item: " + sys0.getName());
    // Verify credentials. Only cred for default accessMethod is returned. In this case PASSWORD.
    cred = tmpSys.getAccessCredential();
    Assert.assertNotNull(cred, "AccessCredential should not be null");
    Assert.assertEquals(cred.getPassword(), cred0.getPassword());
    Assert.assertNull(cred.getPrivateKey(), "AccessCredential private key should be null");
    Assert.assertNull(cred.getPublicKey(), "AccessCredential public key should be null");
    Assert.assertNull(cred.getAccessKey(), "AccessCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AccessCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AccessCredential certificate should be null");
  }

  // Check that when a system is created variable substitution is correct for:
  //   owner, bucketName, rootDir, jobInputDir, jobOutputDir, workDir, scratchDir
  // And when system is retrieved effectiveUserId is resolved
  @Test
  public void testGetSystemByNameWithVariables() throws Exception
  {
    TSystem sys0 = sys8;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystemByName(authenticatedUser, sys0.getName(), false, null);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getName());
    System.out.println("Found item: " + sys0.getName());

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
    Assert.assertEquals(tmpSys.getName(), sys0.getName());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), owner);
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), effectiveUserId);
    Assert.assertEquals(tmpSys.getDefaultAccessMethod().name(), sys0.getDefaultAccessMethod().name());
    Assert.assertEquals(tmpSys.isEnabled(), sys0.isEnabled());
    Assert.assertEquals(tmpSys.getBucketName(), bucketName);
    Assert.assertEquals(tmpSys.getRootDir(), rootDir);
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), jobLocalWorkingDir);
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), jobLocalArchiveDir);
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), jobRemoteArchiveDir);
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    for (TransferMethod txfrMethod : sys0.getTransferMethods())
    {
      Assert.assertTrue(tMethodsList.contains(txfrMethod), "List of transfer methods did not contain: " + txfrMethod.name());
    }
  }

  @Test
  public void testGetSystemNames() throws Exception
  {
    TSystem sys0 = sys3;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = svc.getSystemNames(authenticatedUser);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(sys3.getName()), "List of systems did not contain system name: " + sys3.getName());
    Assert.assertTrue(systemNames.contains(sys4.getName()), "List of systems did not contain system name: " + sys4.getName());
  }

//  @Test
//  public void testGetSystems() throws Exception
//  {
//    TSystem sys0 = sys5;
//    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
//    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
//    List<TSystem> systems = svc.getSystems(authenticatedUser);
//    for (TSystem system : systems) {
//      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
//    }
//  }

  @Test
  public void testDelete() throws Exception
  {
    // Create the system
    TSystem sys0 = sys6;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);

    // Delete the system
    int changeCount = svc.deleteSystemByName(authenticatedUser, sys0.getName());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    TSystem tmpSys2 = svc.getSystemByName(authenticatedUser, sys0.getName(), false, null);
    Assert.assertNull(tmpSys2, "System not deleted. System name: " + sys0.getName());
  }

  @Test
  public void testSystemExists() throws Exception
  {
    // If system not there we should get false
    Assert.assertFalse(svc.checkForSystemByName(authenticatedUser, sys7.getName()));
    // After creating system we should get true
    TSystem sys0 = sys7;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(authenticatedUser, sys7.getName()));
  }

  // Check that if systems already exists we get an IllegalStateException when attempting to create
  @Test(expectedExceptions = {IllegalStateException.class},  expectedExceptionsMessageRegExp = "^SYSLIB_SYS_EXISTS.*")
  public void testCreateSystemAlreadyExists() throws Exception
  {
    // Create the system
    TSystem sys0 = sys9;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystemByName(authenticatedUser, sys0.getName()));
    // Now attempt to create again, should get IllegalStateException with msg SYSLIB_SYS_EXISTS
    svc.createSystem(authenticatedUser, sys0, scrubbedJson);
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    TSystem sys0 = sysA;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Create user perms for the system
    svc.grantUserPermissions(authenticatedUser, sys0.getName(), testUser2, testPerms);
    // Get the system perms for the user and make sure permissions are there
    Set<Permission> userPerms = svc.getUserPermissions(authenticatedUser, sys0.getName(), testUser2);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPerms.size(), "Incorrect number of perms returned.");
    for (Permission perm: testPerms) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm.name()); }
    // Remove perms for the user. Should return a change count of 2
    int changeCount = svc.revokeUserPermissions(authenticatedUser, sys0.getName(), testUser2, testPerms);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(authenticatedUser, sys0.getName(), testUser2);
    for (Permission perm: testPerms) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }
  }

  // Test creating, reading and deleting user credentials for a system
  @Test
  public void testUserCredentials() throws Exception
  {
    // Create a system
    TSystem sys0 = sysB;
    int itemId = svc.createSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    // Store and retrieve multiple secret types: password, ssh keys, access key and secret
    svc.createUserCredential(authenticatedUser, sys0.getName(), testUser2, cred0);
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    Credential cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.PASSWORD);
    // Verify credentials
    Assert.assertEquals(cred1.getPassword(), cred0.getPassword());
    cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.PKI_KEYS);
    Assert.assertEquals(cred1.getPublicKey(), cred0.getPublicKey());
    Assert.assertEquals(cred1.getPrivateKey(), cred0.getPrivateKey());
    cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Delete credentials and verify they were destroyed
    int changeCount = svc.deleteUserCredential(authenticatedUser, sys0.getName(), testUser2);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.PASSWORD);
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0.getName() + " User name: " + testUser2);

    // Attempt to delete again, should return 0 for change count
    changeCount = svc.deleteUserCredential(authenticatedUser, sys0.getName(), testUser2);
    // TODO: Currently the attempt to return 0 if it does not exist is throwing an exception.
//    Assert.assertEquals(changeCount, 0, "Change count incorrect when removing a credential already removed.");

    // Set just ACCESS_KEY only and test
    cred0 = new Credential(null, null, null, "fakeAccessKey2", "fakeAccessSecret2", null);
    svc.createUserCredential(authenticatedUser, sys0.getName(), testUser2, cred0);
    cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Attempt to retrieve secret that has not been set
    cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.PKI_KEYS);
    Assert.assertNull(cred1, "Credential was non-null for missing secret. System name: " + sys0.getName() + " User name: " + testUser2);
    // Delete credentials and verify they were destroyed
    changeCount = svc.deleteUserCredential(authenticatedUser, sys0.getName(), testUser2);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    try {
      cred1 = svc.getUserCredential(authenticatedService, sys0.getName(), testUser2, AccessMethod.ACCESS_KEY);
    } catch (TapisException te) {
      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      cred1 = null;
    }
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0.getName() + " User name: " + testUser2);
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
    Assert.assertFalse(svc.checkForSystemByName(authenticatedUser, fakeSystemName));

    // Get TSystem with no system should return null
    TSystem tmpSys = svc.getSystemByName(authenticatedUser, fakeSystemName, false, null);
    Assert.assertNull(tmpSys, "TSystem not null for non-existent system");

    // Delete system with no system should return 0 changes
    int changeCount = svc.deleteSystemByName(authenticatedUser, fakeSystemName);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when deleting non-existent system.");

    // Get owner with no system should return null
    String owner = svc.getSystemOwner(authenticatedUser, fakeSystemName);
    Assert.assertNull(owner, "Owner not null for non-existent system.");

    // Get perms with no system should return null
    Set<Permission> perms = svc.getUserPermissions(authenticatedUser, fakeSystemName, fakeUserName);
    Assert.assertNull(perms, "Perms list was not null for non-existent system");

    // Revoke perm with no system should return 0 changes
    changeCount = svc.revokeUserPermissions(authenticatedUser, fakeSystemName, fakeUserName, testPerms);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when revoking perms for non-existent system.");

    // Grant perm with no system should throw an exception
    boolean pass = false;
    try { svc.grantUserPermissions(authenticatedUser, fakeSystemName, fakeUserName, testPerms); }
    catch (TapisException tce)
    {
      Assert.assertTrue(tce.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);

    //Get credential with no system should return null
    Credential cred = svc.getUserCredential(authenticatedUser, fakeSystemName, fakeUserName, AccessMethod.PKI_KEYS);
    Assert.assertNull(cred, "Credential was not null for non-existent system");
//    // Get credential with no system should throw an exception
//    // TODO/TBD: this is inconsistent other GETs return null. Make them consistent once decided?
//    pass = false;
//    try { svc.getUserCredential(authenticatedUser, fakeSystemName, fakeUserName, AccessMethod.PKI_KEYS); }
//    catch (TapisException te)
//    {
//      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
//      pass = true;
//    }
//    Assert.assertTrue(pass);

    // Create credential with no system should throw an exception
    pass = false;
    cred = new Credential(null, null, null, null,"fakeAccessKey2", "fakeAccessSecret2");
    try { svc.createUserCredential(authenticatedUser, fakeSystemName, fakeUserName, cred); }
    catch (TapisException te)
    {
      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Delete credential with no system should 0 changes
    changeCount = svc.deleteUserCredential(authenticatedUser, fakeSystemName, fakeUserName);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when deleting a user credential for non-existent system.");
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    svc.deleteSystemByName(authenticatedUser, sys1.getName());
    TSystem tmpSys = svc.getSystemByName(authenticatedUser, sys1.getName(), false, null);
    Assert.assertNull(tmpSys, "System not deleted. System name: " + sys1.getName());
    svc.deleteSystemByName(authenticatedUser, sys2.getName());
    svc.deleteSystemByName(authenticatedUser, sys3.getName());
    svc.deleteSystemByName(authenticatedUser, sys4.getName());
    svc.deleteSystemByName(authenticatedUser, sys5.getName());
    svc.deleteSystemByName(authenticatedUser, sys6.getName());
    svc.deleteSystemByName(authenticatedUser, sys7.getName());
    svc.deleteSystemByName(authenticatedUser, sys8.getName());
    svc.deleteSystemByName(authenticatedUser, sys9.getName());
    svc.deleteSystemByName(authenticatedUser, sysA.getName());
    svc.deleteSystemByName(authenticatedUser, sysB.getName());
    svc.deleteSystemByName(authenticatedUser, sysC.getName());
  }
}
