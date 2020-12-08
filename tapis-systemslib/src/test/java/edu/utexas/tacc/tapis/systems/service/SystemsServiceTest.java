package edu.utexas.tacc.tapis.systems.service;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Capability.Category;
import edu.utexas.tacc.tapis.systems.model.Capability.Datatype;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;

import javax.ws.rs.NotAuthorizedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsService implementation class against a DB running locally
 * Note that this test has the following dependencies running locally or in dev
 *    Database - typically local
 *    Tenants service - typically dev
 *    Tokens service - typically dev and obtained from tenants service
 *    Security Kernel service - typically dev and obtained from tenants service
 *
 *    TODO: Add tests for requireExecPerm argument of getSystem()
 *    TODO: Add tests for getSystem() retrieving various user credentials for the effectiveUserId,
 *          including effectiveUserId = ${apiUserId}
 */
@Test(groups={"integration"})
public class SystemsServiceTest
{
  private SystemsService svc;
  private SystemsServiceImpl svcImpl;
  private AuthenticatedUser authenticatedOwnerUser1, authenticatedTestUser2, authenticatedTestUser3,
          authenticatedTestUser4, authenticatedAdminUser9, authenticatedFilesSvc;
  // Test data
  private static final String svcName = "systems";
  private static final String siteId = "tacc";
  // TODO: Currently admin user for a tenant is hard coded to be 'testuser9'
  private static final String adminUser9 = "testuser9";
  private static final String masterTenantName = "master";
  private static final String filesSvcName = "files";
  private static final String testUser2 = "testuser2";
  private static final String testUser3 = "testuser3";
  private static final String testUser4 = "testuser4";
  private static final Set<Permission> testPermsREADMODIFY = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  private static final Set<Permission> testPermsREAD = new HashSet<>(Set.of(Permission.READ));
  private static final Set<Permission> testPermsMODIFY = new HashSet<>(Set.of(Permission.MODIFY));
  private static final String[] tags2 = {"value3", "value4"};
  private static final Object notes2 = TapisGsonUtils.getGson().fromJson("{\"project\": \"myproj2\", \"testdata\": \"abc2\"}", JsonObject.class);

  private static final Capability capA2 = new Capability(Category.SCHEDULER, Capability.DEFAULT_SUBCATEGORY, "Type",
                                                         Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "Condor");
  private static final Capability capB2 = new Capability(Category.HARDWARE, Capability.DEFAULT_SUBCATEGORY, "CoresPerNode",
                                                         Datatype.INTEGER, Capability.DEFAULT_PRECEDENCE, "128");
  private static final Capability capC2 = new Capability(Category.SOFTWARE, Capability.DEFAULT_SUBCATEGORY, "OpenMP",
                                                         Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "3.1");
  private static final List<Capability> cap2List = new ArrayList<>(List.of(capA2, capB2, capC2));

  // TODO
  int limit = -1;
  String sortBy = "";
  String sortDirection = "";
  int skip = 0;
  String startAfer= "";

  int numSystems = 19;
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, "Svc");

  @BeforeSuite
  public void setUp() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsServiceTest.class.getSimpleName());
    // Setup for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure() {
        bind(SystemsServiceImpl.class).to(SystemsService.class);
        bind(SystemsServiceImpl.class).to(SystemsServiceImpl.class);
        bind(SystemsDaoImpl.class).to(SystemsDao.class);
        bindFactory(SystemsServiceContextFactory.class).to(ServiceContext.class);
        bind(SKClient.class).to(SKClient.class);
      }
    });
    locator.inject(this);

    // Initialize TenantManager and services
    String url = RuntimeParameters.getInstance().getTenantsSvcURL();
    TenantManager.getInstance(url).getTenants();

    // Initialize services
    svc = locator.getService(SystemsService.class);
    svcImpl = locator.getService(SystemsServiceImpl.class);
    svcImpl.initService(siteId);

    // Initialize authenticated user and service
    // TODO: Need to get JWTs. SK now verifies that site is set in the JWT.
    authenticatedOwnerUser1 = new AuthenticatedUser(ownerUser1, tenantName, TapisThreadContext.AccountType.user.name(), null, ownerUser1, tenantName, null, null, null);
    authenticatedAdminUser9 = new AuthenticatedUser(adminUser9, tenantName, TapisThreadContext.AccountType.user.name(), null, adminUser9, tenantName, null, null, null);
    authenticatedTestUser2 = new AuthenticatedUser(testUser2, tenantName, TapisThreadContext.AccountType.user.name(), null, testUser2, tenantName, null, null, null);
    authenticatedTestUser3 = new AuthenticatedUser(testUser3, tenantName, TapisThreadContext.AccountType.user.name(), null, testUser3, tenantName, null, null, null);
    authenticatedTestUser4 = new AuthenticatedUser(testUser4, tenantName, TapisThreadContext.AccountType.user.name(), null, testUser4, tenantName, null, null, null);
    authenticatedFilesSvc = new AuthenticatedUser(filesSvcName, masterTenantName, TapisThreadContext.AccountType.service.name(), null, ownerUser1, tenantName, null, null, null);

    // Cleanup anything leftover from previous failed run
    tearDown();
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown for " + SystemsServiceTest.class.getSimpleName());
    // Remove non-owner permissions granted during the tests
    svc.revokeUserPermissions(authenticatedOwnerUser1, systems[9].getId(), testUser3, testPermsREADMODIFY, scrubbedJson);
    svc.revokeUserPermissions(authenticatedOwnerUser1, systems[12].getId(), testUser3, testPermsREADMODIFY, scrubbedJson);
    svc.revokeUserPermissions(authenticatedOwnerUser1, systems[12].getId(), testUser2, testPermsREADMODIFY, scrubbedJson);
// TODO why is following revoke causing an exception?
    svc.revokeUserPermissions(authenticatedOwnerUser1, systems[14].getId(), testUser3, testPermsREADMODIFY, scrubbedJson);
    svc.revokeUserPermissions(authenticatedOwnerUser1, systems[14].getId(), testUser2, testPermsREADMODIFY, scrubbedJson);

    //Remove all objects created by tests
    for (int i = 0; i < numSystems; i++)
    {
      svcImpl.hardDeleteSystem(authenticatedAdminUser9, systems[i].getId());
    }

    TSystem tmpSys = svc.getSystem(authenticatedAdminUser9, systems[0].getId(), false, null, false);
    Assert.assertNull(tmpSys, "System not deleted. System name: " + systems[0].getId());
  }

  @Test
  public void testCreateSystem() throws Exception
  {
    TSystem sys0 = systems[0];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Create a system using minimal attributes:
  //   name, systemType, host, defaultAuthnMethod, canExec
  @Test
  public void testCreateSystemMinimal() throws Exception
  {
    TSystem sys0 = systems[11];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a system including default authn method
  //   and test retrieving for specified authn method.
  @Test
  public void testGetSystem() throws Exception
  {
    TSystem sys0 = systems[1];
    sys0.setJobCapabilities(capList1);
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Retrieve the system including the credential using the default authn method defined for the system
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    TSystem tmpSys = svc.getSystem(authenticatedFilesSvc, sys0.getId(), true, null, false);
    checkCommonSysAttrs(sys0, tmpSys);
    // Verify credentials. Only cred for default authnMethod is returned. In this case PKI_KEYS.
    Credential cred = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred, "AuthnCredential should not be null");
    Assert.assertEquals(cred.getPrivateKey(), cred0.getPrivateKey());
    Assert.assertEquals(cred.getPublicKey(), cred0.getPublicKey());
    Assert.assertNull(cred.getPassword(), "AuthnCredential password should be null");
    Assert.assertNull(cred.getAccessKey(), "AuthnCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AuthnCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AuthnCredential certificate should be null");

    // Test retrieval using specified authn method
    tmpSys = svc.getSystem(authenticatedFilesSvc, sys0.getId(), true, AuthnMethod.PASSWORD, false);
    System.out.println("Found item: " + sys0.getId());
    // Verify credentials. Only cred for default authnMethod is returned. In this case PASSWORD.
    cred = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred, "AuthnCredential should not be null");
    Assert.assertEquals(cred.getPassword(), cred0.getPassword());
    Assert.assertNull(cred.getPrivateKey(), "AuthnCredential private key should be null");
    Assert.assertNull(cred.getPublicKey(), "AuthnCredential public key should be null");
    Assert.assertNull(cred.getAccessKey(), "AuthnCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AuthnCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AuthnCredential certificate should be null");
  }

  // Test updating a system
  @Test
  public void testUpdateSystem() throws Exception
  {
    TSystem sys0 = systems[13];
    sys0.setJobCapabilities(capList1);
    String createText = "{\"testUpdate\": \"0-create\"}";
    String patch1Text = "{\"testUpdate\": \"1-patch1\"}";
    PatchSystem patchSystem = new PatchSystem("description PATCHED", "hostPATCHED", false, "effUserPATCHED",
            prot2.getAuthnMethod(), prot2.getTransferMethods(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(),
            prot2.getProxyPort(), dtnSystemId, dtnMountPoint, dtnSubDir, jobWorkingDir, jobEnvVariables, jobMaxJobs,
            jobMaxJobsPerUser, jobIsBatch, batchScheduler, queueList1, batchDefaultLogicalQueue,
            cap2List, tags2, notes2);
    patchSystem.setId(sys0.getId());
    patchSystem.setTenant(tenantName);
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, createText);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Update using patchSys
    svc.updateSystem(authenticatedOwnerUser1, patchSystem, patch1Text);
    TSystem tmpSys = svc.getSystem(authenticatedOwnerUser1, sys0.getId(), false, null, false);
//  TSystem sysE = new TSystem(-1, tenantName, "SsysE", "description E", SystemType.LINUX, ownerUser, "hostE", true,
//          "effUserE", prot1.getAuthnMethod(), "bucketE", "/rootE", prot1.getTransferMethods(),
//          prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),false,
//          "jobWorkDirE", "jobLocalArchDirE", "jobRemoteArchSystemE","jobRemoteArchDirE",
//          tags1, notes1, false, null, null);
//  TSystem sysE2 = new TSystem(-1, tenantName, "SsysE", "description PATCHED", SystemType.LINUX, ownerUser, "hostPATCHED", false,
//          "effUserPATCHED", prot2.getAuthnMethod(), "bucketE", "/rootE", prot2.getTransferMethods(),
//          prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),false,
//          "jobWorkDirE", "jobLocalArchDirE", "jobRemoteArchSystemE","jobRemoteArchDirE",
//          tags2, notes2, false, null, null);
    // Update original system definition with patched values
    sys0.setJobCapabilities(cap2List);
    sys0.setDescription("description PATCHED");
    sys0.setHost("hostPATCHED");
    sys0.setEnabled(false);
    sys0.setEffectiveUserId("effUserPATCHED");
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setTransferMethods(prot2.getTransferMethods());
    sys0.setPort(prot2.getPort());
    sys0.setUseProxy(prot2.isUseProxy());
    sys0.setProxyHost(prot2.getProxyHost());
    sys0.setProxyPort(prot2.getProxyPort());
    sys0.setTags(tags2);
    sys0.setNotes(notes2);
    // Check common system attributes:
    // TODO Caps are getting added, not replaced
    checkCommonSysAttrs(sys0, tmpSys);
  }

  // Test changing system owner
  @Test
  public void testChangeSystemOwner() throws Exception
  {
    TSystem sys0 = systems[15];
    sys0.setJobCapabilities(capList1);
    String createText = "{\"testChangeOwner\": \"0-create\"}";
    String newOwnerName = testUser2;
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, createText);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Change owner using api
    svc.changeSystemOwner(authenticatedOwnerUser1, sys0.getId(), newOwnerName);
    TSystem tmpSys = svc.getSystem(authenticatedTestUser2, sys0.getId(), false, null, false);
    Assert.assertEquals(tmpSys.getOwner(), newOwnerName);
    // Check expected auxillary updates have happened
    // New owner should be able to retrieve permissions and have the ALL permission
    Set<Permission> userPerms = svc.getUserPermissions(authenticatedTestUser2, sys0.getId(), newOwnerName);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertTrue(userPerms.contains(Permission.ALL));
    // Original owner should no longer have the ALL permission
    userPerms = svc.getUserPermissions(authenticatedTestUser2, sys0.getId(), ownerUser1);
    Assert.assertFalse(userPerms.contains(Permission.ALL));
    // Original owner should not be able to modify system
    try {
      svc.softDeleteSystem(authenticatedOwnerUser1, sys0.getId());
      Assert.fail("Original owner should not have permission to update system after change of ownership. System name: " + sys0.getId() +
              " Old owner: " + authenticatedOwnerUser1.getName() + " New Owner: " + newOwnerName);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "HTTP 401 Unauthorized");
    }
    // Original owner should not be able to read system
    try {
      svc.getSystem(authenticatedOwnerUser1, sys0.getId(), false, null, false);
      Assert.fail("Original owner should not have permission to read system after change of ownership. System name: " + sys0.getId() +
              " Old owner: " + authenticatedOwnerUser1.getName() + " New Owner: " + newOwnerName);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "HTTP 401 Unauthorized");
    }
  }

  // Check that when a system is created variable substitution is correct for:
  //   owner, bucketName, rootDir, ...
  // And when system is retrieved effectiveUserId is resolved
  @Test
  public void testGetSystemWithVariables() throws Exception
  {
    TSystem sys0 = systems[7];
    sys0.setOwner("${apiUserId}");
    sys0.setEffectiveUserId("${owner}");
    sys0.setBucketName("bucket8-${tenant}-${apiUserId}");
    sys0.setRootDir("/root8/${tenant}");
    sys0.setJobWorkingDir("jobWorkDir8/${owner}/${tenant}/${apiUserId}");
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = svc.getSystem(authenticatedOwnerUser1, sys0.getId(), false, null, false);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());

// sys8 = {tenantName, "Ssys8", "description 8", SystemType.LINUX.name(), "${apiUserId}", "host8",
//         "${owner}", prot1AuthnMethName, "fakePassword8", "bucket8-${tenant}-${apiUserId}", "/root8/${tenant}", prot1TxfrMethods,
//         "jobWorkDir8/${owner}/${tenant}/${apiUserId}", "jobLocalArchDir8/${apiUserId}", "jobRemoteArchSystem8",
//         "jobRemoteArchDir8${owner}${tenant}${apiUserId}", tags, notes, "{}"};
    String effectiveUserId = ownerUser1;
    String bucketName = "bucket8-" + tenantName + "-" + effectiveUserId;
    String rootDir = "/root8/" + tenantName;
    String jobWorkingDir = "jobWorkDir8/" + ownerUser1 + "/" + tenantName + "/" + effectiveUserId;
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), ownerUser1);
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), effectiveUserId);
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod().name(), sys0.getDefaultAuthnMethod().name());
    Assert.assertEquals(tmpSys.isEnabled(), sys0.isEnabled());
    Assert.assertEquals(tmpSys.getBucketName(), bucketName);
    Assert.assertEquals(tmpSys.getRootDir(), rootDir);
    Assert.assertEquals(tmpSys.getJobWorkingDir(), jobWorkingDir);
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    Assert.assertNotNull(sys0.getTransferMethods());
    for (TransferMethod txfrMethod : sys0.getTransferMethods())
    {
      Assert.assertTrue(tMethodsList.contains(txfrMethod), "List of transfer methods did not contain: " + txfrMethod.name());
    }
  }

  @Test
  public void testGetSystemNames() throws Exception
  {
    TSystem sys0 = systems[2];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = systems[3];
    itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = svc.getSystemNames(authenticatedOwnerUser1);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(systems[2].getId()), "List of systems did not contain system name: " + systems[2].getId());
    Assert.assertTrue(systemNames.contains(systems[3].getId()), "List of systems did not contain system name: " + systems[3].getId());
  }

  @Test
  public void testGetSystems() throws Exception
  {
    TSystem sys0 = systems[4];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = svc.getSystems(authenticatedOwnerUser1, null, limit, sortBy, sortDirection, skip, startAfer);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
    }
  }

  // Check that user only sees systems they are authorized to see.
  @Test
  public void testGetSystemsAuth() throws Exception
  {
    // Create 3 systems, 2 of which are owned by testUser4.
    TSystem sys0 = systems[16];
    String sys1Name = sys0.getId();
    sys0.setOwner(authenticatedTestUser4.getName());
    int itemId =  svc.createSystem(authenticatedTestUser4, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = systems[17];
    String sys2Name = sys0.getId();
    sys0.setOwner(authenticatedTestUser4.getName());
    itemId =  svc.createSystem(authenticatedTestUser4, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = systems[18];
    itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // When retrieving systems as testUser4 only 2 should be returned
    List<TSystem> systems = svc.getSystems(authenticatedTestUser4, null, limit, sortBy, sortDirection, skip, startAfer);
    System.out.println("Total number of systems retrieved: " + systems.size());
    for (TSystem system : systems)
    {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
      Assert.assertTrue(system.getId().equals(sys1Name) || system.getId().equalsIgnoreCase(sys2Name));
    }
    Assert.assertEquals(2, systems.size());
  }

  @Test
  public void testSoftDelete() throws Exception
  {
    // Create the system
    TSystem sys0 = systems[5];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);

    // Soft delete the system
    int changeCount = svc.softDeleteSystem(authenticatedOwnerUser1, sys0.getId());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    TSystem tmpSys2 = svc.getSystem(authenticatedOwnerUser1, sys0.getId(), false, null, false);
    Assert.assertNull(tmpSys2, "System not deleted. System name: " + sys0.getId());
  }

  @Test
  public void testSystemExists() throws Exception
  {
    // If system not there we should get false
    Assert.assertFalse(svc.checkForSystem(authenticatedOwnerUser1, systems[6].getId()));
    // After creating system we should get true
    TSystem sys0 = systems[6];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystem(authenticatedOwnerUser1, systems[6].getId()));
  }

  // Check that if systems already exists we get an IllegalStateException when attempting to create
  @Test(expectedExceptions = {IllegalStateException.class},  expectedExceptionsMessageRegExp = "^SYSLIB_SYS_EXISTS.*")
  public void testCreateSystemAlreadyExists() throws Exception
  {
    // Create the system
    TSystem sys0 = systems[8];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Assert.assertTrue(svc.checkForSystem(authenticatedOwnerUser1, sys0.getId()));
    // Now attempt to create again, should get IllegalStateException with msg SYSLIB_SYS_EXISTS
    svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    TSystem sys0 = systems[9];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Create user perms for the system
    svc.grantUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3, testPermsREADMODIFY, scrubbedJson);
    // Get the system perms for the user and make sure permissions are there
    Set<Permission> userPerms = svc.getUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPermsREADMODIFY.size(), "Incorrect number of perms returned.");
    for (Permission perm: testPermsREADMODIFY) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm.name()); }
    // Remove perms for the user. Should return a change count of 2
    int changeCount = svc.revokeUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3, testPermsREADMODIFY, scrubbedJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }
  }

  // Test creating, reading and deleting user credentials for a system
  @Test
  public void testUserCredentials() throws Exception
  {
    // Create a system
    TSystem sys0 = systems[10];
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    // Store and retrieve multiple secret types: password, ssh keys, access key and secret
    svc.createUserCredential(authenticatedOwnerUser1, sys0.getId(), testUser3, cred0, scrubbedJson);
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    Credential cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.PASSWORD);
    // Verify credentials
    Assert.assertEquals(cred1.getPassword(), cred0.getPassword());
    cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertEquals(cred1.getPublicKey(), cred0.getPublicKey());
    Assert.assertEquals(cred1.getPrivateKey(), cred0.getPrivateKey());
    cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Delete credentials and verify they were destroyed
    int changeCount = svc.deleteUserCredential(authenticatedOwnerUser1, sys0.getId(), testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.PASSWORD);
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0.getId() + " User name: " + testUser3);

    // Attempt to delete again, should return 0 for change count
    changeCount = svc.deleteUserCredential(authenticatedOwnerUser1, sys0.getId(), testUser3);
    // TODO: Currently the attempt to return 0 if it does not exist is throwing an exception.
//    Assert.assertEquals(changeCount, 0, "Change count incorrect when removing a credential already removed.");

    // Set just ACCESS_KEY only and test
    cred0 = new Credential(null, null, null, "fakeAccessKey2", "fakeAccessSecret2", null);
    svc.createUserCredential(authenticatedOwnerUser1, sys0.getId(), testUser3, cred0, scrubbedJson);
    cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred1.getAccessKey(), cred0.getAccessKey());
    Assert.assertEquals(cred1.getAccessSecret(), cred0.getAccessSecret());
    // Attempt to retrieve secret that has not been set
    cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertNull(cred1, "Credential was non-null for missing secret. System name: " + sys0.getId() + " User name: " + testUser3);
    // Delete credentials and verify they were destroyed
    changeCount = svc.deleteUserCredential(authenticatedOwnerUser1, sys0.getId(), testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    try {
      cred1 = svc.getUserCredential(authenticatedFilesSvc, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    } catch (TapisException te) {
      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      cred1 = null;
    }
    Assert.assertNull(cred1, "Credential not deleted. System name: " + sys0.getId() + " User name: " + testUser3);
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
    Assert.assertFalse(svc.checkForSystem(authenticatedOwnerUser1, fakeSystemName));

    // Get TSystem with no system should return null
    TSystem tmpSys = svc.getSystem(authenticatedOwnerUser1, fakeSystemName, false, null, false);
    Assert.assertNull(tmpSys, "TSystem not null for non-existent system");

    // Delete system with no system should return 0 changes
    int changeCount = svc.softDeleteSystem(authenticatedOwnerUser1, fakeSystemName);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when deleting non-existent system.");

    // Get owner with no system should return null
    String owner = svc.getSystemOwner(authenticatedOwnerUser1, fakeSystemName);
    Assert.assertNull(owner, "Owner not null for non-existent system.");

    // Get perms with no system should return null
    Set<Permission> perms = svc.getUserPermissions(authenticatedOwnerUser1, fakeSystemName, fakeUserName);
    Assert.assertNull(perms, "Perms list was not null for non-existent system");

    // Revoke perm with no system should return 0 changes
    changeCount = svc.revokeUserPermissions(authenticatedOwnerUser1, fakeSystemName, fakeUserName, testPermsREADMODIFY, scrubbedJson);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when revoking perms for non-existent system.");

    // Grant perm with no system should throw an exception
    boolean pass = false;
    try { svc.grantUserPermissions(authenticatedOwnerUser1, fakeSystemName, fakeUserName, testPermsREADMODIFY, scrubbedJson); }
    catch (TapisException tce)
    {
      Assert.assertTrue(tce.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);

    //Get credential with no system should return null
    Credential cred = svc.getUserCredential(authenticatedOwnerUser1, fakeSystemName, fakeUserName, AuthnMethod.PKI_KEYS);
    Assert.assertNull(cred, "Credential was not null for non-existent system");
//    // Get credential with no system should throw an exception
//    // TODO/TBD: this is inconsistent other GETs return null. Make them consistent once decided?
//    pass = false;
//    try { svc.getUserCredential(authenticatedUser, fakeSystemName, fakeUserName, AuthnMethod.PKI_KEYS); }
//    catch (TapisException te)
//    {
//      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
//      pass = true;
//    }
//    Assert.assertTrue(pass);

    // Create credential with no system should throw an exception
    pass = false;
    cred = new Credential(null, null, null, null,"fakeAccessKey2", "fakeAccessSecret2");
    try { svc.createUserCredential(authenticatedOwnerUser1, fakeSystemName, fakeUserName, cred, scrubbedJson); }
    catch (TapisException te)
    {
      Assert.assertTrue(te.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Delete credential with no system should 0 changes
    changeCount = svc.deleteUserCredential(authenticatedOwnerUser1, fakeSystemName, fakeUserName);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when deleting a user credential for non-existent system.");
  }

  // Test Auth denials
  // testUser4 - no perms, not owner
  // testUser3 - READ perm
  // testUser2 - MODIFY perm
  // NOTE: testUser1 is owner - all perms
  @Test
  public void testAuthDeny() throws Exception
  {
    // NOTE: By default seed data has owner as ownerUser1 == testUser1
    TSystem sys0 = systems[12];
    PatchSystem patchSys = new PatchSystem("description PATCHED", "hostPATCHED", false, "effUserPATCHED",
            prot2.getAuthnMethod(), prot2.getTransferMethods(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(),
            prot2.getProxyPort(), dtnSystemId, dtnMountPoint, dtnSubDir, jobWorkingDir, jobEnvVariables, jobMaxJobs,
            jobMaxJobsPerUser, jobIsBatch, batchScheduler, queueList1, batchDefaultLogicalQueue,
            cap2List, tags2, notes2);
    patchSys.setId(sys0.getId());
    patchSys.setTenant(tenantName);
    // CREATE - Deny user not owner/admin, deny service
    boolean pass = false;
    try { svc.createSystem(authenticatedTestUser4, sys0, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createSystem(authenticatedFilesSvc, sys0, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Create system for remaining auth access tests
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Grant testUesr3 - READ and testUser2 - MODIFY
    svc.grantUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3, testPermsREAD, scrubbedJson);
    svc.grantUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser2, testPermsMODIFY, scrubbedJson);

    // READ - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getSystem(authenticatedTestUser4, sys0.getId(), false, null, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // MODIFY Deny user with no READ or MODIFY, deny user with only READ, deny service
    pass = false;
    try { svc.updateSystem(authenticatedTestUser4, patchSys, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.updateSystem(authenticatedTestUser3, patchSys, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.updateSystem(authenticatedFilesSvc, patchSys, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // DELETE - deny user not owner/admin, deny service
    pass = false;
    try { svc.softDeleteSystem(authenticatedTestUser3, sys0.getId()); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.softDeleteSystem(authenticatedFilesSvc, sys0.getId()); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // CHANGE_OWNER - deny user not owner/admin, deny service
    pass = false;
    try { svc.changeSystemOwner(authenticatedTestUser3, sys0.getId(), testUser2); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.changeSystemOwner(authenticatedFilesSvc, sys0.getId(), testUser2); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GET_PERMS - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getUserPermissions(authenticatedTestUser4, sys0.getId(), ownerUser1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GRANT_PERMS - deny user not owner/admin, deny service
    pass = false;
    try { svc.grantUserPermissions(authenticatedTestUser3, sys0.getId(), testUser4, testPermsREADMODIFY, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.grantUserPermissions(authenticatedFilesSvc, sys0.getId(), testUser4, testPermsREADMODIFY, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // REVOKE_PERMS - deny user not owner/admin, deny service
    pass = false;
    try { svc.revokeUserPermissions(authenticatedTestUser3, sys0.getId(), ownerUser1, testPermsREADMODIFY, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.grantUserPermissions(authenticatedFilesSvc, sys0.getId(), ownerUser1, testPermsREADMODIFY, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // SET_CRED - deny user not owner/admin and not target user, deny service
    pass = false;
    try { svc.createUserCredential(authenticatedTestUser3, sys0.getId(), ownerUser1, cred0, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createUserCredential(authenticatedFilesSvc, sys0.getId(), ownerUser1, cred0, scrubbedJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // REMOVE_CRED - deny user not owner/admin and not target user, deny service
    pass = false;
    try { svc.deleteUserCredential(authenticatedTestUser3, sys0.getId(), ownerUser1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteUserCredential(authenticatedFilesSvc, sys0.getId(), ownerUser1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GET_CRED - deny user not owner/admin, deny owner
    pass = false;
    try { svc.getUserCredential(authenticatedTestUser3, sys0.getId(), ownerUser1, null); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.getUserCredential(authenticatedOwnerUser1, sys0.getId(), ownerUser1, null); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = true;
    }
    Assert.assertTrue(pass);
  }

  // Test Auth allow
  // Many cases covered during other tests
  // Test special cases here:
  //    MODIFY implies READ
  // testUser4 - no perms
  // testUser3 - READ perm
  // testUser2 - MODIFY perm
  // NOTE: testUser1 is owner - all perms
  @Test
  public void testAuthAllow() throws Exception
  {
    // NOTE: By default seed data has owner as testUser1
    TSystem sys0 = systems[14];
    // Create system for remaining auth access tests
    Credential cred0 = new Credential("fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    int itemId = svc.createSystem(authenticatedOwnerUser1, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Grant Usr1 - READ and Usr2 - MODIFY
    svc.grantUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser3, testPermsREAD, scrubbedJson);
    svc.grantUserPermissions(authenticatedOwnerUser1, sys0.getId(), testUser2, testPermsMODIFY, scrubbedJson);

    // READ - allow owner, service, with READ only, with MODIFY only
    boolean pass = true;
    try { svc.getSystem(authenticatedOwnerUser1, sys0.getId(), false, null, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = false;
    }
    Assert.assertTrue(pass);
    pass = true;
    try { svc.getSystem(authenticatedFilesSvc, sys0.getId(), false, null, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = false;
    }
    Assert.assertTrue(pass);
    pass = true;
    try { svc.getSystem(authenticatedTestUser3, sys0.getId(), false, null, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = false;
    }
    Assert.assertTrue(pass);
    pass = true;
    try { svc.getSystem(authenticatedTestUser2, sys0.getId(), false, null, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("HTTP 401 Unauthorized"));
      pass = false;
    }
    Assert.assertTrue(pass);
  }

  /**
   * Check common attributes after creating and retrieving a system
   * @param sys0 - Test system
   * @param tmpSys - Retrieved system
   */
  private static void checkCommonSysAttrs(TSystem sys0, TSystem tmpSys)
  {
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod().name(), sys0.getDefaultAuthnMethod().name());
    Assert.assertEquals(tmpSys.isEnabled(), sys0.isEnabled());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobWorkingDir(), sys0.getJobWorkingDir());
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    // Verify transfer methods
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    Assert.assertNotNull(sys0.getTransferMethods(), "Orig TxfrMethods should not be null");
    for (TransferMethod txfrMethod : sys0.getTransferMethods())
    {
      Assert.assertTrue(tMethodsList.contains(txfrMethod), "List of transfer methods did not contain: " + txfrMethod.name());
    }
    // Verify tags
    String[] origTags = sys0.getTags();
    String[] tmpTags = tmpSys.getTags();
    Assert.assertNotNull(origTags, "Orig Tags should not be null");
    Assert.assertNotNull(tmpTags, "Fetched Tags value should not be null");
    var tagsList = Arrays.asList(tmpTags);
    Assert.assertEquals(tmpTags.length, origTags.length, "Wrong number of tags.");
    for (String tagStr : origTags)
    {
      Assert.assertTrue(tagsList.contains(tagStr));
      System.out.println("Found tag: " + tagStr);
    }
    // Verify notes
    Assert.assertNotNull(sys0.getNotes(), "Orig Notes should not be null");
    Assert.assertNotNull(tmpSys.getNotes(), "Fetched Notes should not be null");
    System.out.println("Found notes: " + sys0.getNotes().toString());
    JsonObject tmpObj = (JsonObject) tmpSys.getNotes();
    JsonObject origNotes = (JsonObject) sys0.getNotes();
    Assert.assertTrue(tmpObj.has("project"));
    String projStr = origNotes.get("project").getAsString();
    Assert.assertEquals(tmpObj.get("project").getAsString(), projStr);
    Assert.assertTrue(tmpObj.has("testdata"));
    String testdataStr = origNotes.get("testdata").getAsString();
    Assert.assertEquals(tmpObj.get("testdata").getAsString(), testdataStr);
    // Verify capabilities
    List<Capability> origCaps = sys0.getJobCapabilities();
    List<Capability> jobCaps = tmpSys.getJobCapabilities();
    Assert.assertNotNull(origCaps, "Orig Caps was null");
    Assert.assertNotNull(jobCaps, "Fetched Caps was null");
    Assert.assertEquals(jobCaps.size(), origCaps.size());
    var capNamesFound = new ArrayList<String>();
    for (Capability capFound : jobCaps) {capNamesFound.add(capFound.getName());}
    for (Capability capSeedItem : origCaps)
    {
      Assert.assertTrue(capNamesFound.contains(capSeedItem.getName()),
              "List of capabilities did not contain a capability named: " + capSeedItem.getName());
    }
  }
}
