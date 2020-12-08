package edu.utexas.tacc.tapis.systems.dao;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

import static edu.utexas.tacc.tapis.search.SearchUtils.*;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsDao class against a DB running locally
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;
  private AuthenticatedUser authenticatedUser;

  // Test data
  int numSystems = 11;
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, "Dao");

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    authenticatedUser = new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(), null, apiUser, tenantName, null, null, null);
    // Cleanup anything leftover from previous failed run
    teardown();
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + SystemsDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (int i = 0; i < numSystems; i++)
    {
      dao.hardDeleteTSystem(tenantName, systems[i].getId());
    }

    TSystem tmpSystem = dao.getTSystem(tenantName, systems[0].getId());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + systems[0].getId());
  }

  // Test create for a single item
  @Test
  public void testCreate() throws Exception
  {
    TSystem sys0 = systems[0];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a single item
  @Test
  public void testGet() throws Exception {
    TSystem sys0 = systems[1];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystem(sys0.getTenant(), sys0.getId());
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobWorkingDir(), sys0.getJobWorkingDir());
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod(), sys0.getDefaultAuthnMethod());
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    // Verify txfr methods
    List<TransferMethod> tMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(tMethodsList);
    List<TransferMethod> sys0TMethodsList = sys0.getTransferMethods();
    Assert.assertNotNull(sys0TMethodsList);
    for (TransferMethod txfrMethod : sys0TMethodsList)
    {
      Assert.assertTrue(tMethodsList.contains(txfrMethod), "List of transfer methods did not contain: " + txfrMethod.name());
    }
    // Verify tags
    String[] tmpTags = tmpSys.getTags();
    Assert.assertNotNull(tmpTags, "Tags value was null");
    var tagsList = Arrays.asList(tmpTags);
    Assert.assertEquals(tmpTags.length, tags.length, "Wrong number of tags");
    for (String tagStr : tags)
    {
      Assert.assertTrue(tagsList.contains(tagStr));
      System.out.println("Found tag: " + tagStr);
    }
    // Verify notes
    JsonObject obj = (JsonObject) tmpSys.getNotes();
    Assert.assertNotNull(obj, "Notes object was null");
    Assert.assertTrue(obj.has("project"));
    Assert.assertEquals(obj.get("project").getAsString(), notesObj.get("project").getAsString());
    Assert.assertTrue(obj.has("testdata"));
    Assert.assertEquals(obj.get("testdata").getAsString(), notesObj.get("testdata").getAsString());
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

  // Test retrieving all system names
  @Test
  public void testGetSystemNames() throws Exception {
    // Create 2 systems
    TSystem sys0 = systems[2];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = systems[3];
    itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    // Get all systems
    List<String> systemNames = dao.getTSystemNames(tenantName);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(systems[2].getId()), "List of systems did not contain system name: " + systems[2].getId());
    Assert.assertTrue(systemNames.contains(systems[3].getId()), "List of systems did not contain system name: " + systems[3].getId());
  }

  // Test retrieving all systems
  @Test
  public void testGetSystems() throws Exception {
    TSystem sys0 = systems[4];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = dao.getTSystems(tenantName, null, null, null, DEFAULT_LIMIT, DEFAULT_SORTBY,
                                            DEFAULT_SORTBY_DIRECTION, DEFAULT_SKIP, DEFAULT_STARTAFTER);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
    }
  }

  // Test retrieving all systems in a list of IDs
  @Test
  public void testGetSystemsInIDList() throws Exception {
    var idList = new ArrayList<Integer>();
    // Create 2 systems
    TSystem sys0 = systems[5];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    idList.add(itemId);
    sys0 = systems[6];
    itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    idList.add(itemId);
    // Get all systems in list of IDs
    List<TSystem> systems = dao.getTSystems(tenantName, null, null, idList, DEFAULT_LIMIT, DEFAULT_SORTBY,
                                            DEFAULT_SORTBY_DIRECTION, DEFAULT_SKIP, DEFAULT_STARTAFTER);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
      Assert.assertTrue(idList.contains(system.getSeqId()));
    }
    Assert.assertEquals(idList.size(), systems.size());
  }

  // Test change system owner
  @Test
  public void testChangeSystemOwner() throws Exception {
    TSystem sys0 = systems[7];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.updateSystemOwner(authenticatedUser, itemId, "newOwner");
    TSystem tmpSystem = dao.getTSystem(sys0.getTenant(), sys0.getId());
    Assert.assertEquals(tmpSystem.getOwner(), "newOwner");
  }

  // Test soft deleting a single item
  @Test
  public void testSoftDelete() throws Exception {
    TSystem sys0 = systems[8];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    int numDeleted = dao.softDeleteTSystem(authenticatedUser, itemId);
    Assert.assertEquals(numDeleted, 1);
    numDeleted = dao.softDeleteTSystem(authenticatedUser, itemId);
    Assert.assertEquals(numDeleted, 0);
    Assert.assertFalse(dao.checkForTSystem(sys0.getTenant(), sys0.getId(), false ),
            "System not deleted. System name: " + sys0.getId());
  }

  // Test hard deleting a single item
  @Test
  public void testHardDelete() throws Exception {
    TSystem sys0 = systems[9];
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.hardDeleteTSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(dao.checkForTSystem(sys0.getTenant(), sys0.getId(), true),"System not deleted. System name: " + sys0.getId());
  }

  // Test create and get for a single item with no transfer methods supported and unusual port settings
  @Test
  public void testNoTxfr() throws Exception
  {
    TSystem sys0 = systems[10];
    sys0.setTransferMethods(txfrMethodsEmpty);
    sys0.setPort(-1);
    sys0.setProxyPort(-1);
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystem(sys0.getTenant(), sys0.getId());
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobWorkingDir(), sys0.getJobWorkingDir());
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod(), sys0.getDefaultAuthnMethod());
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    List<TransferMethod> txfrMethodsList = tmpSys.getTransferMethods();
    Assert.assertNotNull(txfrMethodsList);
    Assert.assertEquals(txfrMethodsList.size(), 0);
  }

  // Test behavior when system is missing, especially for cases where service layer depends on the behavior.
  //  update - throws not found exception
  //  get - returns null
  //  check - returns false
  //  getOwner - returns null
  @Test
  public void testMissingSystem() throws Exception {
    String fakeSystemName = "AMissingSystemName";
    PatchSystem patchSys = new PatchSystem("description PATCHED", "hostPATCHED", false, "effUserPATCHED",
            prot2.getAuthnMethod(), prot2.getTransferMethods(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(),
            prot2.getProxyPort(), dtnSystemId, dtnMountPoint, dtnSubDir, jobWorkingDir, jobEnvVariables, jobMaxJobs,
            jobMaxJobsPerUser, jobIsBatch, batchScheduler, queueList1, batchDefaultLogicalQueue,
            capList1, tags, notes);
    patchSys.setTenant(tenantName);
    patchSys.setId(fakeSystemName);
    TSystem patchedSystem = new TSystem(1, tenantName, fakeSystemName, "description", SystemType.LINUX, "owner", "host", isEnabled,
            "effUser", prot2.getAuthnMethod(), "bucket", "/root", prot2.getTransferMethods(),
            prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemId, dtnMountPoint, dtnSubDir, canExec, "jobWorkDir",
            jobEnvVariables, jobMaxJobs, jobMaxJobsPerUser, jobIsBatch, "batchScheduler", "batchDefaultLogicalQueue",
            tags, notes, null, isDeleted, created, updated);
    // Make sure system does not exist
    Assert.assertFalse(dao.checkForTSystem(tenantName, fakeSystemName, true));
    Assert.assertFalse(dao.checkForTSystem(tenantName, fakeSystemName, false));
    // update should throw not found exception
    boolean pass = false;
    try { dao.updateTSystem(authenticatedUser, patchedSystem, patchSys, scrubbedJson, null); }
    catch (IllegalStateException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);
    Assert.assertNull(dao.getTSystem(tenantName, fakeSystemName));
    Assert.assertNull(dao.getTSystemOwner(tenantName, fakeSystemName));
  }
}
