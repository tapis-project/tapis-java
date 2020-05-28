package edu.utexas.tacc.tapis.systems.dao;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.Protocol;
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
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

/**
 * Test the SystemsDao class against a DB running locally
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;
  private AuthenticatedUser authenticatedUser;
  private static final Gson gson =  TapisGsonUtils.getGson();

  // Test data
  private static final String tenantName = "tenant1";
  private static final String apiUser = "daoTestUser";
  private static final List<TransferMethod> txfrMethodsList = new ArrayList<>(List.of(TransferMethod.SFTP, TransferMethod.S3));
  private static final List<TransferMethod> txfrMethodsEmpty = new ArrayList<>();
  private static final Protocol prot0 = new Protocol(AccessMethod.PASSWORD, txfrMethodsList, 0, false, "", 0);
  private static final Protocol prot1 = new Protocol(AccessMethod.ACCESS_KEY, txfrMethodsList, 0, false, "", 0);
  private static final Protocol prot2 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, 22, false, "",0);
  private static final Protocol prot5 = new Protocol(AccessMethod.CERT, txfrMethodsEmpty, -1, false, null,-1);
  private static final Protocol prot7 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final String scrubbedJson = "{}";
  private static final String[] tags = {"value1", "value2", "a",
      "a long tag with spaces and numbers (1 3 2) and special characters [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  private static final Object notes = TapisGsonUtils.getGson().fromJson("{\"project\": \"myproj1\", \"testdata\": \"abc1\"}", JsonObject.class);
  private static final JsonObject notesObj = (JsonObject) notes;
  private static final Capability capA1 = new Capability(Capability.Category.SCHEDULER, "Type", "Slurm");
  private static final Capability capB1 = new Capability(Capability.Category.HARDWARE, "CoresPerNode", "4");
  private static final Capability capC1 = new Capability(Capability.Category.SOFTWARE, "OpenMP", "4.5");
  private static final Capability capD1 = new Capability(Capability.Category.CONTAINER, "Singularity", null);
  private static final List<Capability> cap1List = new ArrayList<>(List.of(capA1, capB1, capC1, capD1));

  TSystem sys1 = new TSystem(-1, tenantName, "Dsys1", "description 1", SystemType.LINUX, "owner1", "host1", true,
          "effUser1", prot1.getAccessMethod(), "bucket1", "/root1", prot1.getTransferMethods(),
          prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),false,
          "jobLocalWorkDir1", "jobLocalArchDir1", "jobRemoteArchSystem1","jobRemoteArchDir1",
          tags, notes, false, null, null);
  TSystem sys2 = new TSystem(-1, tenantName, "Dsys2", "description 2", SystemType.LINUX, "owner2", "host2", true,
          "effUser2", prot2.getAccessMethod(), "bucket2", "/root2", prot2.getTransferMethods(),
          prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),false,
          "jobLocalWorkDir2", "jobLocalArchDir2", "jobRemoteArchSystem2","jobRemoteArchDir2",
          tags, notes, false, null, null);
  TSystem sys3 = new TSystem(-1, tenantName, "Dsys3", "description 3", SystemType.OBJECT_STORE, "owner3", "host3", true,
          "effUser3", prot0.getAccessMethod(), "bucket3", "/root3", prot0.getTransferMethods(),
          prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),false,
          "jobLocalWorkDir3", "jobLocalArchDir3", "jobRemoteArchSystem3","jobRemoteArchDir3",
          tags, notes, false, null, null);
  TSystem sys4 = new TSystem(-1, tenantName, "Dsys4", "description 4", SystemType.LINUX, "owner4", "host4", true,
          "effUser4", prot0.getAccessMethod(),"bucket4", "/root4", prot0.getTransferMethods(),
          prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),false,
          "jobLocalWorkDir4", "jobLocalArchDir4", "jobRemoteArchSystem4","jobRemoteArchDir4",
          tags, notes, false, null, null);
  TSystem sys5 = new TSystem(-1, tenantName, "Dsys5", "description 5", SystemType.LINUX, "owner5", "host5", true,
          "effUser5", prot5.getAccessMethod(), "bucket5", "/root5", prot5.getTransferMethods(),
          prot5.getPort(), prot5.isUseProxy(), prot5.getProxyHost(), prot5.getProxyPort(),false,
          "jobLocalWorkDir5", "jobLocalArchDir5", "jobRemoteArchSystem5","jobRemoteArchDir5",
          tags, notes, false, null, null);
  TSystem sys6 = new TSystem(-1, tenantName, "Dsys6", "description 6", SystemType.LINUX, "owner6", "host6", true,
          "effUser6", prot0.getAccessMethod(), "bucket6", "/root6", prot0.getTransferMethods(),
          prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),false,
          "jobLocalWorkDir6", "jobLocalArchDir6", "jobRemoteArchSystem6","jobRemoteArchDir6",
          tags, notes, false, null, null);
  TSystem sys7 = new TSystem(-1, tenantName, "Dsys7", "description 7", SystemType.LINUX, "owner7", "host7", true,
          "effUser7", prot7.getAccessMethod(), "bucket7", "/root7", prot7.getTransferMethods(),
          prot7.getPort(), prot7.isUseProxy(), prot7.getProxyHost(), prot7.getProxyPort(),false,
          "jobLocalWorkDir7", "jobLocalArchDir7", "jobRemoteArchSystem7","jobRemoteArchDir7",
          tags, notes, false, null, null);
  TSystem sys8 = new TSystem(-1, tenantName, "Dsys8", "description 8", SystemType.LINUX, "owner8", "host8", true,
          "effUser8", prot0.getAccessMethod(), "bucket8", "/root8", prot0.getTransferMethods(),
          prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),false,
          "jobLocalWorkDir8", "jobLocalArchDir8", "jobRemoteArchSystem8","jobRemoteArchDir8",
          tags, notes, false, null, null);
  TSystem sys9 = new TSystem(-1, tenantName, "Dsys9", "description 9", SystemType.LINUX, "owner9", "host9", true,
          "effUser9", prot0.getAccessMethod(), "bucket9", "/root9", prot0.getTransferMethods(),
          prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(),false,
          "jobLocalWorkDir9", "jobLocalArchDir9", "jobRemoteArchSystem9","jobRemoteArchDir9",
          tags, notes, false, null, null);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    authenticatedUser = new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(), null, null, null, null, null);
    sys1.setJobCapabilities(cap1List);
    sys2.setJobCapabilities(cap1List);
    // Cleanup anything leftover from previous failed run
    teardown();
  }

  // Test create for a single item
  @Test
  public void testCreate() throws Exception
  {
    TSystem sys0 = sys1;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a single item
  @Test
  public void testGetByName() throws Exception {
    TSystem sys0 = sys2;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystemByName(sys0.getTenant(), sys0.getName());
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getName());
    System.out.println("Found item: " + sys0.getName());
    Assert.assertEquals(tmpSys.getName(), sys0.getName());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0.getJobLocalWorkingDir());
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0.getJobLocalArchiveDir());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0.getJobRemoteArchiveSystem());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0.getJobRemoteArchiveDir());
    Assert.assertEquals(tmpSys.getDefaultAccessMethod(), sys0.getDefaultAccessMethod());
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
    TSystem sys0 = sys3;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<String> systemNames = dao.getTSystemNames(tenantName);
    for (String name : systemNames) {
      System.out.println("Found item: " + name);
    }
    Assert.assertTrue(systemNames.contains(sys3.getName()), "List of systems did not contain system name: " + sys3.getName());
    Assert.assertTrue(systemNames.contains(sys4.getName()), "List of systems did not contain system name: " + sys4.getName());
  }

  // Test retrieving all systems
  @Test
  public void testGetSystems() throws Exception {
    TSystem sys0 = sys5;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = dao.getTSystems(tenantName, null, null);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  // Test retrieving all systems in a list of IDs
  @Test
  public void testGetSystemsInIDList() throws Exception {
    TSystem sys0 = new TSystem(sys5);
    var idList = new ArrayList<Integer>();
    sys0.setName(sys5.getName() + "a");
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    idList.add(itemId);
    sys0.setName(sys5.getName() + "b");
    itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    idList.add(itemId);
    List<TSystem> systems = dao.getTSystems(tenantName, null, idList);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
      Assert.assertTrue(idList.contains(system.getId()));
    }
    Assert.assertEquals(idList.size(), systems.size());
  }

  // Test change system owner
  @Test
  public void testChangeSystemOwner() throws Exception {
    TSystem sys0 = sys8;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.updateSystemOwner(authenticatedUser, itemId, "newOwner");
    TSystem tmpSystem = dao.getTSystemByName(sys0.getTenant(), sys0.getName());
    Assert.assertEquals(tmpSystem.getOwner(), "newOwner");
  }

  // Test soft deleting a single item
  @Test
  public void testSoftDelete() throws Exception {
    TSystem sys0 = sys6;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    int numDeleted = dao.softDeleteTSystem(authenticatedUser, itemId);
    Assert.assertEquals(numDeleted, 1);
    numDeleted = dao.softDeleteTSystem(authenticatedUser, itemId);
    Assert.assertEquals(numDeleted, 0);
    Assert.assertFalse(dao.checkForTSystemByName(sys0.getTenant(), sys0.getName(), false ),
            "System not deleted. System name: " + sys0.getName());
  }

  // Test hard deleting a single item
  @Test
  public void testHardDelete() throws Exception {
    TSystem sys0 = sys9;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.hardDeleteTSystem(sys0.getTenant(), sys0.getName());
    Assert.assertFalse(dao.checkForTSystemByName(sys0.getTenant(), sys0.getName(), true),"System not deleted. System name: " + sys0.getName());
  }

  // Test create and get for a single item with no transfer methods supported
  @Test
  public void testNoTxfr() throws Exception
  {
    TSystem sys0 = sys7;
    int itemId = dao.createTSystem(authenticatedUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    TSystem tmpSys = dao.getTSystemByName(sys0.getTenant(), sys0.getName());
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getName());
    System.out.println("Found item: " + sys0.getName());
    Assert.assertEquals(tmpSys.getName(), sys0.getName());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getJobLocalWorkingDir(), sys0.getJobLocalWorkingDir());
    Assert.assertEquals(tmpSys.getJobLocalArchiveDir(), sys0.getJobLocalArchiveDir());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveSystem(), sys0.getJobRemoteArchiveSystem());
    Assert.assertEquals(tmpSys.getJobRemoteArchiveDir(), sys0.getJobRemoteArchiveDir());
    Assert.assertEquals(tmpSys.getDefaultAccessMethod(), sys0.getDefaultAccessMethod());
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
  //  getByName - returns null
  //  checkByName - returns false
  //  getOwner - returns null
  @Test
  public void testMissingSystem() throws Exception {
    String fakeSystemName = "AMissingSystemName";
    PatchSystem patchSys = new PatchSystem("description PATCHED", "hostPATCHED", false, "effUserPATCHED",
            prot2.getAccessMethod(), prot2.getTransferMethods(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(),
            prot2.getProxyPort(), cap1List, tags, notes);
    patchSys.setTenant(tenantName);
    patchSys.setName(fakeSystemName);
    TSystem patchedSystem = new TSystem(1, tenantName, fakeSystemName, "description", SystemType.LINUX, "owner", "host", true,
            "effUser", prot0.getAccessMethod(), "bucket", "/root", prot0.getTransferMethods(),
            prot0.getPort(), prot0.isUseProxy(), prot0.getProxyHost(), prot0.getProxyPort(), false,
            "jobLocalWorkDir", "jobLocalArchDir", "jobRemoteArchSystem","jobRemoteArchDir",
            tags, notes, false, null, null);
    // Make sure system does not exist
    Assert.assertFalse(dao.checkForTSystemByName(tenantName, fakeSystemName, true));
    Assert.assertFalse(dao.checkForTSystemByName(tenantName, fakeSystemName, false));
    // update should throw not found exception
    boolean pass = false;
    try { dao.updateTSystem(authenticatedUser, patchedSystem, patchSys, scrubbedJson, null); }
    catch (IllegalStateException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);
    Assert.assertNull(dao.getTSystemByName(tenantName, fakeSystemName));
    Assert.assertNull(dao.getTSystemOwner(tenantName, fakeSystemName));
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method" + SystemsDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    dao.hardDeleteTSystem(sys1.getTenant(), sys1.getName());
    TSystem tmpSystem = dao.getTSystemByName(sys1.getTenant(), sys1.getName());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys1.getName());
    dao.hardDeleteTSystem(sys2.getTenant(), sys2.getName());
    dao.hardDeleteTSystem(sys3.getTenant(), sys3.getName());
    dao.hardDeleteTSystem(sys4.getTenant(), sys4.getName());
    dao.hardDeleteTSystem(sys5.getTenant(), sys5.getName());
    dao.hardDeleteTSystem(sys6.getTenant(), sys6.getName());
    dao.hardDeleteTSystem(sys7.getTenant(), sys7.getName());
    dao.hardDeleteTSystem(sys8.getTenant(), sys8.getName());
    dao.hardDeleteTSystem(sys9.getTenant(), sys9.getName());
    dao.hardDeleteTSystem(sys9.getTenant(), sys5.getName() + "a");
    dao.hardDeleteTSystem(sys9.getTenant(), sys5.getName() + "b");
  }
}