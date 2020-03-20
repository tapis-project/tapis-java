package edu.utexas.tacc.tapis.systems.dao;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.Protocol;
import org.apache.commons.lang3.StringUtils;
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
  private static final Protocol prot1 = new Protocol(AccessMethod.ACCESS_KEY, txfrMethodsList, 0, false, "", 0);
  private static final Protocol prot2 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, 22, false, "",0);
  private static final Protocol prot3 = new Protocol(AccessMethod.CERT, txfrMethodsList, 23, true, "localhost",22);
  private static final Protocol prot4 = new Protocol(AccessMethod.CERT, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot5 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, null,-1);
  private static final Protocol prot6 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final Protocol prot7 = new Protocol(AccessMethod.PASSWORD, txfrMethodsEmpty, -1, false, "",-1);
  private static final String scrubbedJson = "{}";
  private static final String[] tags = {"value1", "value2", "a",
      "a long tag with spaces and numbers (1 3 2) and special characters [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  private static final String notes = "{\"project\":\"myproj1\", \"testdata\":\"abc\"}";
  private static JsonObject notesJO = gson.fromJson(notes, JsonObject.class);

  TSystem sys1 = new TSystem(-1, tenantName, "Dsys1", "description 1", SystemType.LINUX, "owner1", "host1", true,
          "effUser1", prot1.getAccessMethod(), null,"bucket1", "/root1", prot1.getTransferMethods(),
          prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),false,
          "jobLocalWorkDir1", "jobLocalArchDir1", "jobRemoteArchSystem1","jobRemoteArchDir1",
          null, tags, notesJO, null, null);
  TSystem sys2 = new TSystem(-1, tenantName, "Dsys2", "description 2", SystemType.LINUX, "owner2", "host2", true,
          "effUser2", prot2.getAccessMethod(), null,"bucket2", "/root2", prot2.getTransferMethods(),
          prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),false,
          "jobLocalWorkDir2", "jobLocalArchDir2", "jobRemoteArchSystem2","jobRemoteArchDir2",
          null, tags, notesJO, null, null);
  TSystem sys3 = new TSystem(-1, tenantName, "Dsys3", "description 3", SystemType.OBJECT_STORE, "owner3", "host3", true,
          "effUser3", prot3.getAccessMethod(), null,"bucket3", "/root3", prot3.getTransferMethods(),
          prot3.getPort(), prot3.isUseProxy(), prot3.getProxyHost(), prot3.getProxyPort(),false,
          "jobLocalWorkDir3", "jobLocalArchDir3", "jobRemoteArchSystem3","jobRemoteArchDir3",
          null, tags, notesJO, null, null);
  TSystem sys4 = new TSystem(-1, tenantName, "Dsys4", "description 4", SystemType.LINUX, "owner4", "host4", true,
          "effUser4", prot4.getAccessMethod(), null,"bucket4", "/root4", prot4.getTransferMethods(),
          prot4.getPort(), prot4.isUseProxy(), prot4.getProxyHost(), prot4.getProxyPort(),false,
          "jobLocalWorkDir4", "jobLocalArchDir4", "jobRemoteArchSystem4","jobRemoteArchDir4",
          null, tags, notesJO, null, null);
  TSystem sys5 = new TSystem(-1, tenantName, "Dsys5", "description 5", SystemType.LINUX, "owner5", "host5", true,
          "effUser5", prot5.getAccessMethod(), null,"bucket5", "/root5", prot5.getTransferMethods(),
          prot5.getPort(), prot5.isUseProxy(), prot5.getProxyHost(), prot5.getProxyPort(),false,
          "jobLocalWorkDir5", "jobLocalArchDir5", "jobRemoteArchSystem5","jobRemoteArchDir5",
          null, tags, notesJO, null, null);
  TSystem sys6 = new TSystem(-1, tenantName, "Dsys6", "description 6", SystemType.LINUX, "owner6", "host6", true,
          "effUser6", prot6.getAccessMethod(), null,"bucket6", "/root6", prot6.getTransferMethods(),
          prot6.getPort(), prot6.isUseProxy(), prot6.getProxyHost(), prot6.getProxyPort(),false,
          "jobLocalWorkDir6", "jobLocalArchDir6", "jobRemoteArchSystem6","jobRemoteArchDir6",
          null, tags, notesJO, null, null);
  TSystem sys7 = new TSystem(-1, tenantName, "Dsys7", "description 7", SystemType.LINUX, "owner7", "host7", true,
          "effUser7", prot7.getAccessMethod(), null,"bucket7", "/root7", prot7.getTransferMethods(),
          prot7.getPort(), prot7.isUseProxy(), prot7.getProxyHost(), prot7.getProxyPort(),false,
          "jobLocalWorkDir7", "jobLocalArchDir7", "jobRemoteArchSystem7","jobRemoteArchDir7",
          null, tags, notesJO, null, null);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    authenticatedUser = new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(), null, null, null, null, null);

    // Cleanup anything leftover from previous failed run
    teardown();
  }

  // Test create for a single item
  @Test
  public void testCreate() throws Exception
  {
    TSystem sys0 = sys1;
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
  }

  // Test retrieving a single item
  @Test
  public void testGetByName() throws Exception {
    TSystem sys0 = sys2;
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
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
    for (TransferMethod txfrMethod : sys0.getTransferMethods())
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
    String notesStr = tmpSys.getNotes().toString();
    System.out.println("Found notes: " + notesStr);
    Assert.assertFalse(StringUtils.isBlank(notesStr), "Notes string not found");
    JsonObject obj = JsonParser.parseString(notesStr).getAsJsonObject();
    Assert.assertNotNull(obj, "Error parsing Notes string");
    Assert.assertTrue(obj.has("project"));
    Assert.assertEquals(obj.get("project").getAsString(), "myproj1");
    Assert.assertTrue(obj.has("testdata"));
    Assert.assertEquals(obj.get("testdata").getAsString(), "abc");
  }

  // Test retrieving all system names
  @Test
  public void testGetSystemNames() throws Exception {
    TSystem sys0 = sys3;
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    sys0 = sys4;
    itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
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
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    List<TSystem> systems = dao.getTSystems(tenantName);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test
  public void testDelete() throws Exception {
    TSystem sys0 = sys6;
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
    System.out.println("Created item with id: " + itemId);
    Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    dao.deleteTSystem(sys0.getTenant(), sys0.getName());
    TSystem tmpSystem = dao.getTSystemByName(sys0.getTenant(), sys0.getName());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys0.getName());
  }

  // Test create and get for a single item with no transfer methods supported
  @Test
  public void testNoTxfr() throws Exception
  {
    TSystem sys0 = sys7;
    int itemId = dao.createTSystem(authenticatedUser, sys0, scrubbedJson);
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

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.deleteTSystem(sys1.getTenant(), sys1.getName());
    TSystem tmpSystem = dao.getTSystemByName(sys1.getTenant(), sys1.getName());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys1.getName());
    dao.deleteTSystem(sys2.getTenant(), sys2.getName());
    dao.deleteTSystem(sys3.getTenant(), sys3.getName());
    dao.deleteTSystem(sys4.getTenant(), sys4.getName());
    dao.deleteTSystem(sys5.getTenant(), sys5.getName());
    dao.deleteTSystem(sys7.getTenant(), sys7.getName());
  }
}