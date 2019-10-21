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
  private SystemsDao dao;
  private ProtocolDao protocolDao;

  // Test data
  private static String mechsStr = "{SFTP,S3}";
  private static final String tenant = "tenant1";
  private static final String[] sys1 = {tenant, "sys1", "description 1", "owner1", "host1", "bucket1", "/root1",
      "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", "cpassword1", "tpassword1"};
  private static final String[] sys2 = {tenant, "sys2", "description 2", "owner2", "host2", "bucket2", "/root2",
      "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", "cpassword2", "tpassword2"};
  private static final String[] sys3 = {tenant, "sys3", "description 3", "owner3", "host3", "bucket3", "/root3",
      "jobInputDir3", "jobOutputDir3", "workDir3", "scratchDir3", "effUser3", "cpassword3", "tpassword3"};

  int protId;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDao();
    protocolDao = new ProtocolDao();
    // Use port number different from values in other tests since other tests may be running in parallel. Cleanup in other tests
    //  can fail if protocols are referenced in the systems created here.
    protId = protocolDao.create(AccessMechanism.NONE.name(), mechsStr, 1001, false, "", 0);
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    String[] sys0 = sys1;
    int numRows = dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId);
    Assert.assertEquals(numRows, 1);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByName() throws Exception {
    String[] sys0 = sys2;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId);
    TSystem tmpSys = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0[1]);
    System.out.println("Found item: " + sys0[1]);
    Assert.assertEquals(tmpSys.getName(), sys0[1]);
  }

  // Test retrieving all systems
  @Test(enabled=true)
  public void testGetSystems() throws Exception {
    List<TSystem> systems = dao.getTSystems(tenant);
    for (TSystem system : systems) {
      System.out.println("Found item: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    String[] sys0 = sys3;
    dao.createTSystem(sys0[0], sys0[1], sys0[2], sys0[3], sys0[4], true, sys0[5], sys0[6],
                      sys0[7], sys0[8], sys0[9], sys0[10], sys0[11], protId);
    dao.deleteTSystem(sys0[0], sys0[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys0[0], sys0[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys0[1]);
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
    protocolDao.delete(protId);
  }
}