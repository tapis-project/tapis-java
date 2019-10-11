package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.CommandProtocol;
import edu.utexas.tacc.tapis.systems.model.TransferProtocol;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;

/**
 * Test the SystemsDao class against a running DB
 * System objects need a valid CommandProtocol and TransferProtocol so create those in setup
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDao dao;
  private CommandProtocolDao commandProtocolDao;
  private TransferProtocolDao transferProtocolDao;

  // Test data
  private static final String[] sys1 = {"tenant1", "sys1", "description 1", "owner1", "host1", "bucket1", "/root1",
      "jobInputDir1", "jobOutputDir1", "workDir1", "scratchDir1", "effUser1", "cpassword1", "tpassword1"};
  private static final String[] sys2 = {"tenant2", "sys2", "description 2", "owner2", "host2", "bucket2", "/root2",
      "jobInputDir2", "jobOutputDir2", "workDir2", "scratchDir2", "effUser2", "cpassword2", "tpassword2"};

  int cmdProtId;
  int txfProtId;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDao();
    commandProtocolDao = new CommandProtocolDao();
    transferProtocolDao = new TransferProtocolDao();
    cmdProtId = commandProtocolDao.create(CommandProtocol.Mechanism.NONE.name(), 0, false, "",0);
    txfProtId = transferProtocolDao.create(TransferProtocol.Mechanism.NONE.name(), 0, false, "",0);
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    dao.createTSystem(sys1[0], sys1[1], sys1[2], sys1[3], sys1[4], true, sys1[5], sys1[6],
                      sys1[7], sys1[8], sys1[9], sys1[10], sys1[11], cmdProtId, txfProtId, sys1[12], sys1[13]);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByName() throws Exception {
    TSystem tmpSys = dao.getTSystemByName(sys1[1]);
    System.out.println("Found system: " + sys1[1]);
    Assert.assertEquals(tmpSys.getName(), sys1[1]);
  }

  // Test retrieving all systems
  @Test(enabled=true)
  public void testGetSystems() throws Exception {
    List<TSystem> systems = dao.getTSystems();
    for (TSystem system : systems) {
      System.out.println("Found system: " + system.getName());
    }
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    dao.createTSystem(sys2[0], sys2[1], sys2[2], sys2[3], sys2[4], true, sys2[5], sys2[6], sys2[7],
                      sys2[8], sys2[9], sys2[10], sys2[11], cmdProtId, txfProtId, sys2[12], sys2[13]);
    dao.deleteTSystem(sys2[0], sys2[1]);
    TSystem tmpSystem = dao.getTSystemByName(sys2[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys2[1]);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
//    dao.deleteTSystem(sys1[0], sys1[1]);
//    TSystem tmpSystem = dao.getTSystemByName(sys1[1]);
//    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys1[1]);
    // TODO: more objects created, clean them up
  }
}