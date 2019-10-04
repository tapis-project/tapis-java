package edu.utexas.tacc.tapis.systems.dao;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;

/**
 * Test the SystemsDao class against a running DB
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDao dao;

  // Test data
  private static final String[] sys1 = {"tenant1", "sys1", "description 1", "owner1", "host1", "bucket1", "/root1", "effUser1"};
  private static final String[] sys2 = {"tenant2", "sys2", "description 2", "owner2", "host2", "bucket2", "/root2", "effUser2"};


  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDao();
  }

  // Test create for a single system
  @Test(enabled=true)
  public void testCreateSystem() throws Exception
  {
    dao.createSystem(sys1[0], sys1[1], sys1[2], sys1[3], sys1[4],true, sys1[5], sys1[6], sys1[7]);
  }

  // Test getSystemByName
  @Test(enabled=true)
  public void testGetSystemByName() throws Exception {
    TSystem tmpSys = dao.getSystemByName(sys1[1]);
    System.out.println("Found system: " + sys1[1]);
    Assert.assertEquals(tmpSys.getName(), sys1[1]);
  }

  // Test retrieving all systems
  @Test(enabled=true)
  public void testGetSystems() throws Exception {
    List<TSystem> systems = dao.getSystems();
    for (TSystem system : systems) {
      System.out.println("Found system: " + system.getName());
    }
  }

  // Test deleting a single system
  @Test(enabled=true)
  public void testDeleteSystem() throws Exception {
    dao.createSystem(sys2[0], sys2[1], sys2[2], sys2[3], sys2[4],true, sys2[5], sys2[6], sys2[7]);
    dao.deleteSystem(sys2[0], sys2[1]);
    TSystem tmpSystem = dao.getSystemByName(sys2[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys2[1]);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.deleteSystem(sys1[0], sys1[1]);
    TSystem tmpSystem = dao.getSystemByName(sys1[1]);
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + sys1[1]);
  }
}