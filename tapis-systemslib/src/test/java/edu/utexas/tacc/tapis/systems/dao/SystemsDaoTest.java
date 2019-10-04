package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
/**
 * Test the SystemsDao class
 */
@Test(groups={"unit"})
public class SystemsDaoTest
{
  private SystemsDao dao;
  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new SystemsDao();
  }

  /**
   * Test create and get for a single system
   */
  @Test
  public void testCreateSystem() throws Exception
  {
    // TODO remove hard coded values
    dao.createSystem("tenant1", "sys1", "description1", "owner1", "host1",
                     true,"bucket1", "/root1", "effUser1");
    Assert.fail("Test not implemented");
  }

  @Test
  public void testGetSystemByName()
  {
    Assert.fail("Test not implemented");
  }

  @Test
  public void testGetSystems()
  {
    Assert.fail("Test not implemented");
  }

  @AfterSuite
  public void teardown()
  {
    System.out.println("Executing AfterSuite teardown method");
  }
}