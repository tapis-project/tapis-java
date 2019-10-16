package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.CommandProtocol;
import edu.utexas.tacc.tapis.systems.model.CommandProtocol.Mechanism;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;

/**
 * Test the CommandProtocolDao class against a running DB
 */
@Test(groups={"integration"})
public class CommandProtocolTest
{
  private CommandProtocolDao dao;

  // Test data
  private static final CommandProtocol item1 = new CommandProtocol(-1, Mechanism.NONE, 0, false, "",
                                                                   0, Instant.now());
  private static final CommandProtocol item2 = new CommandProtocol(-1, Mechanism.SSH_ANONYMOUS, 22, false, "",
                                                                   0, Instant.now());
  private static final CommandProtocol item3 = new CommandProtocol(-1, Mechanism.SSH_CERT, 23, true, "localhost",
                                                                   22, Instant.now());
  private static final CommandProtocol item4 = new CommandProtocol(-1, Mechanism.SSH_PASSWORD, 2222, false, "",
                                                                   0, Instant.now());
  int id1, id2, id3;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new CommandProtocolDao();
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    CommandProtocol item0 = item1;
    id1 = dao.create(item0.getMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id1);
  }

  // Test retrieving a single item by Id and by Value
  @Test(enabled=true)
  public void testGet() throws Exception {
    CommandProtocol item0 = item2;
    id2 = dao.create(item0.getMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id2);
    // Get by Value
    CommandProtocol tmpItem = dao.getByValue(item0.getMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertNotNull(tmpItem, "CommandProtocol item not found: " + item0.toString() + " id: " + id2);
    System.out.println("Found item: " + item0.toString());
    Assert.assertEquals(tmpItem.getId(), id2);
    Assert.assertEquals(tmpItem.getMechanism(), item0.getMechanism());
    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
    Assert.assertEquals(tmpItem.useProxy(), item0.useProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), item0.getProxyHost());
    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
    // Get by Id
    item0 = item3;
    id3 = dao.create(item0.getMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id3);
    tmpItem = dao.getById(id3);
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertNotNull(tmpItem, "CommandProtocol item not found: " + item0.toString() + " id: " + id3);
    Assert.assertEquals(tmpItem.getId(), id3);
    System.out.println("Found item: " + item0.toString());
    Assert.assertEquals(tmpItem.getMechanism(), item0.getMechanism());
    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
    Assert.assertEquals(tmpItem.useProxy(), item0.useProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), item0.getProxyHost());
    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
  }

  // Test deleting a single item by Id
  @Test(enabled=true)
  public void testDelete() throws Exception {
    CommandProtocol item0 = item4;
    int id = dao.create(item0.getMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id);
    dao.delete(id);
    CommandProtocol tmpItem = dao.getById(id);
    Assert.assertNull(tmpItem, "CommandProtocol not deleted. Object: " + item0.toString() + " id: " + id);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.delete(id1);
    CommandProtocol tmpItem = dao.getByValue(item1.getMechanism(), item1.getPort(), item1.useProxy(), item1.getProxyHost(), item1.getProxyPort());
    Assert.assertNull(tmpItem, "CommandProtocol not deleted. Object: " + item1.toString());
    dao.delete(id2);
    dao.delete(id3);
  }
}