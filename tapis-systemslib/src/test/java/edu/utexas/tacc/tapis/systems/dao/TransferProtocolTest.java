package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.TransferProtocol;
import edu.utexas.tacc.tapis.systems.model.TransferProtocol.Mechanism;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;

/**
 * Test the TransferProtocolDao class against a running DB
 */
@Test(groups={"integration"})
public class TransferProtocolTest
{
  private TransferProtocolDao dao;

  // Test data
  private static final TransferProtocol item1 = new TransferProtocol(-1, Mechanism.NONE, 0, false, "",
                                                                   0, Instant.now());
  private static final TransferProtocol item2 = new TransferProtocol(-1, Mechanism.SFTP, 22, false, "",
                                                                   0, Instant.now());

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new TransferProtocolDao();
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    int id = dao.create(item1.getMechanism(), item1.getPort(), item1.useProxy(), item1.getProxyHost(), item1.getProxyPort());
    System.out.println("Created object with id: " + id);
  }

  // Test retrieving a single item
  @Test(enabled=true)
  public void testGetByValue() throws Exception {
    TransferProtocol tmpItem = dao.get(item1.getMechanism(), item1.getPort(), item1.useProxy(), item1.getProxyHost(), item1.getProxyPort());
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertNotNull(tmpItem, "TransferProtocol item not found: " + item1.toString());
    System.out.println("Found item: " + item1.toString());
    Assert.assertEquals(tmpItem.getMechanism(), item1.getMechanism());
    Assert.assertEquals(tmpItem.getPort(), item1.getPort());
    Assert.assertEquals(tmpItem.useProxy(), item1.useProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), item1.getProxyHost());
    Assert.assertEquals(tmpItem.getProxyPort(), item1.getProxyPort());
  }

  // Test deleting a single item
  @Test(enabled=true)
  public void testDelete() throws Exception {
    dao.create(item2.getMechanism(), item2.getPort(), item2.useProxy(), item2.getProxyHost(), item2.getProxyPort());
    dao.delete(item2.getMechanism(), item2.getPort(), item2.useProxy(), item2.getProxyHost(), item2.getProxyPort());
    TransferProtocol tmpItem = dao.get(item2.getMechanism(), item2.getPort(), item2.useProxy(), item2.getProxyHost(), item2.getProxyPort());
    Assert.assertNull(tmpItem, "TransferProtocol not deleted. Object: " + item2.toString());
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.delete(item1.getMechanism(), item1.getPort(), item1.useProxy(), item1.getProxyHost(), item1.getProxyPort());
    TransferProtocol tmpItem = dao.get(item1.getMechanism(), item1.getPort(), item1.useProxy(), item1.getProxyHost(), item1.getProxyPort());
    Assert.assertNull(tmpItem, "TransferProtocol not deleted. Object: " + item1.toString());
  }
}