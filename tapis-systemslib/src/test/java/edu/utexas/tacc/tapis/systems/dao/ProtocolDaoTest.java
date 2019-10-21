package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;

/**
 * Test the ProtocolDao class against a running DB
 */
@Test(groups={"integration"})
public class ProtocolDaoTest
{
  private ProtocolDao dao;

  // Test data
  private static String mechsStr = "{SFTP,S3}";
  private static TransferMechanism[] mechs = {TransferMechanism.SFTP, TransferMechanism.S3};
  private static final Protocol item1 = new Protocol(-1, AccessMechanism.NONE, mechs, 0, false, "",
                                                     0, Instant.now());
  private static final Protocol item2 = new Protocol(-1, AccessMechanism.ANONYMOUS, mechs, 22, false, "",
                                                     0, Instant.now());
  private static final Protocol item3 = new Protocol(-1, AccessMechanism.SSH_CERT, mechs, 23, true, "localhost",
                                                     22, Instant.now());
  private static final Protocol item4 = new Protocol(-1, AccessMechanism.SSH_PASSWORD, mechs, 2222, false, "",
                                                     0, Instant.now());
  int id1, id2, id3;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method");
    dao = new ProtocolDao();
  }

  // Test create for a single item
  @Test(enabled=true)
  public void testCreate() throws Exception
  {
    Protocol item0 = item1;
    id1 = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id1);
  }

  // Test retrieving a single item by Id and by Value
  @Test(enabled=true)
  public void testGet() throws Exception {
    Protocol item0 = item2;
    id2 = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id2);
    // Get by Value
    Protocol tmpItem = null;
//    Protocol tmpItem = dao.getByValue(item0.getAccessMechanism(), item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
//    System.out.println("Found object with id: " + tmpItem.getId());
//    Assert.assertNotNull(tmpItem, "Protocol item not found: " + item0.toString() + " id: " + id2);
//    System.out.println("Found item: " + item0.toString());
//    Assert.assertEquals(tmpItem.getId(), id2);
//    Assert.assertEquals(tmpItem.getAccessMechanism(), item0.getAccessMechanism());
//    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
//    Assert.assertEquals(tmpItem.useProxy(), item0.useProxy());
//    Assert.assertEquals(tmpItem.getProxyHost(), item0.getProxyHost());
//    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
    // Get by Id
    item0 = item3;
    id3 = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id3);
    tmpItem = dao.getById(id3);
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertNotNull(tmpItem, "Protocol item not found: " + item0.toString() + " id: " + id3);
    Assert.assertEquals(tmpItem.getId(), id3);
    System.out.println("Found item: " + item0.toString());
    Assert.assertEquals(tmpItem.getAccessMechanism(), item0.getAccessMechanism());
    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
    Assert.assertEquals(tmpItem.useProxy(), item0.useProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), item0.getProxyHost());
    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
  }

  // Test deleting a single item by Id
  @Test(enabled=true)
  public void testDelete() throws Exception {
    Protocol item0 = item4;
    int id = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.useProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id);
    dao.delete(id);
    Protocol tmpItem = dao.getById(id);
    Assert.assertNull(tmpItem, "Protocol not deleted. Object: " + item0.toString() + " id: " + id);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.delete(id1);
    Protocol tmpItem = dao.getById(id1);
    Assert.assertNull(tmpItem, "Protocol not deleted. Object: " + item1.toString());
    dao.delete(id2);
    dao.delete(id3);
  }
}