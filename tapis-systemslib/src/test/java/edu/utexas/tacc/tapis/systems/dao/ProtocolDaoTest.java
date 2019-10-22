package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Test the ProtocolDao class against a running DB
 */
@Test(groups={"integration"})
public class ProtocolDaoTest
{
  private ProtocolDao dao;

  // Test data
  private static String mechsStr = "{SFTP,S3}";
  private static List<TransferMechanism> mechs = new ArrayList<>(List.of(TransferMechanism.SFTP, TransferMechanism.S3));
  private static String mechsStrEmpty = "{}";
  private static List<TransferMechanism> mechsEmpty = new ArrayList<>();
  private static final Protocol item1 = new Protocol(-1, AccessMechanism.NONE, mechs, 0, false, "",
                                                     0, Instant.now());
  private static final Protocol item2 = new Protocol(-1, AccessMechanism.ANONYMOUS, mechs, 22, false, "",
                                                     0, Instant.now());
  private static final Protocol item3 = new Protocol(-1, AccessMechanism.SSH_CERT, mechs, 23, true, "localhost",
                                                     22, Instant.now());
  private static final Protocol item4 = new Protocol(-1, AccessMechanism.SSH_CERT, null, -1, true, null,
                                                     -1, Instant.now());
  int id1, id2, id3, id4;

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
    id1 = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.isUseProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id1);
  }

  // Test retrieving a single item by Id
  @Test(enabled=true)
  public void testGetById() throws Exception {
    Protocol item0 = item2;
    id2 = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.isUseProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id2);
    Protocol tmpItem = null;
    // Get by Id
    tmpItem = dao.getById(id2);
    Assert.assertNotNull(tmpItem, "Protocol item not found: " + item0.toString() + " id: " + id2);
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertEquals(tmpItem.getId(), id2);
    System.out.println("Found item: " + item0.toString());
    Assert.assertEquals(tmpItem.getAccessMechanism(), item0.getAccessMechanism());
    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
    Assert.assertEquals(tmpItem.isUseProxy(), item0.isUseProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), item0.getProxyHost());
    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
    List<TransferMechanism> tmechsList = tmpItem.getTransferMechanisms();
    Assert.assertNotNull(tmechsList);
    Assert.assertTrue(tmechsList.contains(TransferMechanism.S3), "List of transfer mechanisms did not contain: " + TransferMechanism.S3.name());
    Assert.assertTrue(tmechsList.contains(TransferMechanism.SFTP), "List of transfer mechanisms did not contain: " + TransferMechanism.SFTP.name());
  }

  // Test deleting a single item by Id
  @Test(enabled=true)
  public void testDelete() throws Exception {
    Protocol item0 = item3;
    int id = dao.create(item0.getAccessMechanism(), mechsStr, item0.getPort(), item0.isUseProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id);
    dao.delete(id);
    Protocol tmpItem = dao.getById(id);
    Assert.assertNull(tmpItem, "Protocol not deleted. Object: " + item0.toString() + " id: " + id);
  }

  // Test create and get for a single item with no transfer mechanisms supported
  @Test(enabled=true)
  public void testNoTxfr() throws Exception
  {
    Protocol item0 = item4;
    id4 = dao.create(item0.getAccessMechanism(), null, item0.getPort(), item0.isUseProxy(), item0.getProxyHost(), item0.getProxyPort());
    System.out.println("Created object with id: " + id4);
    Protocol tmpItem = null;
    // Get by Id
    tmpItem = dao.getById(id4);
    Assert.assertNotNull(tmpItem, "Protocol item not found: " + item0.toString() + " id: " + id4);
    System.out.println("Found object with id: " + tmpItem.getId());
    Assert.assertEquals(tmpItem.getId(), id4);
    System.out.println("Found item: " + item0.toString());
    Assert.assertEquals(tmpItem.getAccessMechanism(), item0.getAccessMechanism());
    Assert.assertEquals(tmpItem.getPort(), item0.getPort());
    Assert.assertEquals(tmpItem.isUseProxy(), item0.isUseProxy());
    Assert.assertEquals(tmpItem.getProxyHost(), "");
    Assert.assertEquals(tmpItem.getProxyPort(), item0.getProxyPort());
    List<TransferMechanism> tmechsList = tmpItem.getTransferMechanisms();
    Assert.assertNotNull(tmechsList);
    Assert.assertEquals(tmechsList.size(), 0);
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method");
    //Remove all objects created by tests
    dao.delete(id1);
    Protocol tmpItem = dao.getById(id1);
    Assert.assertNull(tmpItem, "Protocol not deleted. Object: " + item1.toString());
    dao.delete(id2);
    dao.delete(id4);
  }
}