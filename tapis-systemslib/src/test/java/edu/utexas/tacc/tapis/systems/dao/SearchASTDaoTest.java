package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsDao getTSystemsUsingSearchAST() call for various search use cases against a DB running locally
 * NOTE: This test pre-processes the sql-like search string just as is done in SystemsServiceImpl before it calls the Dao.
 *       For this reason there is currently no need to have a SearchSystemsTest suite.
 *       If this changes then we will need to create another suite and move the test data into IntegrationUtils so that
 *       it can be re-used.
 * TODO: Test that timestamps are handling timezone correctly.
 */
@Test(groups={"integration"})
public class SearchASTDaoTest
{
  private SystemsDaoImpl dao;
  private AuthenticatedUser authenticatedUser;

  // Test data
  private static final String testKey = "SrchAST";
  private static final String sysNameLikeAll = sq("%" + testKey + "%");

  // Strings for searches involving special characters
  private static final String specialChar7Str = ",()~*!\\"; // These 7 may need escaping
  private static final String specialChar7LikeSearchStr = "\\,\\(\\)\\~\\*\\!\\\\"; // All need escaping for LIKE/NLIKE
  private static final String specialChar7EqSearchStr = "\\,\\(\\)\\~*!\\"; // All but *! need escaping for other operators

  // String for search involving an escaped comma in a list of values
  private static final String escapedCommanInListValue = "abc\\,def";
//
//  // Timestamps in various formats
//  private static final String longPast1 =   "1800-01-01T00:00:00.123456Z";
//  private static final String farFuture1 =  "2200-04-29T14:15:52.123456-06:00";
//  private static final String farFuture2 =  "2200-04-29T14:15:52.123Z";
//  private static final String farFuture3 =  "2200-04-29T14:15:52.123";
//  private static final String farFuture4 =  "2200-04-29T14:15:52-06:00";
//  private static final String farFuture5 =  "2200-04-29T14:15:52";
//  private static final String farFuture6 =  "2200-04-29T14:15-06:00";
//  private static final String farFuture7 =  "2200-04-29T14:15";
//  private static final String farFuture8 =  "2200-04-29T14-06:00";
//  private static final String farFuture9 =  "2200-04-29T14";
//  private static final String farFuture10 = "2200-04-29-06:00";
//  private static final String farFuture11 = "2200-04-29";
//  private static final String farFuture12 = "2200-04Z";
//  private static final String farFuture13 = "2200-04";
//  private static final String farFuture14 = "2200Z";
//  private static final String farFuture15 = "2200";
//
//  // Strings for char relational testings
//  private static final String hostName1 = "host" + testKey + "_001";
//  private static final String hostName7 = "host" + testKey + "_007";

  int numSystems = 20;
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  LocalDateTime createBegin;
  LocalDateTime createEnd;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SearchASTDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    authenticatedUser = new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(), null, apiUser, tenantName, null, null);

    // Cleanup anything leftover from previous failed run
    teardown();

    // Vary port # for checking numeric relational searches
    for (int i = 0; i < numSystems; i++) { systems[i].setPort(i+1); }
    // For half the systems change the owner
    for (int i = 0; i < numSystems/2; i++) { systems[i].setOwner(ownerUser2); }

    // For one system update description to have some special characters. 7 special chars in value: ,()~*!\
    //   and update archiveLocalDir for testing an escaped comma in a list value
    systems[numSystems-1].setDescription(specialChar7Str);
    systems[numSystems-1].setJobLocalArchiveDir(escapedCommanInListValue);

    // Create all the systems in the dB using the in-memory objects, recording start and end times
    createBegin = TapisUtils.getUTCTimeNow();
    Thread.sleep(500);
    for (TSystem sys : systems)
    {
      int itemId = dao.createTSystem(authenticatedUser, sys, gson.toJson(sys), scrubbedJson);
      Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    }
    Thread.sleep(500);
    createEnd = TapisUtils.getUTCTimeNow();
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + SearchASTDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (TSystem sys : systems)
    {
      dao.hardDeleteTSystem(tenantName, sys.getName());
    }

    TSystem tmpSystem = dao.getTSystemByName(tenantName, systems[0].getName());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + systems[0].getName());
  }

  /*
   * Check valid cases
   */
  @Test(groups={"integration"})
  public void testValidCases() throws Exception
  {
    TSystem sys0 = systems[0];
    String sys0Name = sys0.getName();
    String nameList = "noSuchName1,noSuchName2," + sys0Name + ",noSuchName3";
    // Create all input and validation data for tests
    // NOTE: Some cases require "name LIKE " + sysNameLikeAll in the list of conditions since maven runs the tests in
    //       parallel and not all attribute names are unique across integration tests
    class CaseData {public final int count; public final String sqlSearchStr; CaseData(int c, String s) { count = c; sqlSearchStr = s; }}
    var validCaseInputs = new HashMap<Integer, CaseData>();
    // Test basic types and operators
    validCaseInputs.put( 1,new CaseData(1, "name = " + sys0Name)); // 1 has specific name
//    validCaseInputs.put( 2,new CaseData(1, "description = " + sys0.getDescription())); // TODO handle underscore character properly. how?
    validCaseInputs.put( 3,new CaseData(1, "host = " + sys0.getHost()));
    validCaseInputs.put( 4,new CaseData(1, "bucket_name = " + sys0.getBucketName()));
//    validCaseInputs.put( 5,new CaseData(1, "root_dir = " + sys0.getRootDir())); // TODO underscore
    validCaseInputs.put( 6,new CaseData(1, "job_local_working_dir = " + sys0.getJobLocalWorkingDir()));
    validCaseInputs.put( 7,new CaseData(1, "job_local_archive_dir = " + sys0.getJobLocalArchiveDir()));
    validCaseInputs.put( 8,new CaseData(1, "job_remote_archive_system = " + sys0.getJobRemoteArchiveSystem()));
    validCaseInputs.put( 9,new CaseData(1, "job_remote_archive_dir = " + sys0.getJobRemoteArchiveDir()));
    validCaseInputs.put(10,new CaseData(numSystems/2, "name LIKE " + sysNameLikeAll + " AND owner = " + sq(ownerUser)));  // Half owned by one user
    validCaseInputs.put(11,new CaseData(numSystems/2, "name LIKE " + sysNameLikeAll + " AND owner = " + sq(ownerUser2))); // and half owned by another
    validCaseInputs.put(12,new CaseData(numSystems, "name LIKE " + sysNameLikeAll + " AND enabled = true"));  // All are enabled
    validCaseInputs.put(13,new CaseData(numSystems, "name LIKE " + sysNameLikeAll + " AND deleted = false")); // none are deleted
    validCaseInputs.put(14,new CaseData(numSystems, "name LIKE " + sysNameLikeAll + " AND deleted <> true")); // none are deleted
    validCaseInputs.put(15,new CaseData(0, "name LIKE " + sysNameLikeAll + " AND deleted = true"));           // none are deleted
    validCaseInputs.put(16,new CaseData(1, "name LIKE " + sq(sys0Name)));
    validCaseInputs.put(17,new CaseData(0, "name LIKE 'NOSUCHSYSTEMxFM2c29bc8RpKWeE2sht7aZrJzQf3s'"));
    validCaseInputs.put(18,new CaseData(numSystems, "name LIKE " + sysNameLikeAll));
// TODO - continue
//    validCaseInputs.put(19,new CaseData(numSystems-1, "name LIKE " + sysNameLikeAll + " AND name NLIKE " + sys0Name)); // TODO support NLIKE
//    validCaseInputs.put(20,new CaseData(1, "name LIKE " + sysNameLikeAll + " AND name IN " + nameList)); // TODO
//    validCaseInputs.put(21,new CaseData(numSystems-1, "name LIKE " + sysNameLikeAll, "name.nin." + nameList));
//    validCaseInputs.put(22,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "system_type = LINUX"));
//    validCaseInputs.put(23,new CaseData(numSystems/2, "name LIKE " + sysNameLikeAll, "system_type = LINUX","owner <> " + sq(ownerUser2)));
//    // Test numeric relational
//    validCaseInputs.put(50,new CaseData(numSystems/2, "name LIKE " + sysNameLikeAll, "port.between.1," + numSystems/2));
//    validCaseInputs.put(51,new CaseData(numSystems/2-1, "name LIKE " + sysNameLikeAll, "port.between.2," + numSystems/2));
//    validCaseInputs.put(52,new CaseData(numSystems/2, "name LIKE " + sysNameLikeAll, "port.nbetween.1," + numSystems/2));
//    validCaseInputs.put(53,new CaseData(13, "name LIKE " + sysNameLikeAll, "enabled = true","port.lte.13"));
//    validCaseInputs.put(54,new CaseData(5, "name LIKE " + sysNameLikeAll,"enabled = true","port.gt.1","port.lt.7"));
//    // Test char relational
//    validCaseInputs.put(70,new CaseData(1, "name LIKE " + sysNameLikeAll,"host.lt."+hostName1));
//    validCaseInputs.put(71,new CaseData(numSystems-8, "name LIKE " + sysNameLikeAll,"enabled = true","host.gt."+hostName7));
//    validCaseInputs.put(72,new CaseData(5, "name LIKE " + sysNameLikeAll,"host.gt."+hostName1,"host.lt."+hostName7));
//    validCaseInputs.put(73,new CaseData(0, "name LIKE " + sysNameLikeAll,"host.lt."+hostName1,"host.gt."+hostName7));
//    validCaseInputs.put(74,new CaseData(7, "name LIKE " + sysNameLikeAll,"host.between."+hostName1+","+hostName7));
//    validCaseInputs.put(75,new CaseData(numSystems-7, "name LIKE " + sysNameLikeAll,"host.nbetween."+hostName1+","+hostName7));
//    // Test timestamp relational
//    validCaseInputs.put(90,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.gt." + longPast1));
//    validCaseInputs.put(91,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture1));
//    validCaseInputs.put(92,new CaseData(0, "name LIKE " + sysNameLikeAll, "created.lte." + longPast1));
//    validCaseInputs.put(93,new CaseData(0, "name LIKE " + sysNameLikeAll, "created.gte." + farFuture1));
//    validCaseInputs.put(94,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.between." + longPast1 + "," + farFuture1));
//    validCaseInputs.put(95,new CaseData(0, "name LIKE " + sysNameLikeAll, "created.nbetween." + longPast1 + "," + farFuture1));
//    // Variations of timestamp format
//    validCaseInputs.put(96,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture2));
//    validCaseInputs.put(97,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture3));
//    validCaseInputs.put(98,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture4));
//    validCaseInputs.put(99,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture5));
//    validCaseInputs.put(100,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture6));
//    validCaseInputs.put(101,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture7));
//    validCaseInputs.put(102,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture8));
//    validCaseInputs.put(103,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture9));
//    validCaseInputs.put(104,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture10));
//    validCaseInputs.put(105,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture11));
//    validCaseInputs.put(106,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture12));
//    validCaseInputs.put(107,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture13));
//    validCaseInputs.put(108,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture14));
//    validCaseInputs.put(109,new CaseData(numSystems, "name LIKE " + sysNameLikeAll, "created.lt." + farFuture15));
//    // Test wildcards
//    validCaseInputs.put(130,new CaseData(numSystems, "enabled = true","host LIKE host" + testKey + "*"));
//    validCaseInputs.put(131,new CaseData(0, "name LIKE " + sysNameLikeAll, "enabled = true","host NLIKE host" + testKey + "*"));
//    validCaseInputs.put(132,new CaseData(10, "name LIKE " + sysNameLikeAll, "enabled = true","host LIKE host" + testKey + "_00!"));
//    validCaseInputs.put(133,new CaseData(10, "name LIKE " + sysNameLikeAll, "enabled = true","host NLIKE host" + testKey + "_00!"));
//    // Test that underscore and % get escaped as needed before being used as SQL
//    validCaseInputs.put(150,new CaseData(0, "name LIKE " + sysNameLikeAll, "host LIKE host" + testKey + "_00_"));
//    validCaseInputs.put(151,new CaseData(0, "name LIKE " + sysNameLikeAll, "host LIKE host" + testKey + "_00%"));
//    // Check various special characters in description. 7 special chars in value: ,()~*!\
//    validCaseInputs.put(171,new CaseData(1, "name LIKE " + sysNameLikeAll, "description LIKE " + specialChar7LikeSearchStr));
//    validCaseInputs.put(172,new CaseData(numSystems-1, "name LIKE " + sysNameLikeAll, "description NLIKE " + specialChar7LikeSearchStr));
//    validCaseInputs.put(173,new CaseData(1, "name LIKE " + sysNameLikeAll, "description = " + specialChar7EqSearchStr));
//    validCaseInputs.put(174,new CaseData(numSystems-1, "name LIKE " + sysNameLikeAll, "description <> " + specialChar7EqSearchStr));
//    // Escaped comma in a list of values
//    validCaseInputs.put(200,new CaseData(1, "name LIKE " + sysNameLikeAll, "job_local_archive_dir IN " + "noSuchDir," + escapedCommanInListValue));

    // Iterate over valid cases
    for (Map.Entry<Integer,CaseData> item : validCaseInputs.entrySet())
    {
      CaseData cd = item.getValue();
      int caseNum = item.getKey();
      System.out.println("Checking case # " + caseNum + " Input:        " + cd.sqlSearchStr);
      // Build an AST from the sql-like search string
      ASTNode searchAST = ASTParser.parse(cd.sqlSearchStr);
      System.out.println("  Created AST with leaf node count: " + searchAST.countLeaves());
      List<TSystem> searchResults = dao.getTSystemsUsingSearchAST(tenantName, searchAST, null);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), cd.count, "SearchASTDaoTest.testValidCases: Incorrect result count for case number: " + caseNum);
    }
  }

  private static String sq(String s) { return "'" + s + "'"; }
}
