package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.SearchUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;
import static org.testng.Assert.assertEquals;

/**
 * Test the SystemsDao getTSystems() call for various search use cases against a DB running locally
 * NOTE: This test pre-processes the search list just as is done in SystemsServiceImpl before it calls the Dao,
 *       including calling SearchUtils.validateAndProcessSearchCondition(cond)
 *       For this reason there is currently no need to have a SearchSystemsTest suite.
 *       If this changes then we will need to create another suite and move the test data into IntegrationUtils so that
 *       it can be re-used.
 * TODO: Test that timestamps are handling timezone correctly.
 */
@Test(groups={"integration"})
public class SearchDaoTest
{
  private SystemsDaoImpl dao;
  private AuthenticatedUser authenticatedUser;

  // Test data
  private static final String testKey = "SrchGet";
  private static final String sysNameLikeAll = "*" + testKey + "*";

  // Strings for searches involving special characters
  private static final String specialChar7Str = ",()~*!\\"; // These 7 may need escaping
  private static final String specialChar7LikeSearchStr = "\\,\\(\\)\\~\\*\\!\\\\"; // All need escaping for LIKE/NLIKE
  private static final String specialChar7EqSearchStr = "\\,\\(\\)\\~*!\\"; // All but *! need escaping for other operators

  // Timestamps in various formats
  private static final String longPast1 =   "1800-01-01T00:00:00.123456Z";
  private static final String farFuture1 =  "2200-04-29T14:15:52.123456-06:00";
  private static final String farFuture2 =  "2200-04-29T14:15:52.123Z";
  private static final String farFuture3 =  "2200-04-29T14:15:52.123";
  private static final String farFuture4 =  "2200-04-29T14:15:52-06:00";
  private static final String farFuture5 =  "2200-04-29T14:15:52";
  private static final String farFuture6 =  "2200-04-29T14:15-06:00";
  private static final String farFuture7 =  "2200-04-29T14:15";
  private static final String farFuture8 =  "2200-04-29T14-06:00";
  private static final String farFuture9 =  "2200-04-29T14";
  private static final String farFuture10 = "2200-04-29-06:00";
  private static final String farFuture11 = "2200-04-29";
  private static final String farFuture12 = "2200-04Z";
  private static final String farFuture13 = "2200-04";
  private static final String farFuture14 = "2200Z";
  private static final String farFuture15 = "2200";

  // String for search involving an escaped comma in a list of values
  private static final String escapedCommanInListValue = "abc\\,def";

  // Strings for char relational testings
  private static final String hostName1 = "host" + testKey + "_001";
  private static final String hostName7 = "host" + testKey + "_007";

  int numSystems = 20;
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  private LocalDateTime createBegin;
  private LocalDateTime createEnd;

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SearchDaoTest.class.getSimpleName());
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
    System.out.println("Executing AfterSuite teardown for " + SearchDaoTest.class.getSimpleName());
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
    // NOTE: Some cases require "name.like." + sysNameLikeAll in the list of conditions since maven runs the tests in
    //       parallel and not all attribute names are unique across integration tests
    class CaseData {public final int count; public final List<String> searchList; CaseData(int c, List<String> r) { count = c; searchList = r; }}
    var validCaseInputs = new HashMap<Integer, CaseData>();
    // Test basic types and operators
    validCaseInputs.put( 1,new CaseData(1, Arrays.asList("name.eq." + sys0Name))); // 1 has specific name
    validCaseInputs.put( 2,new CaseData(1, Arrays.asList("description.eq." + sys0.getDescription())));
    validCaseInputs.put( 3,new CaseData(1, Arrays.asList("host.eq." + sys0.getHost())));
    validCaseInputs.put( 4,new CaseData(1, Arrays.asList("bucket_name.eq." + sys0.getBucketName())));
    validCaseInputs.put( 5,new CaseData(1, Arrays.asList("root_dir.eq." + sys0.getRootDir())));
    validCaseInputs.put( 6,new CaseData(1, Arrays.asList("job_local_working_dir.eq." + sys0.getJobLocalWorkingDir())));
    validCaseInputs.put( 7,new CaseData(1, Arrays.asList("job_local_archive_dir.eq." + sys0.getJobLocalArchiveDir())));
    validCaseInputs.put( 8,new CaseData(1, Arrays.asList("job_remote_archive_system.eq." + sys0.getJobRemoteArchiveSystem())));
    validCaseInputs.put( 9,new CaseData(1, Arrays.asList("job_remote_archive_dir.eq." + sys0.getJobRemoteArchiveDir())));
    validCaseInputs.put(10,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "owner.eq." + ownerUser)));  // Half owned by one user
    validCaseInputs.put(11,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "owner.eq." + ownerUser2))); // and half owned by another
    validCaseInputs.put(12,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true")));  // All are enabled
    validCaseInputs.put(13,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "deleted.eq.false"))); // none are deleted
    validCaseInputs.put(14,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "deleted.neq.true"))); // none are deleted
    validCaseInputs.put(15,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "deleted.eq.true")));           // none are deleted
    validCaseInputs.put(16,new CaseData(1, Arrays.asList("name.like." + sys0Name)));
    validCaseInputs.put(17,new CaseData(0, Arrays.asList("name.like.NOSUCHSYSTEMxFM2c29bc8RpKWeE2sht7aZrJzQf3s")));
    validCaseInputs.put(18,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll)));
    validCaseInputs.put(19,new CaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "name.nlike." + sys0Name)));
    validCaseInputs.put(20,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "name.in." + nameList)));
    validCaseInputs.put(21,new CaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "name.nin." + nameList)));
    validCaseInputs.put(22,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX")));
    validCaseInputs.put(23,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX","owner.neq." + ownerUser2)));
    // Test numeric relational
    validCaseInputs.put(40,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "port.between.1," + numSystems/2)));
    validCaseInputs.put(41,new CaseData(numSystems/2-1, Arrays.asList("name.like." + sysNameLikeAll, "port.between.2," + numSystems/2)));
    validCaseInputs.put(42,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "port.nbetween.1," + numSystems/2)));
    validCaseInputs.put(43,new CaseData(13, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","port.lte.13")));
    validCaseInputs.put(44,new CaseData(5, Arrays.asList("name.like." + sysNameLikeAll,"enabled.eq.true","port.gt.1","port.lt.7")));
    // Test char relational
    validCaseInputs.put(50,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll,"host.lte."+hostName1)));
    validCaseInputs.put(51,new CaseData(numSystems-7, Arrays.asList("name.like." + sysNameLikeAll,"enabled.eq.true","host.gt."+hostName7)));
    validCaseInputs.put(52,new CaseData(5, Arrays.asList("name.like." + sysNameLikeAll,"host.gt."+hostName1,"host.lt."+hostName7)));
    validCaseInputs.put(53,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll,"host.lte."+hostName1,"host.gt."+hostName7)));
    validCaseInputs.put(54,new CaseData(7, Arrays.asList("name.like." + sysNameLikeAll,"host.between."+hostName1+","+hostName7)));
    validCaseInputs.put(55,new CaseData(numSystems-7, Arrays.asList("name.like." + sysNameLikeAll,"host.nbetween."+hostName1+","+hostName7)));
    // Test timestamp relational
    validCaseInputs.put(60,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.gt." + longPast1)));
    validCaseInputs.put(61,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture1)));
    validCaseInputs.put(62,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "created.lte." + longPast1)));
    validCaseInputs.put(63,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "created.gte." + farFuture1)));
    validCaseInputs.put(64,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.between." + longPast1 + "," + farFuture1)));
    validCaseInputs.put(65,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "created.nbetween." + longPast1 + "," + farFuture1)));
    // Variations of timestamp format
    validCaseInputs.put(66,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture2)));
    validCaseInputs.put(67,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture3)));
    validCaseInputs.put(68,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture4)));
    validCaseInputs.put(69,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture5)));
    validCaseInputs.put(70,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture6)));
    validCaseInputs.put(71,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture7)));
    validCaseInputs.put(72,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture8)));
    validCaseInputs.put(73,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture9)));
    validCaseInputs.put(74,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture10)));
    validCaseInputs.put(75,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture11)));
    validCaseInputs.put(76,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture12)));
    validCaseInputs.put(77,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture13)));
    validCaseInputs.put(78,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture14)));
    validCaseInputs.put(79,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "created.lt." + farFuture15)));
    // Test wildcards
    validCaseInputs.put(80,new CaseData(numSystems, Arrays.asList("enabled.eq.true","host.like.host" + testKey + "*")));
    validCaseInputs.put(81,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "*")));
    validCaseInputs.put(82,new CaseData(9, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.like.host" + testKey + "_00!")));
    validCaseInputs.put(83,new CaseData(11, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "_00!")));
    // Test that underscore and % get escaped as needed before being used as SQL
    validCaseInputs.put(90,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00_")));
    validCaseInputs.put(91,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00%")));
    // Check various special characters in description. 7 special chars in value: ,()~*!\
    validCaseInputs.put(101,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "description.like." + specialChar7LikeSearchStr)));
    validCaseInputs.put(102,new CaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "description.nlike." + specialChar7LikeSearchStr)));
    validCaseInputs.put(103,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "description.eq." + specialChar7EqSearchStr)));
    validCaseInputs.put(104,new CaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "description.neq." + specialChar7EqSearchStr)));
    // Escaped comma in a list of values
    validCaseInputs.put(110,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "job_local_archive_dir.in." + "noSuchDir," + escapedCommanInListValue)));

    // Iterate over valid cases
    for (Map.Entry<Integer,CaseData> item : validCaseInputs.entrySet())
    {
      CaseData cd = item.getValue();
      int caseNum = item.getKey();
      System.out.println("Checking case # " + caseNum + " Input:        " + cd.searchList);
      // Build verified list of search conditions
      var verifiedSearchList = new ArrayList<String>();
      for (String cond : cd.searchList)
      {
        // Use SearchUtils to validate condition
        String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
        verifiedSearchList.add(verifiedCondStr);
      }
      System.out.println("  For case    # " + caseNum + " VerfiedInput: " + verifiedSearchList);
      List<TSystem> searchResults = dao.getTSystems(tenantName, verifiedSearchList, null);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), cd.count,  "SearchDaoTest.testValidCases: Incorrect result count for case number: " + caseNum);
    }
  }
}