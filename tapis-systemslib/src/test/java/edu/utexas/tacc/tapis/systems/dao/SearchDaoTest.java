package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;
import static org.testng.Assert.assertEquals;

/**
 * Test the SystemsDao class for various search use cases against a DB running locally
 */
@Test(groups={"integration"})
public class SearchDaoTest
{
  private SystemsDaoImpl dao;
  private AuthenticatedUser authenticatedUser;

  // Test data
  private static final String ownerUser2 = "owner2";
  private static final String testKey = "Srch";
  private static final String specialChar7Str = ",()~*!\\";
  private static final String specialChar7LikeSearchStr = "\\,\\(\\)\\~\\*\\!\\\\";
  private static final String specialChar7EqSearchStr = ",()~*!\\";

  int numSystems = 20;
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsDaoTest.class.getSimpleName());
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
    systems[numSystems-1].setDescription(specialChar7Str);

    // Create all the systems in the dB using the in-memory objects
    for (TSystem sys : systems)
    {
      int itemId = dao.createTSystem(authenticatedUser, sys, gson.toJson(sys), scrubbedJson);
      Assert.assertTrue(itemId > 0, "Invalid system id: " + itemId);
    }
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
    String sysNameLikeAll = sysNamePrefix + "_" + testKey + "_*";
    // Create all input and validation data for tests
    // NOTE: Some cases require "name.like." + sysNameLikeAll in the list of conditions since maven runs the tests in
    //       parallel and not all attribute names are unique across integration tests
    class CaseData {public final int count; public final List<String> searchList; CaseData(int c, List<String> r) { count = c; searchList = r; }}
    var validCaseInputs = new HashMap<Integer, CaseData>();
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
    validCaseInputs.put(22,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "port.between.1," + numSystems/2)));
    validCaseInputs.put(23,new CaseData(numSystems/2-1, Arrays.asList("name.like." + sysNameLikeAll, "port.between.2," + numSystems/2)));
            // TODO Test timestamp type
    validCaseInputs.put(24,new CaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX")));
    validCaseInputs.put(25,new CaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX","owner.neq." + ownerUser2)));
    validCaseInputs.put(26,new CaseData(numSystems, Arrays.asList("enabled.eq.true","host.like.host" + testKey + "*")));
    validCaseInputs.put(27,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "*")));
    validCaseInputs.put(28,new CaseData(10, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.like.host" + testKey + "_00!")));
    validCaseInputs.put(29,new CaseData(10, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "_00!")));
    validCaseInputs.put(30,new CaseData(13, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","port.lte.13")));
    validCaseInputs.put(31,new CaseData(5, Arrays.asList("name.like." + sysNameLikeAll,"enabled.eq.true","port.gt.1","port.lt.7")));
    // TODO Test char relational
    // TODO Test timestamp relational
    // Test that underscore and % get escaped as needed before being used as SQL
    validCaseInputs.put(32,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00_")));
    validCaseInputs.put(33,new CaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00%")));
    // Check various special characters in description. 7 special chars in value: ,()~*!\
    validCaseInputs.put(34,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "description.like." + specialChar7LikeSearchStr)));
    // TODO: Currently require special chars to be escaped in value, but this is only working for LIKE operator.
//    validCaseInputs.put(35,new CaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "description.eq." + specialChar7EqSearchStr)));

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
        // Add parentheses if not present, check start and end
        if (!cond.startsWith("(") && !cond.endsWith(")")) cond = "(" + cond + ")";
        String verifiedCondStr = SearchUtils.validateAndExtractSearchCondition(cond);
        verifiedSearchList.add(verifiedCondStr);
      }
      System.out.println("  For case    # " + caseNum + " VerfiedInput: " + verifiedSearchList);
      List<TSystem> searchResults = dao.getTSystems(tenantName, verifiedSearchList, null);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), cd.count);
    }
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown method" + SystemsDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (TSystem sys : systems)
    {
      dao.hardDeleteTSystem(tenantName, sys.getName());
    }

    TSystem tmpSystem = dao.getTSystemByName(tenantName, systems[0].getName());
    Assert.assertNull(tmpSystem, "System not deleted. System name: " + systems[0].getName());
  }
}