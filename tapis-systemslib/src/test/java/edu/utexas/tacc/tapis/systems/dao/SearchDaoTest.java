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
import java.util.List;

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
  private static final String specailCharStr7 = "\\,\\(\\)\\~\\*\\!\\\\";

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
    systems[numSystems-1].setDescription(specailCharStr7);

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
    class ValidCaseData {public final int count; public final List<String> searchList; ValidCaseData(int c, List<String> r) { count = c; searchList = r; }}
    ValidCaseData[] validCaseData = {
/*00*/new ValidCaseData(1, Arrays.asList("name.eq." + sys0Name)), // 1 has specific name
/*01*/new ValidCaseData(1, Arrays.asList("description.eq." + sys0.getDescription())),
/*02*/new ValidCaseData(1, Arrays.asList("host.eq." + sys0.getHost())),
/*03*/new ValidCaseData(1, Arrays.asList("bucket_name.eq." + sys0.getBucketName())),
/*04*/new ValidCaseData(1, Arrays.asList("root_dir.eq." + sys0.getRootDir())),
/*05*/new ValidCaseData(1, Arrays.asList("job_local_working_dir.eq." + sys0.getJobLocalWorkingDir())),
/*06*/new ValidCaseData(1, Arrays.asList("job_local_archive_dir.eq." + sys0.getJobLocalArchiveDir())),
/*06*/new ValidCaseData(1, Arrays.asList("job_remote_archive_system.eq." + sys0.getJobRemoteArchiveSystem())),
/*07*/new ValidCaseData(1, Arrays.asList("job_remote_archive_dir.eq." + sys0.getJobRemoteArchiveDir())),
/*08*/new ValidCaseData(numSystems/2, Arrays.asList("owner.eq." + ownerUser)),  // Half owned by one user
/*09*/new ValidCaseData(numSystems/2, Arrays.asList("owner.eq." + ownerUser2)), // and half owned by another
/*10*/new ValidCaseData(numSystems, Arrays.asList("enabled.eq.true")),  // All are enabled
/*11*/new ValidCaseData(numSystems, Arrays.asList("deleted.eq.false")), // none are deleted
/*12*/new ValidCaseData(numSystems, Arrays.asList("deleted.neq.true")), // none are deleted
/*13*/new ValidCaseData(0, Arrays.asList("deleted.eq.true")),           // none are deleted
/*14*/new ValidCaseData(1, Arrays.asList("name.like." + sys0Name)),
/*15*/new ValidCaseData(0, Arrays.asList("name.like.NOSUCHSYSTEMxFM2c29bc8RpKWeE2sht7aZrJzQf3s")),
/*16*/new ValidCaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll)),
/*17*/new ValidCaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "name.nlike." + sys0Name)),
/*18*/new ValidCaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "name.in." + nameList)),
/*19*/new ValidCaseData(numSystems-1, Arrays.asList("name.like." + sysNameLikeAll, "name.nin." + nameList)),
/*20*/new ValidCaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "port.between.1," + numSystems/2)),
      new ValidCaseData(numSystems/2-1, Arrays.asList("name.like." + sysNameLikeAll, "port.between.2," + numSystems/2)),
            // TODO Test timestamp type
      new ValidCaseData(numSystems, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX")),
      new ValidCaseData(numSystems/2, Arrays.asList("name.like." + sysNameLikeAll, "system_type.eq.LINUX","owner.neq." + ownerUser2)),
      new ValidCaseData(numSystems, Arrays.asList("enabled.eq.true","host.like.host" + testKey + "*")),
/*25*/new ValidCaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "*")),
      new ValidCaseData(10, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.like.host" + testKey + "_00!")),
      new ValidCaseData(10, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","host.nlike.host" + testKey + "_00!")),
      new ValidCaseData(13, Arrays.asList("name.like." + sysNameLikeAll, "enabled.eq.true","port.lte.13")),
/*29*/new ValidCaseData(5, Arrays.asList("name.like." + sysNameLikeAll,"enabled.eq.true","port.gt.1","port.lt.7")),
            // TODO Test char relational
            // TODO Test timestamp relational
      // Test that underscore and % get escaped as needed before being used as SQL
/*30*/new ValidCaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00_")),
      new ValidCaseData(0, Arrays.asList("name.like." + sysNameLikeAll, "host.like.host" + testKey + "_00%")),
      // TODO check various special characters in description. 7 special chars in value: ,()~*!\
      new ValidCaseData(1, Arrays.asList("name.like." + sysNameLikeAll, "description.eq." + specailCharStr7))
    };

    // Iterate over valid cases
    for (int i = 0; i < validCaseData.length; i++)
    {
      List<String> searchListTmp = validCaseData[i].searchList;
      System.out.println("Checking case # " + i + " Input:        " + searchListTmp);
      // Build verified list of search conditions
      var verifiedSearchList = new ArrayList<String>();
      for (String cond : searchListTmp)
      {
        // Use SearchUtils to validate condition
        // Add parentheses if not present, check start and end
        if (!cond.startsWith("(") && !cond.endsWith(")")) cond = "(" + cond + ")";
        String verifiedCondStr = SearchUtils.validateAndExtractSearchCondition(cond);
        verifiedSearchList.add(verifiedCondStr);
      }
      System.out.println("  For case    # " + i + " VerfiedInput: " + verifiedSearchList);
      List<TSystem> searchResults = dao.getTSystems(tenantName, verifiedSearchList, null);
      System.out.println("  Result size: " + searchResults.size());
      assertEquals(searchResults.size(), validCaseData[i].count);
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