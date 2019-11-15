package edu.utexas.tacc.tapis.systems.service;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all
 *   top level service operations.
 */
@Singleton
public class SystemsServiceImpl implements SystemsService
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  private static final String SYSTEM_OWNER_ROLE = "SystemOwner";

  // **************** Inject Dao singletons ****************
  @com.google.inject.Inject
  private SystemsDao dao;

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system object
   *
   * @return Sequence id of object created
   * @throws TapisException
   */
  @Override
  public int createSystem(String tenant, String name, String description, String owner, String host,
                          boolean available, String bucketName, String rootDir, String jobInputDir,
                          String jobOutputDir, String workDir, String scratchDir, String effectiveUserId, String tags,
                          String accessCredential, String accessMechanism, String transferMechanisms,
                          int protocolPort, boolean protocolUseProxy,
                          String protocolProxyHost, int protocolProxyPort)
          throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();


    int itemId = dao.createTSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                   jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId, tags,
                                   accessMechanism, transferMechanisms, protocolPort, protocolUseProxy,
                                   protocolProxyHost, protocolProxyPort);

    // TODO: Remove debug System.out statements

    // TODO Store credentials in Security Kernel
    // TODO Do real service location lookup, through tenants service?
    String tokBaseURL = "https://dev.develop.tapis.io";
//    String tokBaseURL = "http://c002.rodeo.tacc.utexas.edu:31357";
//    String skBaseURL = "http://c002.rodeo.tacc.utexas.edu:32169/security";
    String skBaseURL = "https://dev.develop.tapis.io/v3";
    // Get short term JWT from tokens service
    var tokClient = new TokensClient(tokBaseURL);
    // TODO: use real tenant
    String skJWT = null;
    try {skJWT = tokClient.getSvcToken(tenant, SERVICE_NAME_SYSTEMS);}
    catch (Exception e) {throw new TapisException("Exception from Tokens service", e);}
    System.out.println("Got skJWT: " + skJWT);
    _log.error("Got skJWT: " + skJWT);
    // Basic check of JWT
    if (StringUtils.isBlank(skJWT)) throw new TapisException("Token service returned invalid JWT");

    // TODO/TBD: Build perm specs here? review details
    String sysPerm = "system:" + tenant + ":*:" + name;
    String storePerm = "store:" + tenant + ":*:" + name + ":*";

    var skClient = new SKClient(skBaseURL, skJWT);
    // Create Role with perms and grant it to user
    // TODO/TBD: name of system owner role, one for each "tenant+system"?
    String roleName = SYSTEM_OWNER_ROLE + "_" + name;
    try
    {
      skClient.createRole(roleName, "System owner role");
      skClient.addRolePermission(roleName, sysPerm);
      skClient.addRolePermission(roleName, storePerm);
      skClient.grantUserRole(owner, roleName);
    }
    catch (Exception e) { _log.error(e.toString()); throw e;}

    // TODO remove tests
    // Test by retrieving role and permissions from SK
    SkRole skRole = null;
    try { skRole = skClient.getRoleByName(roleName); }
    catch (Exception e) { _log.error(e.toString()); throw e;}
    _log.error("Created and then found SKRole with name: " + skRole.getName() + " Id: " + skRole.getId());
    System.out.println("Created and then found SKRole with name: " + skRole.getName() + " Id: " + skRole.getId());
    // Test retrieving users with the role
    ResultNameArray nameArray = skClient.getUsersWithRole(roleName);
    List<String> names = nameArray.getNames();
    if (names != null && names.contains(owner))
    {
      _log.error("User " + owner + " does have role " + skRole.getName());
      System.out.println("User " + owner + " does have role " + skRole.getName());
    } else {
      _log.error("User " + owner + " does NOT have role " + skRole.getName());
      System.out.println("User " + owner + " does NOT have role " + skRole.getName());
    }
    // Test retrieving all perms for a user
    ResultNameArray permArray = skClient.getUserPerms(owner);
    List<String> perms = permArray.getNames();
    _log.error("User " + owner + " has the following permissions: ");
    System.out.println("User " + owner + " has the following permissions: ");
    for (String perm : perms) {
      _log.error("  perm: " + perm);
      System.out.println("  perm: " + perm);
    }
    return itemId;
  }

  /**
   * Delete a system record given the system name.
   *
   */
  @Override
  public int deleteSystemByName(String tenant, String name) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();
    return dao.deleteTSystem(tenant, name);
  }

  /**
   * getSystemByName
   * @param name
   * @return
   * @throws TapisException
   */
  @Override
  public TSystem getSystemByName(String tenant, String name, boolean getCreds) throws TapisException {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();
    TSystem result = dao.getTSystemByName(tenant, name);
    return result;
  }

  /**
   * Get all systems
   * @param tenant
   * @return
   * @throws TapisException
   */
  @Override
  public List<TSystem> getSystems(String tenant) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();
    return dao.getTSystems(tenant);
  }

  /**
   * Get list of system names
   * @param tenant
   * @return
   * @throws TapisException
   */
  @Override
  public List<String> getSystemNames(String tenant) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();
    return dao.getTSystemNames(tenant);
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

}
