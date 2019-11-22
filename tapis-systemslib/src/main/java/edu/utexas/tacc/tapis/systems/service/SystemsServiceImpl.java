package edu.utexas.tacc.tapis.systems.service;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
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
  private static final String APIUSERID_VAR = "${apiUserId}";
  private static final String OWNER_VAR = "${owner}";
  private static final String TENANT_VAR = "${tenant}";
  private static final String EFFUSERID_VAR = "${effectiveUserId}";

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
  public int createSystem(String tenant, String apiUserId, String name, String description, String owner, String host,
                          boolean available, String bucketName, String rootDir, String jobInputDir,
                          String jobOutputDir, String workDir, String scratchDir, String effectiveUserId, String tags,
                          String notes, String accessCredential, String accessMechanism, String transferMechanisms,
                          int protocolPort, boolean protocolUseProxy, String protocolProxyHost, int protocolProxyPort,
                          String rawRequest)
          throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();

    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with apiUserId
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) owner = apiUserId;

    // Perform variable substitutions for bucketName, rootDir, jobInputDir, jobOutputDir, workDir, scratchDir
    // NOTE: effectiveUserId is not processed. Var reference is retained and substitution done as needed.
    // TODO

    int itemId = dao.createTSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                   jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId, tags, notes,
                                   accessMechanism, transferMechanisms, protocolPort, protocolUseProxy,
                                   protocolProxyHost, protocolProxyPort, rawRequest);

    // TODO: Remove debug System.out statements

    // TODO/TBD: Creation of system and role/perms not in single transaction. Need to handle failure of role/perms operations

    // TODO Store credentials in Security Kernel

    // Get the tenant and token base URLs from the environment
    RuntimeParameters parms = RuntimeParameters.getInstance();
    // TODO Do real service location lookup, through tenants service?
//    String tokensBaseURL = "https://dev.develop.tapis.io";
    String tokensBaseURL = parms.getTokensSvcURL();
    // Get short term service JWT from tokens service
    var tokClient = new TokensClient(tokensBaseURL);
    String svcJWT = null;
    // TODO proper exception handling
    try {svcJWT = tokClient.getSvcToken(tenant, SERVICE_NAME_SYSTEMS);}
    catch (Exception e) {throw new TapisException("Exception from Tokens service", e);}
    System.out.println("Got svcJWT: " + svcJWT);
    _log.error("Got svcJWT: " + svcJWT);
    // Basic check of JWT
    if (StringUtils.isBlank(svcJWT)) throw new TapisException("Token service returned invalid JWT");


    // Get Security Kernel URL from the env or the tenants service
    // Env value has precedence
//    String skBaseURL = "https://dev.develop.tapis.io/v3";
    String skBaseURL = parms.getSkSvcURL();
    if (StringUtils.isBlank(skBaseURL))
    {
      // Lookup tenant info from tenants service
//    String tenantsBaseURL = "https://dev.develop.tapis.io";
      String tenantsBaseURL = parms.getTenantsSvcURL();
      var tenantsClient = new TenantsClient(tenantsBaseURL);
      // TODO proper exception handling
      try {skBaseURL = tenantsClient.getSKBasePath(tenant);}
      catch (Exception e) {throw new TapisException("Exception from Tenants service", e);}
      // TODO remove strip-off of everything after /v3 once tenant is updated
      // Strip off everything after the /v3 so we have a valid SK base URL
      skBaseURL = skBaseURL.substring(0, skBaseURL.indexOf("/v3") + 3);
      // TODO: remove. There is currently a typo in the sk url for tenant dev. hard code for now
      skBaseURL = "https://dev.develop.tapis.io/v3";
    }


    var skClient = new SKClient(skBaseURL, svcJWT);
    // TODO/TBD: Build perm specs here? review details
    String sysPerm = "system:" + tenant + ":*:" + name;
    String storePerm = "store:" + tenant + ":*:" + name + ":*";

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
  public boolean checkForSystemByName(String tenant, String name) throws TapisException {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    var dao = new SystemsDaoImpl();
    boolean result = dao.checkForTSystemByName(tenant, name);
    return result;
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
