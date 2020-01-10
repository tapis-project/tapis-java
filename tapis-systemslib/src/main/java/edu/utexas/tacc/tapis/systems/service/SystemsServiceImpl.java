package edu.utexas.tacc.tapis.systems.service;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TENANT_VAR;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all
 *   top level service operations.
 */
@Singleton
public class SystemsServiceImpl implements SystemsService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  // TODO: thread safety
  Map<String, SKClient> skClientMap = new HashMap<>();

  // TODO *** Inject Dao singletons ***
//  @com.google.inject.Inject
  private SystemsDao dao;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************
  /**
   * Create a new system object
   *
   * @return Sequence id of object created
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public int createSystem(String tenantName, String apiUserId, String systemName, String description, String systemType,
                          String owner, String host, boolean available, String effectiveUserId, String accessMethod,
                          char[] accessCredential, String bucketName, String rootDir, String transferMethods,
                          int port, boolean useProxy, String proxyHost, int proxyPort,
                          boolean jobCanExec, String jobLocalWorkingDir, String jobLocalArchiveDir,
                          String jobRemoteArchiveSystem, String jobRemoteArchiveDir, String jobCapabilities,
                          String tags, String notes, String rawJson)
          throws TapisException, IllegalStateException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();

    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with apiUserId
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) owner = apiUserId;

    // TODO/TBD do this check here? it is already being done in systemsapi front-end. If we are going to support
    //   other front-ends over which we have less control then a lot more checking needs to be done here as well.
    // Check for valid effectiveUserId
    // For CERT access the effectiveUserId cannot be static string other than owner
//    if (accessMechanism != null && accessMechanism.equals(Protocol.AccessMethod.CERT) &&
//        !effectiveUserId.equals(owner) &&
//        !effectiveUserId.equals(APIUSERID_VAR) &&
//        !effectiveUserId.equals(OWNER_VAR))
//    {
//
//    }

    // Perform variable substitutions that happen at create time: bucketName, rootDir, jobInputDir, jobOutputDir, workDir, scratchDir
    // NOTE: effectiveUserId is not processed. Var reference is retained and substitution done as needed when system is retrieved.
    //    ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
    String[] allVarSubstitutions = {apiUserId, owner, tenantName};
    bucketName = StringUtils.replaceEach(bucketName, ALL_VARS, allVarSubstitutions);
    rootDir = StringUtils.replaceEach(rootDir, ALL_VARS, allVarSubstitutions);
    jobLocalWorkingDir = StringUtils.replaceEach(jobLocalWorkingDir, ALL_VARS, allVarSubstitutions);
    jobLocalArchiveDir = StringUtils.replaceEach(jobLocalArchiveDir, ALL_VARS, allVarSubstitutions);
    jobRemoteArchiveDir = StringUtils.replaceEach(jobRemoteArchiveDir, ALL_VARS, allVarSubstitutions);

    int itemId = dao.createTSystem(tenantName, systemName, description, systemType, owner, host, available,
                                   effectiveUserId, accessMethod, bucketName, rootDir, transferMethods,
                                   port, useProxy, proxyHost, proxyPort,
                                   jobCanExec, jobLocalWorkingDir, jobLocalArchiveDir, jobRemoteArchiveSystem,
                                   jobRemoteArchiveDir, jobCapabilities,
                                   tags, notes, rawJson);

    // TODO: Remove debug System.out statements

    // TODO/TBD: Creation of system and role/perms not in single transaction. Need to handle failure of role/perms operations

    // TODO Store credentials in Security Kernel

    // TODO Once credentials stored overwrite characters in memory

    // Get the Security Kernel client
    var skClient = getSKClient(tenantName);

    // TODO/TBD: Build perm specs here? review details
    String sysPerm = "system:" + tenantName + ":*:" + systemName;

    // Give owner and possibly effectiveUser access to the system
    // SK creates a default role for the user
    try
    {
      skClient.grantUserPermission(owner, sysPerm);
      // Grant same perms for effectiveUser unless effUser == apiUser or owner
      if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR))
      {
        skClient.grantUserPermission(effectiveUserId, sysPerm);
      }
    }
    // TODO exception handling, but consider how data integrity will be handled for distributed data
    catch (Exception e) { _log.error(e.toString()); throw e;}

    // TODO *************** remove tests ********************
    printPermInfoForUser(skClient, owner);

    return itemId;
  }

  /**
   * Delete a system record given the system name.
   * Also remove permissions and credentials from the Security Kernel
   *
   */
  @Override
  public int deleteSystemByName(String tenantName, String systemName) throws TapisException
  {
    // TODO: Remove all credentials associated with the system

    // TODO: See if it makes sense to have a SK method to do this in one operation
    // Remove all permissions associated with the system
    // Build list of all perms
    var userPerms = new ArrayList<String>();
    for (TSystem.Permissions perm : TSystem.Permissions.values()) {
      userPerms.add(perm.name());
    }
    // Use Security Kernel client to find all users with perms associated with the system.
    // Get the Security Kernel client
    var skClient = getSKClient(tenantName);
    String permSpec = "system:" + tenantName + ":%:" + systemName;
    var userNames = skClient.getUsersWithPermission(permSpec).getNames();
    // Revoke all perms for all users
    for (String userName : userNames) {
      revokeUserPermissions(tenantName, systemName, userName, userPerms);
      // TODO/TBD: It is currently needed but how to make sure all perms for a system are removed?
      // Remove the "*" permission
      permSpec = "system:" + tenantName + ":*:" + systemName;
      skClient.revokeUserPermission(userName, permSpec);
      // TODO *************** remove debug output ********************
      printPermInfoForUser(skClient, userName);
    }

    // Delete the system
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    return dao.deleteTSystem(tenantName, systemName);
  }

  /**
   * getSystemByName
   * @param systemName - Name of the system
   * @return true if system exists, false if system does not exist
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSystemByName(String tenant, String systemName) throws TapisException {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    boolean result = dao.checkForTSystemByName(tenant, systemName);
    return result;
  }

  /**
   * getSystemByName
   * @param systemName - Name of the system
   * @return TSystem
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public TSystem getSystemByName(String tenant, String systemName, String apiUserId, boolean getCreds) throws TapisException {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    TSystem result = dao.getTSystemByName(tenant, systemName);
    if (result == null) return result;
    // Resolve effectiveUserId if necessary
    result.setEffectiveUserId(resolveEffectiveUserId(result.getEffectiveUserId(), result.getOwner(), apiUserId));
    // TODO If requested retrieve credentials from Security Kernel
    // TODO Once local credentials var not needed overwrite characters in memory
    //result.setAccessCredential();
    return result;
  }

  /**
   * Get all systems
   * @param tenant - Tenant name
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(String tenant, String apiUserId) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    List<TSystem> result = dao.getTSystems(tenant);
    for (TSystem sys : result)
    {
      sys.setEffectiveUserId(resolveEffectiveUserId(sys.getEffectiveUserId(), sys.getOwner(), apiUserId));
      // TODO If requested retrieve credentials from Security Kernel
      // TODO Once local credentials var not needed overwrite characters in memory
      //sys.setAccessCredential();
    }
    return result;
  }

  /**
   * Get list of system names
   * @param tenant - Tenant name
   * @return
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<String> getSystemNames(String tenant) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    return dao.getTSystemNames(tenant);
  }

  /**
   * Get system owner
   * @param tenant - Tenant name
   * @param systemName - Name of the system
   * @return
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public String getSystemOwner(String tenant, String systemName) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    dao = new SystemsDaoImpl();
    return dao.getTSystemOwner(tenant, systemName);
  }

  /**
   * Grant permissions to a user for a system
   * NOTE: This only impacts the default user role
   * Add each perm separately. This makes them easier to remove from the role.
   * If any of the perms are the wildcard then all perms are given
   * NOTE: If wildcard ("*") is given we still add perms individually so we
   *       can more easily remove them in revokeUserPermissions().
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void grantUserPermissions(String tenantName, String systemName, String userName, List<String> permissions)
    throws TapisException
  {
    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) ||
        permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(tenantName, systemName, permissions);

    // Get the Security Kernel client
    var skClient = getSKClient(tenantName);

    // Assign perms to user. SK creates a default role for the user
    try
    {
      for (String permSpec : permSpecSet)
      {
        skClient.grantUserPermission(userName, permSpec);
      }
    }
    // TODO exception handling
    catch (Exception e) { _log.error(e.toString()); throw e;}

    // TODO *************** remove tests ********************
    printPermInfoForUser(skClient, userName);
  }

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   *
   * @return List of permissions
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<String> getUserPermissions(String tenantName, String systemName, String userName) throws TapisException
  {
    var userPerms = new ArrayList<String>();
    // Use Security Kernel client to check for each permission in the enum list
    var skClient = getSKClient(tenantName);
    for (TSystem.Permissions perm : TSystem.Permissions.values())
    {
      String permSpec = "system:" + tenantName + ":" + perm.name() + ":" + systemName;
      try
      {
        Boolean isAuthorized = skClient.isPermitted(userName, permSpec).getIsAuthorized();
        if (Boolean.TRUE.equals(isAuthorized)) userPerms.add(perm.name());
      }
      // TODO exception handling
      catch (Exception e) { _log.error(e.toString()); throw e;}
    }
    return userPerms;
  }

  /**
   * Revoke permissions from a user for a system
   * NOTE: This only impacts the default user role
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void revokeUserPermissions(String tenantName, String systemName, String userName, List<String> permissions)
    throws TapisException
  {
    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) ||
      permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(tenantName, systemName, permissions);

    // Get the Security Kernel client
    var skClient = getSKClient(tenantName);

    // Remove perms from default user role
    try
    {
      for (String permSpec : permSpecSet)
      {
        skClient.revokeUserPermission(userName, permSpec);
      }
    }
    // TODO exception handling
    catch (Exception e) { _log.error(e.toString()); throw e;}

    // TODO *************** remove tests ********************
    printPermInfoForUser(skClient, userName);
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /**
   * If effectiveUserId is dynamic then resolve it
   * @param userId - effectiveUserId string, static or dynamic
   * @return Resolved value for effective user.
   */
  private static String resolveEffectiveUserId(String userId, String owner, String apiUserId)
  {
    if (StringUtils.isBlank(userId)) return userId;
    else if (userId.equals(OWNER_VAR) && !StringUtils.isBlank(owner)) return owner;
    else if (userId.equals(APIUSERID_VAR) && !StringUtils.isBlank(apiUserId)) return apiUserId;
    else return userId;
  }

  /**
   * Get Security Kernel client associated with specified tenant
   * TODO: thread safety?
   * TODO: or worst case it creates some clients a few extra times.
   * Cache the SK clients in a map by tenant name.
   * @param tenantName
   * @return
   * @throws TapisException
   */
  private SKClient getSKClient(String tenantName) throws TapisException
  {
    var skClient = skClientMap.get(tenantName);
    if (skClient != null) return skClient;
    // TBD/TODO: Determine if all this lookup is needed. If yes put it in private method or utility method
    // Use Tenants service to lookup information we need to:
    //  Access the tokens service associated with the tenant.
    //  Access the security kernel service associated with the tenant.
    // NOTE: The front-end is responsible for validating the JWT using the public key for the tenant.
    //       See edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter

    // Tenants and tokens service URLs from the environment have precedence.
    // NOTE: Tenants URL is a required parameter, so no need to check here
    RuntimeParameters parms = RuntimeParameters.getInstance();

//    String tenantsURL = "https://dev.develop.tapis.io";
    String tenantsURL = parms.getTenantsSvcURL();
    var tenantsClient = new TenantsClient(tenantsURL);
    Tenant tenant1 = null;
    try {tenant1 = tenantsClient.getTenant(tenantName);}
    catch (Exception e) {throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TENANTS_ERROR", tenantName, e.getMessage()), e);}
    if (tenant1 == null) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TENANTS_NULL", tenantName));

    // Tokens service URL comes from env or the tenants service
//    String tokensURL = "https://dev.develop.tapis.io";
    String tokensURL = parms.getTokensSvcURL();
    if (StringUtils.isBlank(tokensURL)) tokensURL = tenant1.getTokenService();
    if (StringUtils.isBlank(tokensURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_URL_ERROR", tenantName));

    // Get short term service JWT from tokens service
    var tokClient = new TokensClient(tokensURL);
    String svcJWT = null;
    try {svcJWT = tokClient.getSvcToken(tenantName, SERVICE_NAME_SYSTEMS);}
    catch (Exception e) {throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_ERROR", tenantName, e.getMessage()), e);}
    // Basic check of JWT
    if (StringUtils.isBlank(svcJWT)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_JWT_ERROR", tenantName));

    // Get Security Kernel URL from the env or the tenants service. Env value has precedence
//    String skURL = "https://dev.develop.tapis.io/v3";
    String skURL = parms.getSkSvcURL();
    if (StringUtils.isBlank(skURL)) skURL = tenant1.getSecurityKernel();
    if (StringUtils.isBlank(skURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_SK_URL_ERROR", tenantName));
    // TODO remove strip-off of everything after /v3 once tenant is updated or we do something different for base URL in auto-generated clients
    // Strip off everything after the /v3 so we have a valid SK base URL
    skURL = skURL.substring(0, skURL.indexOf("/v3") + 3);

    skClient = new SKClient(skURL, svcJWT);
    skClientMap.put(tenantName, skClient);
    return skClient;
  }

  /**
   * Create a set of individual permSpec entries based on the list passed in
   * For wildcard add all perms in the enum set
   * @param permList
   * @return
   */
  private static Set<String> getPermSpecSet(String tenantName, String systemName, List<String> permList)
  {
    var permSet = new HashSet<String>();
    for (String permStr : permList)
    {
      if (StringUtils.trimToEmpty(permStr).equals(TSystem.PERMS_WILDCARD))
      {
        for (TSystem.Permissions perm : TSystem.Permissions.values())
        {
          String permSpec = "system:" + tenantName + ":" + perm.name() + ":" + systemName;
          permSet.add(permSpec);
        }
      }
      else
      {
        // TODO/TBD: should we check that the perm matches one in the enum, possibly trimming and ignoring case
        // TODO/TBD: JSON validation at front-end can handle the check
        String permSpec = "system:" + tenantName + ":" + permStr.toUpperCase() + ":" + systemName;
        permSet.add(permSpec);
      }
    }
    return permSet;
  }

  // TODO *************** remove debug output ********************
  private static void printPermInfoForUser(SKClient skClient, String userName)
  {
    if (skClient == null || userName == null) return;
    try {
      ResultNameArray nameArray = null;
      // Test retrieving all roles for a user
      ResultNameArray roleArray = skClient.getUserRoles(userName);
      List<String> roles = roleArray.getNames();
      _log.error("User " + userName + " has the following roles: ");
      for (String role : roles) { _log.error("  role: " + role); }
      // Test retrieving all perms for a user
      ResultNameArray permArray = skClient.getUserPerms(userName);
      List<String> perms = permArray.getNames();
      _log.error("User " + userName + " has the following permissions: ");
      for (String perm : perms) { _log.error("  perm: " + perm); }
    } catch (Exception e) { _log.error(e.toString()); }
  }
  // TODO *************** remove tests ********************
}
