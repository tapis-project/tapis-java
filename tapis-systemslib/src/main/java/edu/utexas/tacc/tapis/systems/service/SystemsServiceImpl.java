package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.model.Credential.*;
import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TENANT_VAR;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 */
public class SystemsServiceImpl implements SystemsService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};

  private static final List<String> ALL_PERMS = new ArrayList<>(List.of("*"));
  private static final String PERM_SPEC_PREFIX = "system:";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  Map<String, ServiceJWT> svcJWTMap = new HashMap<>();

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;

  @Inject
  private SKClient skClient;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  // -----------------------------------------------------------------------
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------

  /**
   * Create a new system object
   *
   * @return Sequence id of object created
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - if system already exists or TSystem is in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int createSystem(String tenantName, String apiUserId, TSystem system, String scrubbedJson)
          throws TapisException, IllegalStateException, IllegalArgumentException
  {

    // Extract system name for convenience
    String systemName = (system == null ? null : system.getName());

    // ---------------------------- Check inputs ------------------------------------
    // Required system attributes: name, type, host, defaultAccessMethod
    if (system == null || StringUtils.isBlank(tenantName) || StringUtils.isBlank(apiUserId) || StringUtils.isBlank(systemName) ||
        system.getSystemType() == null || StringUtils.isBlank(system.getHost()) ||
        system.getDefaultAccessMethod() == null || StringUtils.isBlank(apiUserId) || StringUtils.isBlank(scrubbedJson))
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_CREATE_ARG_ERROR", tenantName, apiUserId, systemName));
    }

    // ---------------- Fill in defaults and check constraints on TSystem attributes ------------------------
    system.setTenant(tenantName);
    validateTSystem(system);

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    system = resolveVariables(system, apiUserId);

    // ------------------- Make Dao call to persist the system -----------------------------------
    int itemId = dao.createTSystem(system, scrubbedJson);

    // TODO/TBD: Creation of system and role/perms/creds not in single transaction. Need to handle failure of role/perms/creds operations
    // TODO possibly have a try/catch/finally to roll back any writes in case of failure.

    // ------------------- Add permissions -----------------------------------
    // Give owner and possibly effectiveUser access to the system
    String effectiveUserId = system.getEffectiveUserId();
    grantUserPermissions(tenantName, apiUserId, systemName, system.getOwner(), ALL_PERMS);
    if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR))
    {
      grantUserPermissions(tenantName, apiUserId, systemName, effectiveUserId, ALL_PERMS);
    }
    // TODO/TBD: remove addition of files related permSpec
    // Give owner/effectiveUser files service related permission for root directory
    var skClient = getSKClient(tenantName, apiUserId);
    String permSpec = "files:" + tenantName + ":*:" +  systemName;
    skClient.grantUserPermission(system.getOwner(), permSpec);
    if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR)) skClient.grantUserPermission(effectiveUserId, permSpec);

    // ------------------- Store credentials -----------------------------------
    // Store credentials in Security Kernel if cred provided and effectiveUser is static
    Credential credential = system.getAccessCredential();
    if (credential != null && !effectiveUserId.equals(APIUSERID_VAR))
    {
      String accessUser = effectiveUserId;
      // If effectiveUser is owner resolve to static string.
      if (effectiveUserId.equals(OWNER_VAR)) accessUser = system.getOwner();
      createUserCredential(tenantName, apiUserId, systemName, accessUser, credential);
    }

    return itemId;
  }

  /**
   * Delete a system record given the system name.
   * Also remove permissions and credentials from the Security Kernel
   *
   * @param tenantName - name of tenant
   * @param systemName - name of system
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int deleteSystemByName(String tenantName, String apiUserId, String systemName) throws TapisException
  {
    var skClient = getSKClient(tenantName, apiUserId);
    // TODO: Remove all credentials associated with the system.
    // TODO: Have SK do this in one operation?
//    deleteUserCredential(tenantName, systemName, );
    // Construct basic SK secret parameters
//    var sParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysOwner(accessUser);
//    skClient.destroySecretMeta(sParms);

    // TODO: See if it makes sense to have a SK method to do this in one operation
    // Use Security Kernel client to find all users with perms associated with the system.
    // Get the Security Kernel client
    String permSpec = PERM_SPEC_PREFIX + tenantName + ":%:" + systemName;
    var userNames = skClient.getUsersWithPermission(permSpec);
    // Revoke all perms for all users
    for (String userName : userNames) {
      revokeUserPermissions(tenantName, apiUserId, systemName, userName, ALL_PERMS);
      // TODO/TBD: How to make sure all perms for a system are removed?
      // TODO *************** remove debug output ********************
      printPermInfoForUser(skClient, userName);
    }

    // Delete the system
    return dao.deleteTSystem(tenantName, systemName);
  }

  /**
   * getSystemByName
   * @param tenantName - name of tenant
   * @param systemName - Name of the system
   * @return true if system exists, false if system does not exist
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSystemByName(String tenantName, String apiUserId, String systemName) throws TapisException {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    boolean result = dao.checkForTSystemByName(tenantName, systemName);
    return result;
  }

  /**
   * getSystemByName
   * @param tenantName - name of tenant
   * @param systemName - Name of the system
   * @return TSystem - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public TSystem getSystemByName(String tenantName, String apiUserId, String systemName, boolean getCreds, AccessMethod accMethod1) throws TapisException {
    TSystem result = dao.getTSystemByName(tenantName, systemName);
    if (result == null) return null;
    // Resolve effectiveUserId if necessary
    String effectiveUserId = resolveEffectiveUserId(result.getEffectiveUserId(), result.getOwner(), apiUserId);
    // Update result with effectiveUserId
    result.setEffectiveUserId(effectiveUserId);
    // If requested retrieve credentials from Security Kernel
    if (getCreds)
    {
      AccessMethod accMethod = result.getDefaultAccessMethod();
      // If accessMethod specified then use it instead of default access method defined for the system.
      if (accMethod1 != null) accMethod = accMethod1;
      Credential cred = getUserCredential(tenantName, apiUserId, systemName, effectiveUserId, accMethod);
      result.setAccessCredential(cred);
    }
    return result;
  }

  /**
   * Get all systems
   * @param tenantName - Tenant name
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(String tenantName, String apiUserId) throws TapisException
  {
    List<TSystem> result = dao.getTSystems(tenantName);
    for (TSystem sys : result)
    {
      sys.setEffectiveUserId(resolveEffectiveUserId(sys.getEffectiveUserId(), sys.getOwner(), apiUserId));
    }
    return result;
  }

  /**
   * Get list of system names
   * @param tenantName - Tenant name
   * @return - list of systems
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<String> getSystemNames(String tenantName, String apiUserId) throws TapisException
  {
    return dao.getTSystemNames(tenantName);
  }

  /**
   * Get system owner
   * @param tenantName - Tenant name
   * @param systemName - Name of the system
   * @return - Owner or null if system not found
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public String getSystemOwner(String tenantName, String apiUserId, String systemName) throws TapisException
  {
    return dao.getTSystemOwner(tenantName, systemName);
  }

  // -----------------------------------------------------------------------
  // --------------------------- Permissions -------------------------------
  // -----------------------------------------------------------------------

  /**
   * Grant permissions to a user for a system
   * NOTE: This only impacts the default user role
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void grantUserPermissions(String tenantName, String apiUserId, String systemName, String userName, List<String> permissions)
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
    var skClient = getSKClient(tenantName, apiUserId);

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
    // TODO remove code
//    printPermInfoForUser(skClient, userName);
  }

  /**
   * Revoke permissions from a user for a system
   * NOTE: This only impacts the default user role
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void revokeUserPermissions(String tenantName, String apiUserId, String systemName, String userName, List<String> permissions)
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
    var skClient = getSKClient(tenantName, apiUserId);

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

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   *
   * @return List of permissions
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<String> getUserPermissions(String tenantName, String apiUserId, String systemName, String userName) throws TapisException
  {
    var userPerms = new ArrayList<String>();
    // Use Security Kernel client to check for each permission in the enum list
    var skClient = getSKClient(tenantName, apiUserId);
    for (TSystem.Permission perm : TSystem.Permission.values())
    {
      String permSpec = PERM_SPEC_PREFIX + tenantName + ":" + perm.name() + ":" + systemName;
      try
      {
        Boolean isAuthorized = skClient.isPermitted(userName, permSpec);
        if (Boolean.TRUE.equals(isAuthorized)) userPerms.add(perm.name());
      }
      // TODO exception handling
      catch (Exception e) { _log.error(e.toString()); throw e;}
    }
    return userPerms;
  }

  // -----------------------------------------------------------------------
  // ---------------------------- Credentials ------------------------------
  // -----------------------------------------------------------------------

  /**
   * Store or update credential for given system and user.
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void createUserCredential(String tenantName, String apiUserId, String systemName, String userName, Credential credential)
          throws TapisException
  {
    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) || credential == null)
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }
    // Get the Security Kernel client
    var skClient = getSKClient(tenantName, apiUserId);
    try {
      // Construct basic SK secret parameters
      var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysUser(userName);
      Map<String, String> dataMap;
      // Check for each secret type and write values if they are present
      // Not that multiple secrets may be present.
      // Store password if present
      if (!StringUtils.isBlank(credential.getPassword())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.password);
        dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
        sParms.setData(dataMap);
        skClient.writeSecret(sParms);
      }
      // Store PKI keys if both present
      if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.sshkey);
        dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
        dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
        sParms.setData(dataMap);
        skClient.writeSecret(sParms);
      }
      // Store Access key and secret if both present
      if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.accesskey);
        dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
        dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
        sParms.setData(dataMap);
        skClient.writeSecret(sParms);
      }
      // TODO what about ssh certificate? Nothing to do here?
    }
    // TODO exception handling
    catch (Exception e) { _log.error(e.toString()); throw e;}
  }

  /**
   * Delete credential for given system and user
   *
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void deleteUserCredential(String tenantName, String apiUserId, String systemName, String userName)
          throws TapisException
  {
    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName))
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }
    // Get the Security Kernel client
    var skClient = getSKClient(tenantName, apiUserId);
    // Construct basic SK secret parameters
    var sParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysUser(userName);
    try
    {
      sParms.setKeyType(KeyType.password);
      skClient.destroySecretMeta(sParms);
      sParms.setKeyType(KeyType.sshkey);
      skClient.destroySecretMeta(sParms);
      sParms.setKeyType(KeyType.accesskey);
      skClient.destroySecretMeta(sParms);
      sParms.setKeyType(KeyType.cert);
      skClient.destroySecretMeta(sParms);
    }
    // TODO exception handling
    // TODO/TBD: If tapis client exception then log warning but continue
    // TODO/TBD: for other exectpions log error and re-throw the exception
    catch (TapisClientException tce) { _log.warn(tce.toString()); }
    catch (Exception e) { _log.error(e.toString()); throw e;}
  }

  /**
   * Get credential for given system, user and access method
   * @return Credential - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public Credential getUserCredential(String tenantName, String apiUserId, String systemName, String userName, AccessMethod accessMethod) throws TapisException
  {
    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName))
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }
    // If system does not exist throw an exception
    if (!checkForSystemByName(tenantName, apiUserId, systemName)) throw new TapisException(LibUtils.getMsg("SYSLIB_NOT_FOUND", systemName));

    // If accessMethod not passed in fill in with default from system
    if (accessMethod == null)
    {
      TSystem sys = dao.getTSystemByName(tenantName, systemName);
      if (sys == null)  throw new TapisException(LibUtils.getMsg("SYSLIB_NOT_FOUND", systemName));
      accessMethod = sys.getDefaultAccessMethod();
    }


    Credential credential = null;
    try
    {
      // Get the Security Kernel client
      var skClient = getSKClient(tenantName, apiUserId);
      // Construct basic SK secret parameters
      var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysUser(userName);
      // Set key type based on access method
      if (accessMethod.equals(AccessMethod.PASSWORD))sParms.setKeyType(KeyType.password);
      else if (accessMethod.equals(AccessMethod.PKI_KEYS))sParms.setKeyType(KeyType.sshkey);
      else if (accessMethod.equals(AccessMethod.ACCESS_KEY))sParms.setKeyType(KeyType.accesskey);
      else if (accessMethod.equals(AccessMethod.CERT))sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      SkSecret skSecret = skClient.readSecret(sParms);
      if (skSecret == null) return null;
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) return null;

      // Create a credential
      credential = new Credential(dataMap.get(SK_KEY_PASSWORD),
              dataMap.get(SK_KEY_PRIVATE_KEY),
              dataMap.get(SK_KEY_PUBLIC_KEY),
              null, //dataMap.get(CERT) TODO: how to get ssh certificate
              dataMap.get(SK_KEY_ACCESS_KEY),
              dataMap.get(SK_KEY_ACCESS_SECRET));
    }
    // TODO exception handling
    // TODO/TBD: If tapis client exception then log error but continue so null is returned.
    // TODO/TBD: for other exectpions log error and re-throw the exception
    catch (TapisClientException tce) { _log.error(tce.toString()); }
    catch (Exception e) { _log.error(e.toString()); throw e;}

    return credential;
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /**
   * Get Security Kernel client associated with specified tenant
   * @param tenantName - name of tenant
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient(String tenantName, String apiUserId) throws TapisException
  {
    Tenant tenant = TenantManager.getInstance().getTenant(tenantName);
    // TODO Put back in use of ServiceJWT when working.
    // Check ServiceJWT cache, if not there create one.
//    var svcJWT = svcJWTMap.get(tenantName);
//    if (svcJWT == null)
//    {
//      var svcJWTParms = new ServiceJWTParms();
//      svcJWTParms.setServiceName(SERVICE_NAME_SYSTEMS);
//      svcJWTParms.setTenant(tenantName);
//      svcJWTParms.setTokensBaseUrl(tenant.getTokenService());
//      svcJWT = new ServiceJWT(svcJWTParms, "fakeServicePassword");
//      svcJWTMap.put(tenantName, svcJWT);
//    }

    // TODO remove this in favor of ServiceJWT when working.
    // Get short term service JWT from tokens service
    // Tokens service URL comes from env or the tenants service
    String tokensURL = RuntimeParameters.getInstance().getTokensSvcURL();
    if (StringUtils.isBlank(tokensURL)) tokensURL = tenant.getTokenService();
    if (StringUtils.isBlank(tokensURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_URL_ERROR", tenantName));
    var tokClient = new TokensClient(tokensURL);
    String svcJWTStr;
    try {svcJWTStr = tokClient.getSvcToken(tenantName, SERVICE_NAME_SYSTEMS);}
    catch (Exception e) {throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_ERROR", tenantName, e.getMessage()), e);}
    // Basic check of JWT
    if (StringUtils.isBlank(svcJWTStr)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_JWT_ERROR", tenantName));



    // Update SKClient on the fly. If this becomes a bottleneck we can add a cache.
    // Get Security Kernel URL from the env or the tenants service. Env value has precedence.
    //    String skURL = "https://dev.develop.tapis.io/v3";
    String skURL = RuntimeParameters.getInstance().getSkSvcURL();
    if (StringUtils.isBlank(skURL)) skURL = tenant.getSecurityKernel();
    if (StringUtils.isBlank(skURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_SK_URL_ERROR", tenantName));
    // TODO remove strip-off of everything after /v3 once tenant is updated or we do something different for base URL in auto-generated clients
    // Strip off everything after the /v3 so we have a valid SK base URL
    skURL = skURL.substring(0, skURL.indexOf("/v3") + 3);
    skClient.setBasePath(skURL);
//    skClient.addDefaultHeader("X-Tapis-Token", svcJWT.getAccessJWT());
    skClient.addDefaultHeader("X-Tapis-Token", svcJWTStr);
    skClient.addDefaultHeader("X-Tapis-User", apiUserId);
    skClient.addDefaultHeader("X-Tapis-Tenant", tenantName);
    return skClient;

//    // Use TenantManager to get tenant info. Needed for SK base URL.
//    // TenantManager initialized in front end api class SystemsApplication
//    Tenant tenant1 = TenantManager.getInstance().getTenant(tenantName);
//
//    String skURL = RuntimeParameters.getInstance().getSkSvcURL();
//    if (StringUtils.isBlank(skURL)) skURL = tenant1.getSecurityKernel();
//    if (StringUtils.isBlank(skURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_SK_URL_ERROR", tenantName));
//    // TODO remove strip-off of everything after /v3 once tenant is updated or we do something different for base URL in auto-generated clients
//    // Strip off everything after the /v3 so we have a valid SK base URL
//    skURL = skURL.substring(0, skURL.indexOf("/v3") + 3);
//
//    // TODO replace all service JWT related code with ServiceJWT
//    // TODO Initialize this at runtime
//// TODO ServiceJWT throwing error trying to generate jwt.
//    //TODO  difference between working code below and serviceJWT is working code is only setting
//    //  accountType, service name and tenant
//    //  serviceJWT is also setting
//    //      accessTokenTtl, generateRefreshToken=true and refreshTokeTtl values
//    //      and this appears to cause tokens svc to return an error (Internal Server Error)
//
//
//    // Create and configure the SKClient
//    SKClient skClient = new SKClient(skURL, svcJWTStr);
////    SKClient skClient = new SKClient(skURL, serviceJWT.getAccessJWT());
//
//    // Service to Service calls require user header, set it to be the same as the service name
//    // TODO Get string constants from shared code when available
//    String TAPIS_USER_HEADER = "X-Tapis-User";
//    String TAPIS_TENANT_HEADER = "X-Tapis-Tenant";
//    skClient.addDefaultHeader(TAPIS_USER_HEADER, SERVICE_NAME_SYSTEMS);
//    skClient.addDefaultHeader(TAPIS_TENANT_HEADER, tenantName);
////    skClientMap.put(tenantName, skClient);
//    return skClient;
//
//// TODO If need to cache, consider creating a:
////    Class providing everything needed for managing an SKClient for a specific tenant.
////          Contains an SKClient and ServiceJWT.
////    Provides logic and attributes needed to check for refresh of service JWT.
//
////    // TODO: Check with serviceJWT to see if our service jwt has been refreshed and we need to update clients with new jwt.
////
////    // Check cache, if we have it already we are done, otherwise continue and create one.
////    var skClient = skClientMap.get(tenantName);
////    if (skClient != null) return skClient;
////
////    // Use TenantManager to get tenant info. Needed for SK base URL.
////    // TenantManager initialized in front end api class SystemsApplication
////    Tenant tenant1 = TenantManager.getInstance().getTenant(tenantName);
////
////    // Tokens service URL comes from env or the tenants service
////    RuntimeParameters runTimeParms = RuntimeParameters.getInstance();
////    String tokensURL = runTimeParms.getTokensSvcURL();
////    if (StringUtils.isBlank(tokensURL)) tokensURL = tenant1.getTokenService();
////    if (StringUtils.isBlank(tokensURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_URL_ERROR", tenantName));
////
////    // TODO Create a ServiceJWT manager to get auto-magically refreshed service JWT for the tenant
////    // TODO: Create class to store all needed: SKClient, ServiceJWT, logic and attributes needed to check for refresh,
////    //       then cache objects of this new class. Call it SKTenantClient (?) which pkg?
////    // Put in a cache
////    var svcJWTParms = new ServiceJWTParms();
////    svcJWTParms.setServiceName(SERVICE_NAME_SYSTEMS);
////    svcJWTParms.setTenant(tenantName);
////    svcJWTParms.setTokensBaseUrl(tokensURL);
////    var serviceJwt = new ServiceJWT(svcJWTParms, "fakeServicePassword");
////    svcJWTMap.put(tenantName, serviceJwt);
////
////
////    // TODO remove in favor of ServiceJWT manager
////    // Get short term service JWT from tokens service
////    var tokClient = new TokensClient(tokensURL);
////    String svcJWT;
////    try {svcJWT = tokClient.getSvcToken(tenantName, SERVICE_NAME_SYSTEMS);}
////    catch (Exception e) {throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_ERROR", tenantName, e.getMessage()), e);}
////    // Basic check of JWT
////    if (StringUtils.isBlank(svcJWT)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_TOKENS_JWT_ERROR", tenantName));
////
////    // Get Security Kernel URL from the env or the tenants service. Env value has precedence
//////    String skURL = "https://dev.develop.tapis.io/v3";
////    String skURL = runTimeParms.getSkSvcURL();
////    if (StringUtils.isBlank(skURL)) skURL = tenant1.getSecurityKernel();
////    if (StringUtils.isBlank(skURL)) throw new TapisException(LibUtils.getMsg("SYSLIB_CREATE_SK_URL_ERROR", tenantName));
////    // TODO remove strip-off of everything after /v3 once tenant is updated or we do something different for base URL in auto-generated clients
////    // Strip off everything after the /v3 so we have a valid SK base URL
////    skURL = skURL.substring(0, skURL.indexOf("/v3") + 3);
////
////    skClient = new SKClient(skURL, svcJWT);
////    // Service to Service calls require user header, set it to be the same as the service name
////    // TODO Get string constants from shared code when available
////    String TAPIS_USER_HEADER = "X-Tapis-User";
////    String TAPIS_TENANT_HEADER = "X-Tapis-Tenant";
////    skClient.addDefaultHeader(TAPIS_USER_HEADER, SERVICE_NAME_SYSTEMS);
////    skClient.addDefaultHeader(TAPIS_TENANT_HEADER, tenantName);
////    skClientMap.put(tenantName, skClient);
////    return skClient;
  }

  /**
   * Check constraints on TSystem attributes
   * @param system - the TSystem to process
   */
  private static TSystem resolveVariables(TSystem system, String apiUserId)
  {
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with apiUserId
    String owner = system.getOwner();
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) owner = apiUserId;
    system.setOwner(owner);

    // Perform variable substitutions that happen at create time: bucketName, rootDir, jobLocalWorkingDir, jobLocalArchiveDir
    // NOTE: effectiveUserId is not processed. Var reference is retained and substitution done as needed when system is retrieved.
    //    ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
    String[] allVarSubstitutions = {apiUserId, owner, system.getTenant()};
    system.setBucketName(StringUtils.replaceEach(system.getBucketName(), ALL_VARS, allVarSubstitutions));
    system.setRootDir(StringUtils.replaceEach(system.getRootDir(), ALL_VARS, allVarSubstitutions));
    system.setJobLocalWorkingDir(StringUtils.replaceEach(system.getJobLocalWorkingDir(), ALL_VARS, allVarSubstitutions));
    system.setJobLocalArchiveDir(StringUtils.replaceEach(system.getJobLocalArchiveDir(), ALL_VARS, allVarSubstitutions));
    system.setJobRemoteArchiveDir(StringUtils.replaceEach(system.getJobRemoteArchiveDir(), ALL_VARS, allVarSubstitutions));
    return system;
  }

  /**
   * Fill in defaults and check constraints on TSystem attributes.
   * effectiveUserId is restricted.
   * If transfer mechanism S3 is supported then bucketName must be set.
   * @param system - the TSystem to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private static void validateTSystem(TSystem system) throws IllegalStateException
  {
    String msg;
    var errMessages = new ArrayList<String>();
    // Make sure owner, effectiveUserId, notes and tags are all set
    system = TSystem.checkAndSetDefaults(system);

    // Check for valid effectiveUserId
    // For CERT access the effectiveUserId cannot be static string other than owner
    String effectiveUserId = system.getEffectiveUserId();
    if (system.getDefaultAccessMethod().equals(AccessMethod.CERT) &&
        !effectiveUserId.equals(TSystem.APIUSERID_VAR) &&
        !effectiveUserId.equals(TSystem.OWNER_VAR) &&
        !StringUtils.isBlank(system.getOwner()) &&
        !effectiveUserId.equals(system.getOwner()))
    {
      // For CERT access the effectiveUserId cannot be static string other than owner
      msg = LibUtils.getMsg("SYSLIB_INVALID_EFFECTIVEUSERID_INPUT");
      errMessages.add(msg);
    }
    else if (system.getTransferMethods().contains(TransferMethod.S3) && StringUtils.isBlank(system.getBucketName()))
    {
      // For S3 support bucketName must be set
      msg = LibUtils.getMsg("SYSLIB_S3_NOBUCKET_INPUT");
      errMessages.add(msg);
    }
    else if (system.getAccessCredential() != null && effectiveUserId.equals(TSystem.APIUSERID_VAR))
    {
      // If effectiveUserId is dynamic then providing credentials is disallowed
      msg = LibUtils.getMsg("SYSLIB_CRED_DISALLOWED_INPUT");
      errMessages.add(msg);
    }
    // If validation failed throw an exception
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors("SYSLIB_CREATE_INVALID_ERRORLIST", errMessages);
      _log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }

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
   * Create a set of individual permSpec entries based on the list passed in
   * @param permList - list of individual permissions
   * @return - Set of permSpec entries based on permissions
   */
  private static Set<String> getPermSpecSet(String tenantName, String systemName, List<String> permList)
  {
    var permSet = new HashSet<String>();
    for (String permStr : permList)
    {
      // TODO/TBD: should we check that the perm matches one in the enum, possibly trimming and ignoring case
      // TODO/TBD: JSON validation at front-end can handle the check
      String permSpec = PERM_SPEC_PREFIX + tenantName + ":" + permStr.toUpperCase() + ":" + systemName;
      permSet.add(permSpec);
    }
    return permSet;
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(String firstLineKey, List<String> msgList) {
    if (StringUtils.isBlank(firstLineKey) || msgList == null || msgList.isEmpty()) return "";
    var sb = new StringBuilder(LibUtils.getMsg(firstLineKey));
    sb.append(System.lineSeparator());
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  // TODO *************** remove debug output ********************
  private static void printPermInfoForUser(SKClient skClient, String userName)
  {
    if (skClient == null || userName == null) return;
    try {
      // Test retrieving all roles for a user
      List<String> roles = skClient.getUserRoles(userName);
      _log.error("User " + userName + " has the following roles: ");
      for (String role : roles) { _log.error("  role: " + role); }
      // Test retrieving all perms for a user
      List<String> perms = skClient.getUserPerms(userName, null, null);
      _log.error("User " + userName + " has the following permissions: ");
      for (String perm : perms) { _log.error("  perm: " + perm); }
    } catch (Exception e) { _log.error(e.toString()); }
  }
  // TODO *************** remove tests ********************
}
