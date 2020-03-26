package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretDeleteParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.model.Credential.*;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TENANT_VAR;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 * Annotate as an hk2 Service so that default scope for DI is singleton
 */
@Service
public class SystemsServiceImpl implements SystemsService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
  private static final Set<Permission> ALL_PERMS = new HashSet<>(Set.of(Permission.ALL));
  private static final Set<Permission> READMODIFY_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  private static final String PERM_SPEC_PREFIX = "system:";

  private static final String SYSTEMS_ADMIN_ROLE = "SystemsAdmin";
  private static final String HDR_TAPIS_TOKEN = "X-Tapis-Token";
  private static final String HDR_TAPIS_TENANT = "X-Tapis-Tenant";
  private static final String HDR_TAPIS_USER = "X-Tapis-User";

  private static final String FILES_SERVICE = "files";
  private static final String JOBS_SERVICE = "jobs";
  private static final Set<String> SVCLIST_GETCRED = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_READ = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  private enum SystemOperation {create, read, modify, delete, changeOwner, getPerms,
                                grantPerms, revokePerms, setCred, removeCred, getCred}

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;

  @Inject
  private SKClient skClient;

  @Inject
  private ServiceJWT serviceJWT;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  // -----------------------------------------------------------------------
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------

  /**
   * Create a new system object
   * @param authenticatedUser - principal user containing tenant and user info
   * @param system - Pre-populated TSystem object
   * @return Sequence id of object created
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int createSystem(AuthenticatedUser authenticatedUser, TSystem system, String scrubbedJson)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.create;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (system == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();
    String apiUserId = authenticatedUser.getName();
    String systemName = system.getName();
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // ---------------------------- Check inputs ------------------------------------
    // Required system attributes: name, type, host, defaultAccessMethod
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(apiUserId) || StringUtils.isBlank(systemName) ||
        system.getSystemType() == null || StringUtils.isBlank(system.getHost()) ||
        system.getDefaultAccessMethod() == null || StringUtils.isBlank(apiUserId) || StringUtils.isBlank(scrubbedJson))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", authenticatedUser, systemName));
    }

    // Check if system already exists
    if (dao.checkForTSystemByName(systemTenantName, systemName))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", authenticatedUser, systemName));
    }

    // Make sure owner, effectiveUserId, notes and tags are all set
    // Note that this is done before auth so owner can get resolved and used during auth check.
    system.setTenant(systemTenantName);
    system = TSystem.checkAndSetDefaults(system);
    String effectiveUserId = system.getEffectiveUserId();

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    system = resolveVariables(system, authenticatedUser.getOboUser());

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, system.getName(), system.getOwner(), null, null);

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(authenticatedUser, system);

    // ----------------- Create all artifacts --------------------
    // Creation of system and role/perms/creds not in single DB transaction. Need to handle failure of role/perms/creds operations
    // Use try/catch to rollback any writes in case of failure.
    int itemId = -1;
    // Get SK client now. If we cannot get this rollback not needed.
    var skClient = getSKClient(authenticatedUser);
    try {
      // ------------------- Make Dao call to persist the system -----------------------------------
       itemId = dao.createTSystem(authenticatedUser, system, scrubbedJson);

      // ------------------- Add permissions -----------------------------------
      // Give owner and possibly effectiveUser access to the system
      String systemsPermSpec = getPermSpecStr(tenantName, systemName, Permission.ALL);
      skClient.grantUserPermission(systemTenantName, system.getOwner(), systemsPermSpec);
      if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR)) {
        skClient.grantUserPermission(systemTenantName, effectiveUserId, systemsPermSpec);
      }
      // TODO remove addition of files related permSpec
      // Give owner/effectiveUser files service related permission for root directory
      String filesPermSpec = "files:" + systemTenantName + ":*:" + systemName;
      skClient.grantUserPermission(systemTenantName, system.getOwner(), filesPermSpec);
      if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR))
        skClient.grantUserPermission(systemTenantName, effectiveUserId, filesPermSpec);

      // ------------------- Store credentials -----------------------------------
      // Store credentials in Security Kernel if cred provided and effectiveUser is static
      if (system.getAccessCredential() != null && !effectiveUserId.equals(APIUSERID_VAR)) {
        String accessUser = effectiveUserId;
        // If effectiveUser is owner resolve to static string.
        if (effectiveUserId.equals(OWNER_VAR)) accessUser = system.getOwner();
        // TODO/TBD Use private internal method instead of public API to skip auth and other checks?
        createUserCredential(authenticatedUser, systemName, accessUser, system.getAccessCredential());
      }
    }
    catch (Exception e0)
    {
      // Attempt to undo all changes and then re-throw the exception
      // Log error
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ROLLBACK", authenticatedUser, systemName, e0.getMessage());
      _log.error(msg);

      // Rollback
      // Remove system from DB
      if (itemId != -1) try {dao.deleteTSystem(tenantName, systemName); } catch (Exception e) {};
      // Remove perms
      String systemsPermSpec = getPermSpecStr(tenantName, systemName, Permission.ALL);
      String filesPermSpec = "files:" + tenantName + ":*:" + systemName;
      try { skClient.revokeUserPermission(systemTenantName, system.getOwner(), systemsPermSpec); } catch (Exception e) {};
      try { skClient.revokeUserPermission(systemTenantName, effectiveUserId, systemsPermSpec); } catch (Exception e) {};
      try { skClient.revokeUserPermission(systemTenantName, system.getOwner(), filesPermSpec);  } catch (Exception e) {};
      try { skClient.revokeUserPermission(systemTenantName, effectiveUserId, filesPermSpec);  } catch (Exception e) {};
      // Remove creds
      try
      {
        if (system.getAccessCredential() != null && !effectiveUserId.equals(APIUSERID_VAR)) {
          String accessUser = effectiveUserId;
          if (effectiveUserId.equals(OWNER_VAR)) accessUser = system.getOwner();
          // TODO/TBD Use private internal method instead of public API to skip auth and other checks?
          deleteUserCredential(authenticatedUser, systemName, accessUser);
        }
      } catch (Exception e) {};
      throw e0;
    }
    return itemId;
  }

  /**
   * Delete a system record given the system name.
   * Also remove permissions and credentials from the Security Kernel
   *
   * @param authenticatedUser - principal user containing tenant and user info
   * @param systemName - name of system
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int deleteSystemByName(AuthenticatedUser authenticatedUser, String systemName) throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.delete;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then 0 changes
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return 0;

      // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, null, null);

    var skClient = getSKClient(authenticatedUser);
    // TODO: Remove all credentials associated with the system.
    // TODO: Have SK do this in one operation?
//    deleteUserCredential(tenantName, systemName, );
    // Construct basic SK secret parameters
//    var sParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysOwner(accessUser);
//    skClient.destroySecretMeta(sParms);

    // TODO/TBD: How to make sure all perms for a system are removed?
    // TODO: See if it makes sense to have a SK method to do this in one operation
    // Use Security Kernel client to find all users with perms associated with the system.
    // Get the Security Kernel client
    String permSpec = PERM_SPEC_PREFIX + tenantName + ":%:" + systemName;
    var userNames = skClient.getUsersWithPermission(tenantName, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames) {
      revokeUserPermissions(authenticatedUser, systemName, userName, ALL_PERMS);
    }

    // Delete the system
    return dao.deleteTSystem(tenantName, systemName);
  }

  /**
   * checkForSystemByName
   * @param authenticatedUser - principal user containing tenant and user info
   * @param systemName - Name of the system
   * @return true if system exists, false if system does not exist
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSystemByName(AuthenticatedUser authenticatedUser, String systemName) throws TapisException
  {
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();
    boolean result = dao.checkForTSystemByName(systemTenantName, systemName);
    return result;
  }

  /**
   * getSystemByName
   * @param authenticatedUser - principal user containing tenant and user info
   * @param systemName - Name of the system
   * @return TSystem - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public TSystem getSystemByName(AuthenticatedUser authenticatedUser, String systemName, boolean getCreds, AccessMethod accMethod1)
          throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.read;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    // Extract various names for convenience
    String apiUserId = authenticatedUser.getName();
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then return null
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return null;
//TODO/TBD    // If system does not exist then throw an exception
//    if (!dao.checkForTSystemByName(systemTenantName, systemName))
//      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, null, null);

    TSystem result = dao.getTSystemByName(systemTenantName, systemName);
    if (result == null) return null;
    // Resolve effectiveUserId if necessary
    String effectiveUserId = resolveEffectiveUserId(result.getEffectiveUserId(), result.getOwner(), apiUserId);
    // Update result with effectiveUserId
    result.setEffectiveUserId(effectiveUserId);
    // If requested and effectiveUserid is not ${apiUserId} (i.e. is static) then retrieve credentials from Security Kernel
    if (getCreds && !result.getEffectiveUserId().equals(TSystem.APIUSERID_VAR))
    {
      AccessMethod accMethod = result.getDefaultAccessMethod();
      // If accessMethod specified then use it instead of default access method defined for the system.
      if (accMethod1 != null) accMethod = accMethod1;
      Credential cred = getUserCredential(authenticatedUser, systemName, effectiveUserId, accMethod);
      result.setAccessCredential(cred);
    }
    return result;
  }

//  /**
//   * Get all systems
//   * @param authenticatedUser - principal user containing tenant and user info
//   * @return List of TSystem objects
//   * @throws TapisException - for Tapis related exceptions
//   * @throws NotAuthorizedException - unauthorized
//   */
//  @Override
//  public List<TSystem> getSystems(AuthenticatedUser authenticatedUser) throws TapisException, NotAuthorizedException
//  {
//    SystemOperation op = SystemOperation.read;
//    // ------------------------- Check service level authorization -------------------------
//    checkAuth(authenticatedUser, op, null, null, null, null);
//
//    List<TSystem> result = dao.getTSystems(authenticatedUser.getTenantId());
//    for (TSystem sys : result)
//    {
//      sys.setEffectiveUserId(resolveEffectiveUserId(sys.getEffectiveUserId(), sys.getOwner(), authenticatedUser.getName()));
//    }
//    return result;
//  }

  /**
   * Get list of system names
   * @param authenticatedUser - principal user containing tenant and user info
   * @return - list of systems
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public List<String> getSystemNames(AuthenticatedUser authenticatedUser) throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.read;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    // ------------------------- Check service level authorization -------------------------
    // TODO: Filter based on user authorization
//    checkAuth(authenticatedUser, op, null, null, null, null);

    return dao.getTSystemNames(authenticatedUser.getTenantId());
  }

  /**
   * Get system owner
   * @param authenticatedUser - principal user containing tenant and user info
   * @param systemName - Name of the system
   * @return - Owner or null if system not found
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public String getSystemOwner(AuthenticatedUser authenticatedUser, String systemName) throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.read;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));

    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist return null
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return null;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, null, null);

    return dao.getTSystemOwner(authenticatedUser.getTenantId(), systemName);
  }

  // -----------------------------------------------------------------------
  // --------------------------- Permissions -------------------------------
  // -----------------------------------------------------------------------

  /**
   * Grant permissions to a user for a system
   * NOTE: This only impacts the default user role
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void grantUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions)
    throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.grantPerms;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then throw an exception
    if (!dao.checkForTSystemByName(systemTenantName, systemName))
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, null, null);

    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();

    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) ||
        permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(systemTenantName, systemName, permissions);

    // Get the Security Kernel client
    var skClient = getSKClient(authenticatedUser);

    // Assign perms to user. SK creates a default role for the user
    try
    {
      for (String permSpec : permSpecSet)
      {
        skClient.grantUserPermission(systemTenantName, userName, permSpec);
      }
    }
    // TODO exception handling
    catch (Exception e) { _log.error(e.toString()); throw e;}
  }

  /**
   * Revoke permissions from a user for a system
   * NOTE: This only impacts the default user role
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int revokeUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions)
    throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.revokePerms;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then return 0 changes
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return 0;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, userName, permissions);

    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();

    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) ||
      permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(systemTenantName, systemName, permissions);

    // Get the Security Kernel client
    var skClient = getSKClient(authenticatedUser);

    // Remove perms from default user role
    try
    {
      for (String permSpec : permSpecSet)
      {
        skClient.revokeUserPermission(systemTenantName, userName, permSpec);
      }
    }
    // TODO exception handling
    catch (Exception e) { _log.error(e.toString()); throw e;}
    return permSpecSet.size();
  }

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   *
   * @return List of permissions
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public Set<Permission> getUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.getPerms;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then return null
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return null;
//TODO/TBD    // If system does not exist then throw an exception
//    if (!dao.checkForTSystemByName(systemTenantName, systemName))
//      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, userName, null);

    var userPerms = new HashSet<Permission>();
    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();

    // Use Security Kernel client to check for each permission in the enum list
    var skClient = getSKClient(authenticatedUser);
    for (Permission perm : Permission.values())
    {
      String permSpec = PERM_SPEC_PREFIX + systemTenantName + ":" + perm.name() + ":" + systemName;
      try
      {
        Boolean isAuthorized = skClient.isPermitted(systemTenantName, userName, permSpec);
        if (Boolean.TRUE.equals(isAuthorized)) userPerms.add(perm);
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void createUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, Credential credential)
          throws TapisException, NotAuthorizedException, IllegalStateException
  {
    SystemOperation op = SystemOperation.setCred;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then throw an exception
    if (!dao.checkForTSystemByName(systemTenantName, systemName))
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, userName, null);

    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();
    String apiUserId = authenticatedUser.getName();

    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName) || credential == null)
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }
    // Get the Security Kernel client
    var skClient = getSKClient(authenticatedUser);
    try {
      // Construct basic SK secret parameters including tenant, system and user for credential
      var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      sParms.setTenant(systemTenantName).setSysId(systemName).setSysUser(userName);
      Map<String, String> dataMap;
      // Check for each secret type and write values if they are present
      // Note that multiple secrets may be present.
      // Store password if present
      if (!StringUtils.isBlank(credential.getPassword())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.password);
        dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
        sParms.setData(dataMap);
        skClient.writeSecret(tenantName, apiUserId , sParms);
      }
      // Store PKI keys if both present
      if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.sshkey);
        dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
        dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
        sParms.setData(dataMap);
        skClient.writeSecret(tenantName, apiUserId, sParms);
      }
      // Store Access key and secret if both present
      if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret())) {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.accesskey);
        dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
        dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
        sParms.setData(dataMap);
        skClient.writeSecret(tenantName, apiUserId, sParms);
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int deleteUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException, IllegalStateException
  {
    SystemOperation op = SystemOperation.removeCred;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    int changeCount = 0;
    // If system does not exist then return 0 changes
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return changeCount;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, userName, null);

    // Extract various names for convenience
    String tenantName = authenticatedUser.getTenantId();
    String apiUserId = authenticatedUser.getName();

    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(tenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName))
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }
    // Get the Security Kernel client
    var skClient = getSKClient(authenticatedUser);

    // TODO: Return 0 if credential does not exist
    // TODO: Currently this throws an exception
//    var sReadParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysUser(userName);
//    // TODO/TBD Currently using oboTeneant but apiUser. Should these be OBO or svc values?
//    sReadParms.setTenant(oboTenantName).setUser(apiUserId).setKeyType(KeyType.sshkey);
//    SkSecret skSecret = skClient.readSecret(sReadParms);
//    if (skSecret == null) return changeCount;

    // Construct basic SK secret parameters
//    var sParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME).setSysId(systemName).setSysUser(userName);
    var sParms = new SKSecretDeleteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    sParms.setTenant(systemTenantName).setSysId(systemName).setSysUser(userName);
    sParms.setUser(apiUserId).setVersions(Collections.emptyList());
    try
    {
//    // TODO/TBD Currently using oboTeneant but apiUser. Should these be OBO or svc values?
// TODO/TBD: Do we need to call setUser?
      sParms.setKeyType(KeyType.password);
      List<Integer> intList = skClient.destroySecret(tenantName, apiUserId, sParms);
      // Return value is a list of destroyed versions. If any destroyed increment changeCount by 1
      if (intList != null && !intList.isEmpty()) changeCount++;
      sParms.setKeyType(KeyType.sshkey);
      intList = skClient.destroySecret(tenantName, apiUserId, sParms);
      if (intList != null && !intList.isEmpty()) changeCount++;
      sParms.setKeyType(KeyType.accesskey);
      intList = skClient.destroySecret(tenantName, apiUserId, sParms);
      if (intList != null && !intList.isEmpty()) changeCount++;
      // TODO/TBD: This currently throws a "not found" exception. How to handle it? Have SK make it a no-op? Catch exception for each call?
//      sParms.setKeyType(KeyType.cert);
//      skClient.destroySecret(tenantName, apiUserId, sParms);
    }
    // TODO Also clean up secret metadata

    // TODO exception handling
    // If tapis client exception then log error and convert to TapisException
    // TODO/TBD: for other exectpions log error and re-throw the exception
    catch (TapisClientException tce)
    {
      _log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_DELETE_ERROR", authenticatedUser, systemName, op.name()), tce);
    }
    catch (Exception e) { _log.error(e.toString()); throw e;}
    // TODO/TBD If anything destroyed we consider it the removal of a single credential
    if (changeCount > 0) changeCount = 1;
    return changeCount;
  }

  /**
   * Get credential for given system, user and access method
   * @return Credential - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public Credential getUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, AccessMethod accessMethod)
          throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.getCred;
    if (authenticatedUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemName)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", authenticatedUser));
    String systemTenantName = authenticatedUser.getTenantId();
    // For service request use oboTenant for tenant associated with the system
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) systemTenantName = authenticatedUser.getOboTenantId();

    // If system does not exist then return null
    if (!dao.checkForTSystemByName(systemTenantName, systemName)) return null;
//TODO/TBD    // If system does not exist then throw an exception
//    if (!dao.checkForTSystemByName(systemTenantName, systemName))
//      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(authenticatedUser, op, systemName, null, userName, null);

    // Extract various names for convenience
    String oboTenantName = authenticatedUser.getOboTenantId();
    String apiUserId = authenticatedUser.getName();

    // Check inputs. If anything null or empty throw an exception
    if (StringUtils.isBlank(oboTenantName) || StringUtils.isBlank(systemName) || StringUtils.isBlank(userName))
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // If accessMethod not passed in fill in with default from system
    if (accessMethod == null)
    {
      TSystem sys = dao.getTSystemByName(systemTenantName, systemName);
      if (sys == null)  throw new TapisException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", authenticatedUser, systemName));
      accessMethod = sys.getDefaultAccessMethod();
    }

    Credential credential = null;
    try
    {
      // Get the Security Kernel client
      var skClient = getSKClient(authenticatedUser);
      // Construct basic SK secret parameters
      var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      sParms.setTenant(systemTenantName).setSysId(systemName).setSysUser(userName);
      // todo remove next line
//      sParms.setSysUser("alsdkfjdskfj");
      // TODO/TBD Currently using oboTeneant but apiUser. Should these be OBO or svc values?
// TODO/TBD: do we need to setUser?
      sParms.setUser(apiUserId);
      // Set key type based on access method
      if (accessMethod.equals(AccessMethod.PASSWORD))sParms.setKeyType(KeyType.password);
      else if (accessMethod.equals(AccessMethod.PKI_KEYS))sParms.setKeyType(KeyType.sshkey);
      else if (accessMethod.equals(AccessMethod.ACCESS_KEY))sParms.setKeyType(KeyType.accesskey);
      else if (accessMethod.equals(AccessMethod.CERT))sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      // TODO/TBD: why not pass in tenant and apiUser here?
      SkSecret skSecret = skClient.readSecret(sParms);
      if (skSecret == null) return null;
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) return null;

      // Create a credential
      credential = new Credential(dataMap.get(SK_KEY_PASSWORD),
              dataMap.get(SK_KEY_PRIVATE_KEY),
              dataMap.get(SK_KEY_PUBLIC_KEY),
              dataMap.get(SK_KEY_ACCESS_KEY),
              dataMap.get(SK_KEY_ACCESS_SECRET),
              null); //dataMap.get(CERT) TODO: how to get ssh certificate
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
   * @param authenticatedUser - name of tenant
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient(AuthenticatedUser authenticatedUser) throws TapisException
  {
    String tenantName = authenticatedUser.getTenantId();
    String apiUserId = authenticatedUser.getName();
    // Use TenantManager to get tenant info. Needed for tokens and SK base URLs.
    Tenant tenant = TenantManager.getInstance().getTenant(tenantName);

    // Update SKClient on the fly. If this becomes a bottleneck we can add a cache.
    // Get Security Kernel URL from the env or the tenants service. Env value has precedence.
    //    String skURL = "https://dev.develop.tapis.io/v3";
    String skURL = RuntimeParameters.getInstance().getSkSvcURL();
    if (StringUtils.isBlank(skURL)) skURL = tenant.getSecurityKernel();
    if (StringUtils.isBlank(skURL)) throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CREATE_SK_URL_ERROR", authenticatedUser));
    // TODO remove strip-off of everything after /v3 once tenant is updated or we do something different for base URL in auto-generated clients
    // Strip off everything after the /v3 so we have a valid SK base URL
    skURL = skURL.substring(0, skURL.indexOf("/v3") + 3);
    skClient.setBasePath(skURL);

    skClient.addDefaultHeader(HDR_TAPIS_TOKEN, serviceJWT.getAccessJWT());
    // For service jwt pass along boTenant and oboUser in headers
    // For user jwt use master tenant name and service name in headers
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType()))
    {
      skClient.addDefaultHeader(HDR_TAPIS_TENANT, authenticatedUser.getOboTenantId());
      skClient.addDefaultHeader(HDR_TAPIS_USER, authenticatedUser.getOboUser());
    }
    else
    {
      skClient.addDefaultHeader(HDR_TAPIS_TENANT, serviceJWT.getTenant());
      skClient.addDefaultHeader(HDR_TAPIS_USER, serviceJWT.getServiceName());
    }
    return skClient;
  }

  /**
   * Resolve variables for TSystem attributes
   * @param system - the TSystem to process
   */
  private static TSystem resolveVariables(TSystem system, String oboUser)
  {
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in oboUser.
    // Note that for a user request oboUser and apiUserId are the same and for a service request we want oboUser here.
    String owner = system.getOwner();
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) owner = oboUser;
    system.setOwner(owner);

    // Perform variable substitutions that happen at create time: bucketName, rootDir, jobLocalWorkingDir, jobLocalArchiveDir
    // NOTE: effectiveUserId is not processed. Var reference is retained and substitution done as needed when system is retrieved.
    //    ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
    String[] allVarSubstitutions = {oboUser, owner, system.getTenant()};
    system.setBucketName(StringUtils.replaceEach(system.getBucketName(), ALL_VARS, allVarSubstitutions));
    system.setRootDir(StringUtils.replaceEach(system.getRootDir(), ALL_VARS, allVarSubstitutions));
    system.setJobLocalWorkingDir(StringUtils.replaceEach(system.getJobLocalWorkingDir(), ALL_VARS, allVarSubstitutions));
    system.setJobLocalArchiveDir(StringUtils.replaceEach(system.getJobLocalArchiveDir(), ALL_VARS, allVarSubstitutions));
    system.setJobRemoteArchiveDir(StringUtils.replaceEach(system.getJobRemoteArchiveDir(), ALL_VARS, allVarSubstitutions));
    return system;
  }

  /**
   * Check constraints on TSystem attributes.
   * effectiveUserId is restricted.
   * If transfer mechanism S3 is supported then bucketName must be set.
   * @param system - the TSystem to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private static void validateTSystem(AuthenticatedUser authenticatedUser, TSystem system) throws IllegalStateException
  {
    String msg;
    var errMessages = new ArrayList<String>();

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
      String allErrors = getListOfErrors(authenticatedUser, system.getName(), errMessages);
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
  private static Set<String> getPermSpecSet(String tenantName, String systemName, Set<Permission> permList)
  {
    var permSet = new HashSet<String>();
    if (permList.contains(Permission.ALL)) permSet.add(getPermSpecStr(tenantName, systemName, Permission.ALL));
    else {
      for (Permission perm : permList) {
        permSet.add(getPermSpecStr(tenantName, systemName, perm));
      }
    }
    return permSet;
  }

  /**
   * Create a permSpec given a permission
   * @param perm - permission
   * @return - permSpec entry based on permission
   */
  private static String getPermSpecStr(String tenantName, String systemName, Permission perm)
  {
    if (perm.equals(Permission.ALL)) return PERM_SPEC_PREFIX + tenantName + ":*:" + systemName;
    else return PERM_SPEC_PREFIX + tenantName + ":" + perm.name().toUpperCase() + ":" + systemName;
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(AuthenticatedUser authenticatedUser, String systemName, List<String> msgList) {
    var sb = new StringBuilder(LibUtils.getMsgAuth("SYSLIB_CREATE_INVALID_ERRORLIST", authenticatedUser, systemName));
    sb.append(System.lineSeparator());
    if (msgList == null || msgList.isEmpty()) return sb.toString();
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  /**
   * Check service level authorization
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and
   *   authorization is denied.
   * Operations:
   *  Create - must be owner or have admin role
   *  Read - must be owner or have admin role or have READ or MODIFY permission or be in list of allowed services
   *  Delete - must be owner or have admin role
   *  Modify - must be owner or have admin role or have MODIFY permission
   *  ChangeOwner - must be owner or have admin role
   *  GrantPerm -  must be owner or have admin role
   *  RevokePerm -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserRevokePerm)
   *  SetCred -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserCredOp)
   *  RemoveCred -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserCredOp)
   *  GetCred -  must be a service in the list of allowed services
   *
   * @param authenticatedUser - principal user containing tenant and user info
   * @param operation - operation name
   * @param systemName - name of the system
   * @param owner - system owner
   * @param perms - List of permissions for the revokePerm case
   * @throws NotAuthorizedException - apiUserId not authorized to perform operation
   */
  private void checkAuth(AuthenticatedUser authenticatedUser, SystemOperation operation, String systemName,
                         String owner, String targetUser, Set<Permission> perms)
      throws TapisException, NotAuthorizedException, IllegalStateException
  {
    // Check service and user requests separately to avoid confusing a service name with a user name
    if (TapisThreadContext.AccountType.service.name().equals(authenticatedUser.getAccountType())) {
      // Service check
      switch (operation) {
        case read:
          if (SVCLIST_READ.contains(authenticatedUser.getName())) return;
          break;
        case getCred:
          if (SVCLIST_GETCRED.contains(authenticatedUser.getName())) return;
          break;
      }
    }
    else
    {
      // User check
      // Requires owner. If no owner specified and owner cannot be determined then log an error and deny.
      if (StringUtils.isBlank(owner)) owner = dao.getTSystemOwner(authenticatedUser.getTenantId(), systemName);
      if (StringUtils.isBlank(owner)) {
        String msg = LibUtils.getMsgAuth("SYSLIB_AUTH_NO_OWNER", authenticatedUser, systemName, operation.name());
        _log.error(msg);
        throw new NotAuthorizedException(msg);
      }
      switch(operation) {
        case create:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser)) return;
          break;
        case read:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
              isPermittedAny(authenticatedUser, systemName, READMODIFY_PERMS)) return;
          break;
        case modify:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
              isPermitted(authenticatedUser, systemName, Permission.MODIFY)) return;
          break;
        case delete:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser)) return;
          break;
        case changeOwner:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser)) return;
          break;
        case getPerms:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
                  isPermittedAny(authenticatedUser, systemName, READMODIFY_PERMS)) return;
          break;
        case grantPerms:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser)) return;
          break;
        case revokePerms:
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
              (authenticatedUser.getName().equals(targetUser) &&
                      allowUserRevokePerm(authenticatedUser, systemName, perms))) return;
          break;
        case setCred:
          // TODO usr case
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
              (authenticatedUser.getName().equals(targetUser) &&
                      allowUserCredOp(authenticatedUser, systemName, operation))) return;
          break;
        case removeCred:
          // TODO usr case
          if (owner.equals(authenticatedUser.getName()) || hasAdminRole(authenticatedUser) ||
              (authenticatedUser.getName().equals(targetUser) &&
                      allowUserCredOp(authenticatedUser, systemName, operation))) return;
          break;
      }
    }
    // Not authorized, throw an exception
    String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH", authenticatedUser, systemName, operation.name());
    throw new NotAuthorizedException(msg);
  }

  /**
   * Check to see if apiUserId has the service admin role
   */
  private boolean hasAdminRole(AuthenticatedUser authenticatedUser) throws TapisException
  {
    var skClient = getSKClient(authenticatedUser);
    return skClient.hasRole(authenticatedUser.getTenantId(), authenticatedUser.getName(), SYSTEMS_ADMIN_ROLE);
  }

  /**
   * Check to see if apiUserId has the specified permission
   */
  private boolean isPermitted(AuthenticatedUser authenticatedUser, String systemName, Permission perm) throws TapisException
  {
    var skClient = getSKClient(authenticatedUser);
    String permSpecStr = getPermSpecStr(authenticatedUser.getTenantId(), systemName, perm);
    return skClient.isPermitted(authenticatedUser.getTenantId(), authenticatedUser.getName(), permSpecStr);
  }

  /**
   * Check to see if apiUserId has any of the set of permissions
   */
  private boolean isPermittedAny(AuthenticatedUser authenticatedUser, String systemName, Set<Permission> perms) throws TapisException
  {
    var skClient = getSKClient(authenticatedUser);
    var permSpecs = new ArrayList<String>();
    for (Permission perm : perms) {
      permSpecs.add(getPermSpecStr(authenticatedUser.getTenantId(), systemName, perm));
    }
    return skClient.isPermittedAny(authenticatedUser.getTenantId(), authenticatedUser.getName(), permSpecs.toArray(new String[0]));
  }

  /**
   * Check to see if apiUserId who is not owner or admin is authorized to revoke permissions
   */
  private boolean allowUserRevokePerm(AuthenticatedUser authenticatedUser, String systemName, Set<Permission> perms) throws TapisException
  {
    if (perms.contains(Permission.MODIFY)) return isPermitted(authenticatedUser, systemName, Permission.MODIFY);
    if (perms.contains(Permission.READ)) return isPermittedAny(authenticatedUser, systemName, READMODIFY_PERMS);
    // TODO what if perms contains ALL?
    return false;
  }

  /**
   * Check to see if apiUserId who is not owner or admin is authorized to operate on a credential
   * No checks are done for incoming arguments and the system must exist
   */
  private boolean allowUserCredOp(AuthenticatedUser authenticatedUser, String systemName, SystemOperation op)
          throws TapisException, IllegalStateException
  {
    // Get the effectiveUserId. If not ${apiUserId} then considered an error since credential would never be used.
    String effectiveUserId = dao.getTSystemEffectiveUserId(authenticatedUser.getTenantId(), systemName);
    if (StringUtils.isBlank(effectiveUserId) || !effectiveUserId.equals(APIUSERID_VAR))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOTAPIUSER", authenticatedUser, systemName, op.name());
      _log.error(msg);
      throw new IllegalStateException(msg);

    }
    return true;
  }

// TODO *************** remove debug output ********************
//  private static void printPermInfoForUser(SKClient skClient, String userName)
//  {
//    if (skClient == null || userName == null) return;
//    try {
//      // Test retrieving all roles for a user
//      List<String> roles = skClient.getUserRoles(userName);
//      _log.error("User " + userName + " has the following roles: ");
//      for (String role : roles) { _log.error("  role: " + role); }
//      // Test retrieving all perms for a user
//      List<String> perms = skClient.getUserPerms(userName, null, null);
//      _log.error("User " + userName + " has the following permissions: ");
//      for (String perm : perms) { _log.error("  perm: " + perm); }
//    } catch (Exception e) { _log.error(e.toString()); }
//  }
// TODO *************** remove tests ********************
}
