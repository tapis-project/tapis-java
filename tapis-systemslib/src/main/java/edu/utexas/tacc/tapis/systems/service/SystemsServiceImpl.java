package edu.utexas.tacc.tapis.systems.service;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ResultNameArray;
import edu.utexas.tacc.tapis.security.client.gen.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
                          String jobOutputDir, String workDir, String scratchDir, String effectiveUserId,
                          String accessCredential, String accessMechanism, String transferMechanisms,
                          int protocolPort, boolean protocolUseProxy,
                          String protocolProxyHost, int protocolProxyPort)
          throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    SystemsDao dao = new SystemsDaoImpl();


    int itemId = dao.createTSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                   jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId,
                                   accessMechanism, transferMechanisms, protocolPort, protocolUseProxy,
                                   protocolProxyHost, protocolProxyPort);

    // TODO Store credentials in Security Kernel
    // TODO Do real service location lookup
    String skBaseURL = "http://c002.rodeo.tacc.utexas.edu:32169/security/v3";
    // TODO Get real JWT
    // TODO This JWT encodes tenant_id = dev, username = testuser2
    String skJWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2Rldi5hcGkudGFwaXMuaW8vdG9rZW5zL3YzIiwiZXhwIjoyLjM4NDQ4MTcxMzg0MkUxMiwic3ViIjoidGVzdHVzZXIyQGRldiIsInRhcGlzL3Rva2VuX3R5cGUiOiJhY2Nlc3MiLCJ0YXBpcy9hY2NvdW50X3R5cGUiOiJ1c2VyIiwidGFwaXMvdGVuYW50X2lkIjoiZGV2IiwidGFwaXMvdXNlcm5hbWUiOiJ0ZXN0dXNlcjIiLCJ0YXBpcy9kZWxlZ2F0aW9uIjpmYWxzZX0.jsYjzhPcT2mgZm1rCc92_HjLANHWXoYi9tg5yxeS205DeTGlr_MPx_AxeAUswH7FSsxDfFDo7h6byvjR6QjCJfZ6wZgYuFirwZxL3PHaPqGtqZ1z8852vvjkiEOr6U5CD_jMsuIJJ-VH3V-4YqS01GdNSx0pukRkhEh72rhghrhy-TiA6d1tAcIMcqTf9QNOB9uOoR6H0zu6nwWlZo5SkNG2WcmOxtLJt_GvM-b9ceJYxIA7bfopBjnQpJnznXbSldTW6KfUhNucmOfl63O7_DXyzt1wsS4dGIjtmsM_B556zf6K_fDm6LODQfXftancksT0aO7fgle_DKD2hd3GByj5JTBbL5L9mZQwBuYMfH04aTRcQ8rTsNzm-B65MVgsfIN7s-x4RL4tOP_tBOaQzG7KHo-a6Ntk4LjTp2mziJCXctNYN-9IQ9g_BAoZ8HZAEMUdjx8PJHLBpNDu7o4L2wJgXUafwsNsWh64UlgmVYWh8LJYqCvBOkbIhDRWf7XRmiJexU3Y5L_0W5_ByGWcm5N5996QSnVEtL_b_bzgyKcNk4BxMpB-nsZhm8Xdo3c6Ry7TG3em3VqjZcPQDoF-smYxiG5gxmqg8AFspv7S6Nk0YspzloarkBdEO89iP97yoUt_83URVZ_Pc28Lrs7fP5oKvRH9bbGcHmb98p4Aixc";

    // TODO/TBD: Build perm specs here?
    String sysPerm = "system:" + tenant + ":*:" + name;
    String storePerm = "store:" + tenant + ":*:" + name + ":*";

//    SKClient skClient = new SKClient(skBaseURL, null);
////    skClient.addDefaultHeader("X-Tapis-Token", skJWT);
//    // Create Role and grant it to user
//    skClient.createRole(owner, "User role");
//    skClient.grantUserRole(owner, owner);
//    skClient.addRolePermission(owner, sysPerm);
//    skClient.addRolePermission(owner, storePerm);
//
//    // TODO remove test
//    // Test by retrieving role and permissions from SK
//    SkRole skRole = skClient.getRoleByName(owner);
//    _log.info("Created and then found SKRole with name: " + skRole.getName() + " Id: " + skRole.getId());
//    ResultNameArray nameArray = skClient.getUsersWithRole(owner);
//    List<String> names = nameArray.getNames();
//    if (names != null && names.contains(owner))
//    {
//      _log.info("User " + owner + " does have role " + skRole.getName());
//    } else {
//      _log.error("User " + owner + " does NOT have role " + skRole.getName());
//    }
//    ResultNameArray permArray = skClient.getUserPerms(owner);
//    List<String> perms = permArray.getNames();
//    _log.info("User " + owner + " has the following permissions: ");
//    for (String perm : perms) {
//      _log.info("  perm: " + perm);
//    }
//
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
    SystemsDao dao = new SystemsDaoImpl();
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
    SystemsDao dao = new SystemsDaoImpl();
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
    SystemsDao dao = new SystemsDaoImpl();
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
    SystemsDao dao = new SystemsDaoImpl();
    return dao.getTSystemNames(tenant);
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

}
