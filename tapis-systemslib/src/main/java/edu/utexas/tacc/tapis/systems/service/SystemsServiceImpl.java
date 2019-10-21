package edu.utexas.tacc.tapis.systems.service;

//import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.dao.ProtocolDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all
 *   top level service operations.
 */
//@Singleton
public class SystemsServiceImpl implements SystemsService
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system object
   *
   * @return Number of rows inserted
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
    SystemsDao dao = new SystemsDao();
    ProtocolDao protocolDao = new ProtocolDao();


    int protocolId = protocolDao.create(accessMechanism, transferMechanisms, protocolPort, protocolUseProxy, protocolProxyHost, protocolProxyPort);
    int numRows = dao.createTSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                    jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId, protocolId);

    // TODO Store credentials in Security Kernel

    return numRows;
  }

  /**
   * Delete a system record given the system name.
   *
   */
  @Override
  public int deleteSystemByName(String tenant, String name) throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    SystemsDao dao = new SystemsDao();
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
    SystemsDao dao = new SystemsDao();
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
    SystemsDao dao = new SystemsDao();
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
    SystemsDao dao = new SystemsDao();
    return dao.getTSystemNames(tenant);
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

}
