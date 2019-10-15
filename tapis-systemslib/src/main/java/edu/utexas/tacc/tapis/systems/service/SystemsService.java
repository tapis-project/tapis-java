package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.dao.CommandProtocolDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.TransferProtocolDao;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all
 *   top level service operations.
 */
public class SystemsService
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsService.class);

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /**
   * Create a new system object
   *
   * @return Number of rows inserted
   * @throws TapisException
   */
  public int createSystem(String tenant, String name, String description, String owner, String host,
                          boolean available, String bucketName, String rootDir, String jobInputDir,
                          String jobOutputDir, String workDir, String scratchDir, String effectiveUserId,
                          String cmdMech, int cmdPort, boolean cmdUseProxy, String cmdProxyHost, int cmdProxyPort,
                          String txfMech, int txfPort, boolean txfUseProxy, String txfProxyHost, int txfProxyPort,
                          String commandCredential, String transferCredential)
          throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    SystemsDao dao = new SystemsDao();
    CommandProtocolDao cmdDao = new CommandProtocolDao();
    TransferProtocolDao txfDao = new TransferProtocolDao();
    int cmdProtId = cmdDao.create(cmdMech, cmdPort, cmdUseProxy, cmdProxyHost, cmdProxyPort);
    int txfProtId = txfDao.create(txfMech, txfPort, txfUseProxy, txfProxyHost, txfProxyPort);
    int numRows = dao.createTSystem(tenant, name, description, owner, host, available, bucketName, rootDir,
                                    jobInputDir, jobOutputDir, workDir, scratchDir, effectiveUserId,
                                    cmdProtId, txfProtId);

    // TODO Store credentials in Security Kernel

    return numRows;
  }

  /**
   * Delete a system record given the system name.
   *
   */
  public int deleteSystem(String tenant, String name)
      throws TapisException
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
  public List<TSystem> getSystems(String tenant)
          throws TapisException
  {
    // TODO Use static factory methods for DAOs, or better yet use DI, maybe Guice
    SystemsDao dao = new SystemsDao();
    return dao.getTSystems(tenant);
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

}
