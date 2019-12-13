package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

/*
 * Interface for Systems Service
 * Needed for dependency injection.
 */
public interface SystemsService
{
  int createSystem(String tenantNameName, String apiUserId, String systemName, String description, String owner, String host,
                   boolean available, String bucketName, String rootDir, String jobInputDir,
                   String jobOutputDir, String workDir, String scratchDir, String effectiveUserId, String tags,
                   String notes, char[] accessCredential, String accessMechanism, String transferMechanisms,
                   int protocolPort, boolean protocolUseProxy, String protocolProxyHost, int protocolProxyPort,
                   String rawRequest)
    throws TapisException, IllegalStateException;

  int deleteSystemByName(String tenantName, String systemName) throws TapisException;

  boolean checkForSystemByName(String tenantName, String systemName) throws TapisException;

  TSystem getSystemByName(String tenantName, String systemName, String apiUserId, boolean getCreds) throws TapisException;

  List<TSystem> getSystems(String tenantName, String apiUserId) throws TapisException;

  List<String> getSystemNames(String tenantName) throws TapisException;

  String getSystemOwner(String tenantName, String systemName) throws TapisException;

  void grantUserPermissions(String tenantName, String systemName, String userName, List<String> permissions) throws TapisException;

  void revokeUserPermissions(String tenantName, String systemName, String userName, List<String> permissions) throws TapisException;

  List<String> getUserPermissions(String tenantName, String systemName, String userName) throws TapisException;
}
