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
  int createSystem(String tenant, String apiUserId, String name, String description, String owner, String host,
                   boolean available, String bucketName, String rootDir, String jobInputDir,
                   String jobOutputDir, String workDir, String scratchDir, String effectiveUserId, String tags,
                   String notes, String accessCredential, String accessMechanism, String transferMechanisms,
                   int protocolPort, boolean protocolUseProxy, String protocolProxyHost, int protocolProxyPort,
                   String rawRequest)
    throws TapisException;

  int deleteSystemByName(String tenant, String name) throws TapisException;

  boolean checkForSystemByName(String tenant, String name) throws TapisException;

  TSystem getSystemByName(String tenant, String name, String apiUserId, boolean getCreds) throws TapisException;

  List<TSystem> getSystems(String tenant, String apiUserId) throws TapisException;

  List<String> getSystemNames(String tenant) throws TapisException;
}
