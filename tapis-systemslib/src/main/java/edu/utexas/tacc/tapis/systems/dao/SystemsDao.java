package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

public interface SystemsDao
{
  int createTSystem(String tenantName, String systemName, String description, String systemType,
                    String owner, String host, boolean available, String effectiveUserId, String accessMethod,
                    String bucketName, String rootDir, String transferMethods,
                    int port, boolean useProxy, String proxyHost, int proxyPort,
                    boolean jobCanExec, String jobLocalWorkingDir, String jobLocalArchiveDir,
                    String jobRemoteArchiveSystem, String jobRemoteArchiveDir, String jobCapabilities,
                    String tags, String notes, String rawJson)
    throws TapisException, IllegalStateException;

  int deleteTSystem(String tenant, String name) throws TapisException;

  boolean checkForTSystemByName(String tenant, String name) throws TapisException;

  TSystem getTSystemByName(String tenant, String name) throws TapisException;

  List<TSystem> getTSystems(String tenant) throws TapisException;

  List<String> getTSystemNames(String tenant) throws TapisException;

  String getTSystemOwner(String tenant, String name) throws TapisException;
}
