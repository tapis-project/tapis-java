package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

public interface SystemsDao
{
  int createTSystem(String tenant, String name, String description, String owner, String host,
                    boolean available, String bucketName, String rootDir,
                    String jobInputDir, String jobOutputDir, String workDir, String scratchDir,
                    String effectiveUserId, String accessMechanism, String tags, String transferMechanisms,
                    int port, boolean useProxy, String proxyHost, int proxyPort)
    throws TapisException;

  int deleteTSystem(String tenant, String name) throws TapisException;

  TSystem getTSystemByName(String tenant, String name) throws TapisException;

  List<TSystem> getTSystems(String tenant) throws TapisException;

  List<String> getTSystemNames(String tenant) throws TapisException;
}
