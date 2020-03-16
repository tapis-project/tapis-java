package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import org.jvnet.hk2.annotations.Contract;

import java.util.List;

/*
 * Interface for Systems Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface SystemsService
{
  int createSystem(String tenantName, String apiUserId, TSystem system, String scrubbedJson) throws TapisException, IllegalStateException, IllegalArgumentException;

  int deleteSystemByName(String tenantName, String apiUserId, String systemName) throws TapisException;

  boolean checkForSystemByName(String tenantName, String apiUserId, String systemName) throws TapisException;

  TSystem getSystemByName(String tenantName, String apiUserId, String systemName, boolean getCreds, AccessMethod accessMethod) throws TapisException;

  List<TSystem> getSystems(String tenantName, String apiUserId) throws TapisException;

  List<String> getSystemNames(String tenantName, String apiUserId) throws TapisException;

  String getSystemOwner(String tenantName, String apiUserId, String systemName) throws TapisException;

  void grantUserPermissions(String tenantName, String apiUserId, String systemName, String userName, List<String> permissions) throws TapisException;

  void revokeUserPermissions(String tenantName, String apiUserId, String systemName, String userName, List<String> permissions) throws TapisException;

  List<String> getUserPermissions(String tenantName, String apiUserId, String systemName, String userName) throws TapisException;

  void createUserCredential(String tenantName, String apiUserId, String systemName, String userName, Credential credential) throws TapisException;

  void deleteUserCredential(String tenantName, String apiUserId, String systemName, String userName) throws TapisException;

  Credential getUserCredential(String tenantName, String apiUserId, String systemName, String userName, AccessMethod accessMethod) throws TapisException;
}
