package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
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
  int createSystem(AuthenticatedUser authenticatedUser, TSystem system, String scrubbedJson) throws TapisException, IllegalStateException, IllegalArgumentException;

  int deleteSystemByName(AuthenticatedUser authenticatedUser, String systemName) throws TapisException;

  boolean checkForSystemByName(AuthenticatedUser authenticatedUser, String systemName) throws TapisException;

  TSystem getSystemByName(AuthenticatedUser authenticatedUser, String systemName, boolean getCreds, AccessMethod accessMethod) throws TapisException;

  List<TSystem> getSystems(AuthenticatedUser authenticatedUser) throws TapisException;

  List<String> getSystemNames(AuthenticatedUser authenticatedUser) throws TapisException;

  String getSystemOwner(AuthenticatedUser authenticatedUser, String systemName) throws TapisException;

  void grantUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, List<String> permissions) throws TapisException;

  void revokeUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, List<String> permissions) throws TapisException;

  List<String> getUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName) throws TapisException;

  void createUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, Credential credential) throws TapisException;

  void deleteUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName) throws TapisException;

  Credential getUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, AccessMethod accessMethod) throws TapisException;
}
