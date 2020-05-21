package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import org.jvnet.hk2.annotations.Contract;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Set;

/*
 * Interface for Systems Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface SystemsService
{
  int createSystem(AuthenticatedUser authenticatedUser, TSystem system, String scrubbedText)
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException;

  int updateSystem(AuthenticatedUser authenticatedUser, PatchSystem patchSystem, String scrubbedText)
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int changeSystemOwner(AuthenticatedUser authenticatedUser, String systemName, String newOwnerName)
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int softDeleteSystemByName(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException;

  boolean checkForSystemByName(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException;

  TSystem getSystemByName(AuthenticatedUser authenticatedUser, String systemName, boolean getCreds, AccessMethod accessMethod)
          throws TapisException, NotAuthorizedException;

  List<TSystem> getSystems(AuthenticatedUser authenticatedUser, List<String> selectList)
          throws TapisException;

  List<String> getSystemNames(AuthenticatedUser authenticatedUser)
          throws TapisException;

  String getSystemOwner(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException;

  void grantUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException;

  int revokeUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException;

  Set<Permission> getUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException;

  void createUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, Credential credential, String scrubbedText)
          throws TapisException, NotAuthorizedException, IllegalStateException;

  int deleteUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException, IllegalStateException;

  Credential getUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, AccessMethod accessMethod)
          throws TapisException, NotAuthorizedException;
}
