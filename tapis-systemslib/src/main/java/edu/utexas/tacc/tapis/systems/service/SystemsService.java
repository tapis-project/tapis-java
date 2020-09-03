package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
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
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, TapisClientException;

  int updateSystem(AuthenticatedUser authenticatedUser, PatchSystem patchSystem, String scrubbedText)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int changeSystemOwner(AuthenticatedUser authenticatedUser, String systemName, String newOwnerName)
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException, TapisClientException;

  int softDeleteSystemByName(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException, TapisClientException;

  boolean checkForSystemByName(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException, TapisClientException;

  TSystem getSystemByName(AuthenticatedUser authenticatedUser, String systemName, boolean getCreds, AccessMethod accessMethod)
          throws TapisException, NotAuthorizedException, TapisClientException;

  List<TSystem> getSystems(AuthenticatedUser authenticatedUser, List<String> searchList)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsUsingSqlSearchStr(AuthenticatedUser authenticatedUser, String searchStr)
          throws TapisException, TapisClientException;

  List<String> getSystemNames(AuthenticatedUser authenticatedUser)
          throws TapisException;

  String getSystemOwner(AuthenticatedUser authenticatedUser, String systemName)
          throws TapisException, NotAuthorizedException, TapisClientException;

  void grantUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException;

  int revokeUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException;

  Set<Permission> getUserPermissions(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException, TapisClientException;

  void createUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, Credential credential, String scrubbedText)
          throws TapisException, NotAuthorizedException, IllegalStateException, TapisClientException;

  int deleteUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName)
          throws TapisException, NotAuthorizedException, IllegalStateException, TapisClientException;

  Credential getUserCredential(AuthenticatedUser authenticatedUser, String systemName, String userName, AccessMethod accessMethod)
          throws TapisException, TapisClientException, NotAuthorizedException;
}
