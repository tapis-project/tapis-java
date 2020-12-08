package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SystemBasic;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
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

  int changeSystemOwner(AuthenticatedUser authenticatedUser, String systemId, String newOwnerName)
          throws TapisException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException, TapisClientException;

  int softDeleteSystem(AuthenticatedUser authenticatedUser, String systemId)
          throws TapisException, NotAuthorizedException, TapisClientException;

  boolean checkForSystem(AuthenticatedUser authenticatedUser, String systemId)
          throws TapisException, NotAuthorizedException, TapisClientException;

  TSystem getSystem(AuthenticatedUser authenticatedUser, String systemId, boolean getCreds, AuthnMethod authnMethod,
                    boolean requireExecPerm)
          throws TapisException, NotAuthorizedException, TapisClientException;

  int getSystemsTotalCount(AuthenticatedUser authenticatedUser, List<String> searchList, String sortBy,
                           String sortDirection, String startAfter) throws TapisException, TapisClientException;

  List<TSystem> getSystems(AuthenticatedUser authenticatedUser, List<String> searchList, int limit,
                           String sortBy, String sortDirection, int skip, String startAfter)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsUsingSqlSearchStr(AuthenticatedUser authenticatedUser, String searchStr, int limit,
                                            String sortBy, String sortDirection, int skip, String startAfter)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsSatisfyingConstraints(AuthenticatedUser authenticatedUser, String matchStr)
          throws TapisException, TapisClientException;

  SystemBasic getSystemBasic(AuthenticatedUser authenticatedUser, String systemId)
          throws TapisException, NotAuthorizedException, TapisClientException;

  List<SystemBasic> getSystemsBasic(AuthenticatedUser authenticatedUser, List<String> searchList, int limit,
                                    String sortBy, String sortDirection, int skip, String startAfter)
          throws TapisException, TapisClientException;

  List<SystemBasic> getSystemsBasicUsingSqlSearchStr(AuthenticatedUser authenticatedUser, String searchStr, int limit,
                                                     String sortBy, String sortDirection, int skip, String startAfter)
          throws TapisException, TapisClientException;

  List<String> getSystemNames(AuthenticatedUser authenticatedUser) throws TapisException;

  String getSystemOwner(AuthenticatedUser authenticatedUser, String systemId)
          throws TapisException, NotAuthorizedException, TapisClientException;

  void grantUserPermissions(AuthenticatedUser authenticatedUser, String systemId, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException;

  int revokeUserPermissions(AuthenticatedUser authenticatedUser, String systemId, String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException;

  Set<Permission> getUserPermissions(AuthenticatedUser authenticatedUser, String systemId, String userName)
          throws TapisException, NotAuthorizedException, TapisClientException;

  void createUserCredential(AuthenticatedUser authenticatedUser, String systemId, String userName, Credential credential, String scrubbedText)
          throws TapisException, NotAuthorizedException, IllegalStateException, TapisClientException;

  int deleteUserCredential(AuthenticatedUser authenticatedUser, String systemId, String userName)
          throws TapisException, NotAuthorizedException, IllegalStateException, TapisClientException;

  Credential getUserCredential(AuthenticatedUser authenticatedUser, String systemId, String userName, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, NotAuthorizedException;
}
