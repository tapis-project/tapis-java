package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.sql.Connection;
import java.util.List;

public interface SystemsDao
{
  int createTSystem(AuthenticatedUser authenticatedUser, TSystem system, String createJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  int updateTSystem(AuthenticatedUser authenticatedUser, TSystem patchedSystem, PatchSystem patchSystem,
                    String updateJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  void updateSystemOwner(AuthenticatedUser authenticatedUser, int systemId, String newOwnerName) throws TapisException;

  int softDeleteTSystem(AuthenticatedUser authenticatedUser, int systemId) throws TapisException;

  void addUpdateRecord(AuthenticatedUser authenticatedUser, int systemId, SystemOperation op, String upd_json, String upd_text) throws TapisException;

  int hardDeleteTSystem(String tenant, String name) throws TapisException;

  boolean checkDB();

  boolean checkForTSystemByName(String tenant, String name, boolean includeDeleted) throws TapisException;

  TSystem getTSystemByName(String tenant, String name) throws TapisException;

  List<TSystem> getTSystems(String tenant, List<String> selectList) throws TapisException;

  List<String> getTSystemNames(String tenant) throws TapisException;

  String getTSystemOwner(String tenant, String name) throws TapisException;

  String getTSystemEffectiveUserId(String tenant, String name) throws TapisException;

  int getTSystemId(String tenant, String name) throws TapisException;
}
