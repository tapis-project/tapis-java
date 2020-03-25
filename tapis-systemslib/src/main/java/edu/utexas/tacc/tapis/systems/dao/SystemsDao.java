package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

public interface SystemsDao
{
  int createTSystem(AuthenticatedUser authenticatedUser, TSystem system, String scrubbedJson) throws TapisException, IllegalStateException;

  int deleteTSystem(String tenant, String name) throws TapisException;

  boolean checkForTSystemByName(String tenant, String name) throws TapisException;

  TSystem getTSystemByName(String tenant, String name) throws TapisException;

  List<TSystem> getTSystems(String tenant) throws TapisException;

  List<String> getTSystemNames(String tenant) throws TapisException;

  String getTSystemOwner(String tenant, String name) throws TapisException;

  String getTSystemEffectiveUserId(String tenant, String name) throws TapisException;
}
