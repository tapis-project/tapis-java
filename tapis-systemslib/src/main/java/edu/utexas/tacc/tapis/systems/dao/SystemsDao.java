package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SystemBasic;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.util.List;

public interface SystemsDao
{
  int createTSystem(AuthenticatedUser authenticatedUser, TSystem system, String createJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  int updateTSystem(AuthenticatedUser authenticatedUser, TSystem patchedSystem, PatchSystem patchSystem,
                    String updateJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  void updateSystemOwner(AuthenticatedUser authenticatedUser, int seqId, String newOwnerName) throws TapisException;

  int softDeleteTSystem(AuthenticatedUser authenticatedUser, int seqId) throws TapisException;

  void addUpdateRecord(AuthenticatedUser authenticatedUser, int seqId, SystemOperation op, String upd_json, String upd_text) throws TapisException;

  int hardDeleteTSystem(String tenant, String id) throws TapisException;

  Exception checkDB();

  void migrateDB() throws TapisException;

  boolean checkForTSystem(String tenant, String id, boolean includeDeleted) throws TapisException;

  TSystem getTSystem(String tenant, String id) throws TapisException;

  int getTSystemsCount(String tenant, List<String> searchList, ASTNode searchAST, List<Integer> seqIDs,
                       String sortBy, String sortDirection, String startAfter) throws TapisException;

  List<TSystem> getTSystems(String tenant, List<String> searchList, ASTNode searchAST, List<Integer> seqIDs, int limit,
                            String sortBy, String sortDirection, int skip, String startAfter) throws TapisException;

  List<TSystem> getTSystemsSatisfyingConstraints(String tenant, ASTNode matchAST, List<Integer> seqIDs) throws TapisException;

  SystemBasic getSystemBasic(String tenant, String id) throws TapisException;

  List<SystemBasic> getSystemsBasic(String tenant, List<String> searchList, ASTNode searchAST, List<Integer> seqIDs, int limit,
                                    String sortBy, String sortDirection, int skip, String startAfter) throws TapisException;

  List<String> getTSystemNames(String tenant) throws TapisException;

  String getTSystemOwner(String tenant, String id) throws TapisException;

  String getTSystemEffectiveUserId(String tenant, String id) throws TapisException;

  int getTSystemSeqId(String tenant, String id) throws TapisException;
}
