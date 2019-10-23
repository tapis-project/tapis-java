package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.model.Protocol;

public interface ProtocolDao
{
  int create(String accessMechanism, String transferMechanisms,
             int port, boolean useProxy, String proxyHost, int proxyPort)
    throws TapisException;

  Protocol getById(int id) throws TapisException;

  int delete(int id) throws TapisException;
}
