package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkRole;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkShareDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkShareDao.class);
  
  // Keep track of the last monitoring outcome.
  private static final CallSiteToggle _lastQueryDBSucceeded = new CallSiteToggle();
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  /** The superclass initializes the datasource.
   * 
   * @throws TapisException on database errors
   */
  public SkShareDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getRoles:                                                              */
  /* ---------------------------------------------------------------------- */
}
