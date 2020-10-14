  /* ---------------------------------------------------------------------- */
  /* get{%ClassName}ByUUID:                                                 */
  /* ---------------------------------------------------------------------- */
  public {%ClassName} get{%ClassName}ByUUID(String uuid) 
    throws TapisException
  {
      // ------------------------- Check Input -------------------------
      if (StringUtils.isBlank(uuid)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "get{%ClassName}ByUUID", "uuid");
          _log.error(msg);
          throw new TapisException(msg);
      }
      
      // Initialize result.
      {%ClassName} result = null;

      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Get the select command.
          String sql = SqlStatements.SELECT_{%UpperClassName}_BY_UUID;
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, uuid);
                      
          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          result = populate{%ClassName}(rs);
          
          // Close the result and statement.
          rs.close();
          pstmt.close();
    
          // Commit the transaction.
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "{%ClassName}", uuid, e.getMessage());
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      finally {
          // Always return the connection back to the connection pool.
          try {if (conn != null) conn.close();}
            catch (Exception e) 
            {
              // If commit worked, we can swallow the exception.  
              // If not, the commit exception will be thrown.
              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
              _log.error(msg, e);
            }
      }
      
      return result;
  }
