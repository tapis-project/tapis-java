package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.SQLException;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class DBLoginTest 
{
    @Test
    public void connectTest() throws TapisException, SQLException
    {
        // Disable vault so we only test the db login.
        System.setProperty("tapis.sk.vault.disable", "true");
        Connection conn = SkAbstractDao.getDataSource().getConnection();
        conn.close();
    }
}
