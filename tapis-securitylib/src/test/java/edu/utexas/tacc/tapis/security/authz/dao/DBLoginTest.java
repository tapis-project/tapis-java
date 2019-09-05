package edu.utexas.tacc.tapis.security.authz.dao;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class DBLoginTest 
{
    @Test
    public void connectTest() throws TapisException
    {
        TapisAuthzDao.getConnection();
    }
}
