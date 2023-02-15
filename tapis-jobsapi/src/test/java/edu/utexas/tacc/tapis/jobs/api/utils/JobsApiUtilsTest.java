package edu.utexas.tacc.tapis.jobs.api.utils;

import javax.ws.rs.core.Response.Status;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;

public class JobsApiUtilsTest 
{
    @Test
    public void statusTest() {
        
        // Test that all conditions translate into their corresponding
        // status types as performed by JobsApiUtils.toHttpStatus().
        Condition condition = Condition.FORBIDDEN;
        Status status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.FORBIDDEN, status);
        
        condition = Condition.UNAUTHORIZED;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.UNAUTHORIZED, status);
        
        condition = Condition.NOT_FOUND;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.NOT_FOUND, status);
                
        condition = Condition.BAD_REQUEST;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.BAD_REQUEST, status);
        
        condition = Condition.INTERNAL_SERVER_ERROR;
        status = Status.valueOf(condition.name());
        Assert.assertEquals(Status.INTERNAL_SERVER_ERROR, status);
    }

    @Test
    public void implTest() {
        
        // Test that all conditions translate into their corresponding
        // status types as performed by JobsApiUtils.toHttpStatus().
        Status status = JobsApiUtils.toHttpStatus(Condition.FORBIDDEN);
        Assert.assertEquals(Status.FORBIDDEN, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.UNAUTHORIZED);
        Assert.assertEquals(Status.UNAUTHORIZED, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.NOT_FOUND);
        Assert.assertEquals(Status.NOT_FOUND, status);
                
        status = JobsApiUtils.toHttpStatus(Condition.BAD_REQUEST);
        Assert.assertEquals(Status.BAD_REQUEST, status);
        
        status = JobsApiUtils.toHttpStatus(Condition.INTERNAL_SERVER_ERROR);
        Assert.assertEquals(Status.INTERNAL_SERVER_ERROR, status);
    }
}
