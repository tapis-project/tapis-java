package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.authz.model.SkSecretVersion;

public class FuncTest {

    public static void main(String[] args) 
    {
        Function <SkSecretVersion, Boolean> f1 = t ->
                StringUtils.isBlank(t.deletion_time) && !t.destroyed;
                
        var v1 = new SkSecretVersion();
        v1.destroyed = true;
        var v2 = new SkSecretVersion();
        v2.destroyed = false;
        
        System.out.println("v1: " + f1.apply(v1));
        System.out.println("v2: " + f1.apply(v2));
                
    }

}
