package edu.utexas.tacc.tapis.security.api.resources;

import java.util.ArrayList;

import edu.utexas.tacc.tapis.security.secrets.SecretType;

public class Junk {

    public static void main(String[] args) 
    {
        SecretType[] types = SecretType.values();
       // var typeArray = new String[types.length];
        var typeArray = new ArrayList<String>(types.length);
        for (int i = 0; i < types.length; i++) typeArray.add(types[i].getUrlText());
        System.out.println(typeArray.toString());

    }

}
