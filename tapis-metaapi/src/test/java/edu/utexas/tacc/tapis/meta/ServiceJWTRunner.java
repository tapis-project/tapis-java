package edu.utexas.tacc.tapis.meta;

import java.util.Arrays;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.ServiceJWTParms;

public class ServiceJWTRunner {
  // TODO: FIX-FOR-ASSOCIATE-SITES
  private static final String SITE = "tacc";	
	
  public static void main(String[] args) {
    ServiceJWTParms parms = new ServiceJWTParms();
    parms.setServiceName("meta");
    parms.setTenant("master");
    parms.setTokensBaseUrl("https://dev.develop.tapis.io");
    parms.setTargetSites(Arrays.asList(SITE));
  
    String servicePassword = "awzlENwICM03MeGcn7p8CDsmoTxsEfVDFd7Fp+f20pA=";
    try {
      ServiceJWT serviceJWT = new ServiceJWT(parms,servicePassword);
      System.out.println(serviceJWT.getAccessJWT(SITE));
    } catch (TapisException | TapisClientException e) {
      e.printStackTrace();
    }
  }
}
