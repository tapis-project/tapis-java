package edu.utexas.tacc.tapis.jobs.queue;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public class InitRabbitVHost 
{
    // Configuration.
    private static final String HOST       = "localhost";
    private static final int    PORT       = 15672;
    private static final String ADMIN_USER = "tapis";
    private static final String ADMIN_PASS = "password";
    private static final String JOBS_VHOST = "jobshost";
    private static final String JOBS_USER  = "jobs";
    private static final String JOBS_PASS  = "password";
    private static final String JOBS_PERM  = ".*";
    

    public static void main(String[] args) 
     throws IOException, InterruptedException, TapisException 
    {
        var initializer = new InitRabbitVHost();
        initializer.init();
    }
    
    private void init() 
     throws IOException, InterruptedException, TapisException
    {
        // Get a client.
        HttpClient client = HttpClient.newBuilder()
                .authenticator(new PasswordAuth())
                .build();
        
        // Do we have to create the jobshost vhost?
        if (!hasJobsHost(client)) {
            createVHost(client);
            hasJobsHost(client);
        }
        
        // Do we have the jobs user?
        if (!hasUser(client)) {
            createJobsUser(client);
            hasUser(client);
        }
        
        // Does the jobs user have the required permissions in the jobshost vhost?
        if (!hasPerms(client)) {
            assignPerms(client);
            hasPerms(client);
        }
    }
    
    private boolean hasPerms(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        // List the existing vhosts.
        HttpRequest request = HttpRequest.newBuilder()
               .uri(URI.create(makeListUserPermsUri()))
               .header("Accept", "application/json")
               .build();
        HttpResponse<String> response =
           client.send(request, BodyHandlers.ofString());
        String users = response.body();
        if (StringUtils.isBlank(users)) {
            throw new TapisException("Unable to list RabbitMQ users.");
        }
        System.out.println(users);
        var gsonArray = TapisGsonUtils.getGson().fromJson(users, JsonArray.class);
        for (JsonElement obj : gsonArray) {
            if (!obj.isJsonObject()) continue;
            var userElem = ((JsonObject)obj).get("user");
            if (userElem == null || !userElem.getAsString().equals(JOBS_USER)) continue;
            var vhostElem = ((JsonObject)obj).get("vhost");
            if (vhostElem == null || !vhostElem.getAsString().equals(JOBS_VHOST)) continue;
            
            // We have the user@vhost record.
            var configElem = ((JsonObject)obj).get("configure");
            var writeElem  = ((JsonObject)obj).get("write");
            var readElem   = ((JsonObject)obj).get("read");
            
            // Make sure all perms are allowed.
            if (configElem == null || !configElem.getAsString().equals(JOBS_PERM)) return false;
            if (writeElem == null  || !writeElem.getAsString().equals(JOBS_PERM)) return false;
            if (readElem == null   || !readElem.getAsString().equals(JOBS_PERM)) return false;
            System.out.println(JOBS_USER + " HAS required permissions");  
            return true;
        }
               
        // Not found if we get here.
        System.out.println(JOBS_USER + " DOES NOT HAVE required permissions");  
        return false;
    }    
    
    private void assignPerms(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        String body = """
            {"configure":".*", "write":".*", "read":".*"}
            """;
            
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(makeAssignUserPermsUri()))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response =
            client.send(request, BodyHandlers.ofString());
        int code = response.statusCode();
        if (code != 200 && code != 201) {
            System.out.println(JOBS_USER + " user creation failed with code " + code);
        }
    }
    
    private boolean hasUser(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        // List the existing vhosts.
        HttpRequest request = HttpRequest.newBuilder()
               .uri(URI.create(makeListUsersUri()))
               .header("Accept", "application/json")
               .build();
        HttpResponse<String> response =
           client.send(request, BodyHandlers.ofString());
        String users = response.body();
        if (StringUtils.isBlank(users)) {
            throw new TapisException("Unable to list RabbitMQ users.");
        }
        System.out.println(users);
        var gsonArray = TapisGsonUtils.getGson().fromJson(users, JsonArray.class);
        for (JsonElement obj : gsonArray) {
            if (!obj.isJsonObject()) continue;
            var nameElem = ((JsonObject)obj).get("name");
            if (nameElem == null) continue;
            if (nameElem.getAsString().equals(JOBS_USER)) {
                 System.out.println(JOBS_USER + " user FOUND");
                 return true;
            }
        }
               
        // Not found if we get here.
        System.out.println(JOBS_USER + " user NOT FOUND");  
        return false;
   }
        
    private void createJobsUser(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        String body = """
            {"password":%s, "tags":"administrator"}
            """.formatted("\"" + JOBS_PASS + "\"");
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(makeCreateUserUri()))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response =
          client.send(request, BodyHandlers.ofString());
        int code = response.statusCode();
        if (code != 200 && code != 201) {
            System.out.println(JOBS_USER + " user creation failed with code " + code);
        }
    }
           
    private boolean hasJobsHost(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        // List the existing vhosts.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(makeListVHostsUri()))
                .header("Accept", "application/json")
                .build();
        HttpResponse<String> response =
            client.send(request, BodyHandlers.ofString());
        String vhosts = response.body();
        if (StringUtils.isBlank(vhosts)) {
            throw new TapisException("Unable to list RabbitMQ vhosts.");
        }
        System.out.println(vhosts);
        var gsonArray = TapisGsonUtils.getGson().fromJson(vhosts, JsonArray.class);
        for (JsonElement obj : gsonArray) {
            if (!obj.isJsonObject()) continue;
            var nameElem = ((JsonObject)obj).get("name");
            if (nameElem == null) continue;
            if (nameElem.getAsString().equals(JOBS_VHOST)) {
                System.out.println(JOBS_VHOST + " vhost FOUND");
                return true;
            }
        }
        
        // Not found if we get here.
        System.out.println(JOBS_VHOST + " vhost NOT FOUND");  
        return false;
    }
 
    private void createVHost(HttpClient client)
     throws IOException, InterruptedException, TapisException
    {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(makeCreateVHostUri()))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .PUT(BodyPublishers.ofString(""))
            .build();
        HttpResponse<String> response =
                client.send(request, BodyHandlers.ofString());
        int code = response.statusCode();
        if (code != 200 && code != 201) {
            System.out.println(JOBS_VHOST + " vhost creation failed with code " + code);
        }
    }
    
    private String makeListUserPermsUri()
    {
        return makeHttpCmdPrefix() + "users" + "/" + JOBS_USER + "/permissions";
    }

    private String makeAssignUserPermsUri()
    {
        return makeHttpCmdPrefix() + "permissions" + "/" + JOBS_VHOST + "/" + JOBS_USER; 
    }
    
    private String makeListUsersUri()
    {
        return makeHttpCmdPrefix() + "users";
    }
    
    private String makeCreateUserUri()
    {
        return makeHttpCmdPrefix() + "users" + "/" + JOBS_USER;
    }
    
    private String makeListVHostsUri()
    {
        return makeHttpCmdPrefix() + "vhosts";
    }
    
    private String makeCreateVHostUri()
    {
        return makeHttpCmdPrefix() + "vhosts" + "/" + JOBS_VHOST;
    }
    
    private String makeHttpCmdPrefix() {return "http://" + HOST + ":" + PORT + "/api/";}
    
    private static final class PasswordAuth
     extends Authenticator
    {
        @Override
        public PasswordAuthentication getPasswordAuthentication()
        {
            return new PasswordAuthentication(ADMIN_USER, ADMIN_PASS.toCharArray());
        }
    }
}
