package edu.utexas.tacc.tapis.security.commands.model;

import java.util.Map;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor.Op;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public final class SkAdminResults 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminResults.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Singleton instance.
    private static SkAdminResults _instance;
    
    // Summary outcome information.
    private int secretsCreated;
    private int secretsUpdated;
    private int secretsDeployed;
    private int secretsSkipped;
    private int secretsFailed;
    
    // Summary generation information.
    private int keyPairsGenerated;
    private int passwordsGenerated;
    
    // Detailed information
    private TreeSet<String> dbCredentialMsgs = new TreeSet<>();
    private TreeSet<String> jwtSigningMsgs = new TreeSet<>();
    private TreeSet<String> servicePwdMsgs = new TreeSet<>();
    private TreeSet<String> userMsgs = new TreeSet<>();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private SkAdminResults() {}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** No need for synchronization since SkAdmin is a singled threaded program.
     * 
     * @return the singleton instance of this class
     */
    public static SkAdminResults getInstance()
    {
        if (_instance == null) _instance = new SkAdminResults();
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordSuccess:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordSuccess(Op op, SecretType type, String message)
    {
        // Tally the outcome.
        switch (op) {
            case create: 
                secretsCreated++;
                break;
            case update: 
                secretsUpdated++;
                break;
            case deploy: 
                secretsDeployed++;
                break;
        }
        
        // Record the message.
        addMessage(type, message);
    }
    
    /* ---------------------------------------------------------------------- */
    /* recordFailure:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordFailure(Op op, SecretType type, String message)
    {
        // Tally the outcome.
        secretsFailed++;
        
        // Record the message.
        addMessage(type, message);
    }   
    
    /* ---------------------------------------------------------------------- */
    /* recordSkipped:                                                         */
    /* ---------------------------------------------------------------------- */
    public void recordSkipped(Op op, SecretType type, String message)
    {
        // Tally the outcome.
        secretsSkipped++;
        
        // Record the message.
        addMessage(type, message);
    }
    
    /* ---------------------------------------------------------------------- */
    /* incrementKeyPairsGenerated:                                            */
    /* ---------------------------------------------------------------------- */
    public void incrementKeyPairsGenerated()  {keyPairsGenerated++;}
    
    /* ---------------------------------------------------------------------- */
    /* incrementPasswordsGenerated:                                           */
    /* ---------------------------------------------------------------------- */
    public void incrementPasswordsGenerated() {passwordsGenerated++;}
    
    /* ---------------------------------------------------------------------- */
    /* toJson:                                                                */
    /* ---------------------------------------------------------------------- */
    public String toJson()
    {
        return TapisGsonUtils.getGson(true).toJson(this);
    }
    
    /* ---------------------------------------------------------------------- */
    /* toYaml:                                                                */
    /* ---------------------------------------------------------------------- */
    public String toYaml()
    {
        // Set output options.
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        options.setExplicitStart(true);
        options.setExplicitEnd(true);
        
        // Convert to json first and then to yaml.
        var yaml = new Yaml(options);
        Map<String, Object> map = (Map<String, Object>) yaml.load(toJson());
        return yaml.dumpAsMap(map);
    }
    
    /* ---------------------------------------------------------------------- */
    /* toText:                                                                */
    /* ---------------------------------------------------------------------- */
    public String toText()
    {
        // Hand build the output.
        var buf = new StringBuilder(1024);
        buf.append("----------- Summary -----------\n");
        buf.append("Secrets created:     ");
        buf.append(secretsCreated);
        buf.append("\n");
        buf.append("Secrets updated:     ");
        buf.append(secretsUpdated);
        buf.append("\n");
        buf.append("Secrets deployed     ");
        buf.append(secretsDeployed);
        buf.append("\n");
        buf.append("Secrets skipped:     ");
        buf.append(secretsSkipped);
        buf.append("\n");
        buf.append("Secrets failed:      ");
        buf.append(secretsFailed);
        buf.append("\n");
        buf.append("Keypairs generated:  ");
        buf.append(keyPairsGenerated);
        buf.append("\n");
        buf.append("Passwords generated: ");
        buf.append(passwordsGenerated);
        buf.append("\n\n");
        buf.append("----------- Details -----------\n");
        buf.append("--> DB Credenatials:\n");
        var it = dbCredentialMsgs.iterator();
        while (it.hasNext()) {buf.append("   "); buf.append(it.next()); buf.append("\n");}
        buf.append("\n");
        buf.append("--> JWT Signing Keys:\n");
        it = jwtSigningMsgs.iterator();
        while (it.hasNext()) {buf.append("   "); buf.append(it.next()); buf.append("\n");}
        buf.append("\n");
        buf.append("--> Service Passwords:\n");
        it = servicePwdMsgs.iterator();
        while (it.hasNext()) {buf.append("   "); buf.append(it.next()); buf.append("\n");}
        buf.append("\n");
        buf.append("--> User Secrets:\n");
        it = userMsgs.iterator();
        while (it.hasNext()) {buf.append("   "); buf.append(it.next()); buf.append("\n");}
        buf.append("\n");
        
        return buf.toString();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    private void addMessage(SecretType type, String message)
    {
        // Record the message.
        switch (type) {
            case DBCredential:
                dbCredentialMsgs.add(message);
                break;
            case JWTSigning:
                jwtSigningMsgs.add(message);
                break;
            case ServicePwd:
                servicePwdMsgs.add(message);
                break;
            case User:
                userMsgs.add(message);
                break;
        }
    }
}
