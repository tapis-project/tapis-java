package edu.utexas.tacc.tapis.sharedapi.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Type;
import java.security.PrivateKey;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

/** This class takes an JSON input file of JWT claims and adds a minimal number 
 * of headers, base64url encodes the header and claims and signs the encoded
 * data to make a JWT.  The JWT is written to a output file if one is specified
 * or to standard out if not.
 * 
 * There currently is no way to add additional headers to the JWT during 
 * invocation.  If and when the need arises, this class can be enhanced.
 * 
 * 
 * @author rcardone
 */
public class CreateJWT 
{
    /* ********************************************************************** */
    /*                                   Constants                            */
    /* ********************************************************************* */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(CreateJWT.class);
    
    // The signature algorithm we use.
    private static final SignatureAlgorithm signatureAlg = SignatureAlgorithm.RS256;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Command line parameters.
    private final CreateJWTParameters _parms;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public CreateJWT(CreateJWTParameters parms)
    {
        // Parameters cannot be null.
        if (parms == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "CreateJWT", "parms");
          _log.error(msg);
          throw new IllegalArgumentException(msg);
        }
        _parms = parms;
    }
    
    /* ********************************************************************** */
    /*                                Public Methods                          */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) throws Exception 
    {
        CreateJWTParameters parms = new CreateJWTParameters(args);
        CreateJWT cj = new CreateJWT(parms);
        cj.exec();
    }

    /* ---------------------------------------------------------------------- */
    /* exec:                                                                  */
    /* ---------------------------------------------------------------------- */
    /** The main driver method.
     * 
     * @throws TapisException on error
     */
    public void exec() 
     throws TapisException
    {
        // Get the claims in there order of appearance in the input file.
        Map<String,Object> claimsMap = getClaims();
        
        // Create the claims and header objects.
        Claims claims = Jwts.claims(claimsMap);
        
        // Get the signing key.
        PrivateKey privateKey = getPrivateKey();
        
        // Create the encode JWT object.
        String encodedJwt = Jwts.builder().setHeaderParam("typ", "JWT").setClaims(claims).
                              signWith(privateKey, signatureAlg).compact();
        
        // Output the encoded string.
        outputJwt(encodedJwt);
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getClaims:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Read the claims from the input JSON file into a map.  The values in the
     * map can be any valid json type.
     * 
     * @return the key/value pairs that define the claims for a JWT
     * @throws TapisException on error
     */
    private Map<String,Object> getClaims()
     throws TapisException
    {
        // Initialize the order-preserving result object.
        // The values can be any of the recognized json types.
        LinkedHashMap<String,Object> map = null;
        
        // Use gson to parse the input file contents.  The contents
        // are expected to only contain JWT claims in json format.
        Gson gson = TapisGsonUtils.getGson(true);
        try (BufferedReader rdr = new BufferedReader(new FileReader(_parms.inFilename))) {
            Type typeOfT = new TypeToken<LinkedHashMap<String,Object>>(){}.getType();
            map = gson.fromJson(rdr, typeOfT);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_PARSE_ERROR", getClass().getSimpleName(),
                                         _parms.inFilename, e.getMessage());
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        return map;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getPrivateKey:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Get the private key from the default tapis keystore associated with an 
     * alias.  Thes alias is specified as an the input parameter.
     * 
     * @return the alias's private key  
     * @throws TapisException on error
     */
    private PrivateKey getPrivateKey()
     throws TapisException
    {
        // Load a new or existing key store.
        KeyManager km = null;
        try {km = new KeyManager(null,_parms.keyStorefile);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_NO_KEYSTORE", e.getMessage());
            _log.error(msg, e);
            throw new TapisSecurityException(msg, e);
        }
        
        try {km.load(_parms.password);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_KEYSTORE_LOAD_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisSecurityException(msg, e);
        }
        
        // See if the key already exists.
        PrivateKey privk = null;
        try {privk = km.getPrivateKey(_parms.alias, _parms.password);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_GET_PRIVATE_KEY", _parms.alias, e.getMessage());
            _log.error(msg, e);
            throw new TapisSecurityException(msg, e);
        }
        
        return privk;
    }
    
    /* ---------------------------------------------------------------------- */
    /* outputJwt:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Write the encoded, signed JWT to standard out or an output file depending
     * on user specification.
     * 
     * @param encodedJwt the complete JWT
     * @throws TapisException on error
     */
    private void outputJwt(String encodedJwt) 
     throws TapisException
    {
        // Write jwt as-is to stdout if no output file was specified.
        if (StringUtils.isBlank(_parms.outFilename)) {
            System.out.print(encodedJwt);
            return;
        }
        
        // Write jwt as-is to a file.
        try (BufferedWriter wtr = new BufferedWriter(new FileWriter(_parms.outFilename))) {
            wtr.write(encodedJwt);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_IO_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
        }
    }
}
