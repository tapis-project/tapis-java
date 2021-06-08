package edu.utexas.tacc.tapis.jobs.api.driver;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class TestUtils 
{
    // The profiles are always in the $HOME/TapisProfiles directory.
    // The profiles contain the BASE_URL and USER_JWT key/value pairs.
    public static String PROFILE_DIRECTORY = "TapisProfiles";
    
    // Profile file name extension.
    public static String PROFILE_EXT = ".properties";
    
    /** Load the contents of the properties file into memory.
     * 
     * @param profileName the file name without extension
     * @return the populated properties file
     * @throws IOException on error
     */
    public static Properties getTestProfile(String profileName) throws IOException
    {
        // Create the path to the profile and read the file into a string.
        // The absolute path is:  /<user.home>/TapisProfiles/<profileName>.properties.
        Path reqFile = Path.of(System.getProperty("user.home"), PROFILE_DIRECTORY, profileName+PROFILE_EXT);
        String reqString = Files.readString(reqFile);
        
        // Populate and return the properties.
        var properties = new Properties();
        properties.load(new StringReader(reqString));
        return properties;
    }
    
    /** Return the template for the profile path. */
    public static String getProfilePathTemplate()
    {
        return "$HOME/" + PROFILE_DIRECTORY + "/<profileName>.properties";
    }
}
