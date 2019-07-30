package edu.utexas.tacc.tapis.parameters;

import java.util.Map;
import java.util.Properties;

/*
This class holds all the settings for the web services. This is implemented as a singleton
so that the aggregation of the JVM settings and environment variables only has to happen once.
Environment variables are overridden by JVM settings.
 */
public class Settings {

    private Map<String, String> envars;
    private Properties properties;
    private Properties config = new Properties();
    private static Settings INSTANCE;

    public Settings() {
        envars = System.getenv();
        properties = System.getProperties();
    }

    private void aggregateSettings() {
        config.putAll(envars);
        config.putAll(properties);
    }

    public static Settings getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new Settings();
            INSTANCE.aggregateSettings();
        }
        return INSTANCE;
    }

    public static String get(String key) {
        Settings instance = getInstance();
        String val = instance.config.getOrDefault(key, "").toString();
        return val;
    }

    /** Retrieve the environment variable as a boolean.
     *
     * @param key the environment variable key
     * @return the key's value or false if the value doesn't exist or is invalid.
     */
    public static boolean getBoolean(String key)
    {
        if (key == null) return false;
        String s = get(key);
        if (s == null) return false;
        try {return Boolean.valueOf(s);}
        catch (Exception  e) {
            return false;
        }
    }

    /** Retrieve the environment variable as an Integer.
     *
     * @param key the environment variable key
     * @return the key's value or null if the value doesn't exist or is invalid.
     */
    public static Integer getInteger(String key)
    {
        if (key == null) return null;
        String s = get(key);
        if (s == null) return null;
        try {return Integer.valueOf(s);}
        catch (Exception  e) {
            return null;
        }
    }
}
