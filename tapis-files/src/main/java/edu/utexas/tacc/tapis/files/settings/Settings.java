package edu.utexas.tacc.tapis.files.settings;

import java.util.Map;
import java.util.Properties;



/*

 */
public class Settings {


    private Map<String, String> envars;
    private Properties properties;
    private Properties config;
    private Settings INSTANCE;

    private void aggregateSettings() {

    }


    public static String get(String key) {
        Settings settings = new Settings();
        settings.aggregateSettings();
        return settings.config.get(key).toString();
    }
}
