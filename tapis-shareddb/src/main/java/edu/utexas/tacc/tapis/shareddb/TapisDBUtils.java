package edu.utexas.tacc.tapis.shareddb;

public final class TapisDBUtils 
{
    /** Create the aloe database url with the given parameters.
     * 
     * @param host db host
     * @param port db port
     * @param database schema name
     * @return the jdbc url
     */
    public static String makeJdbcUrl(String host, int port, String database) 
    {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }
}
