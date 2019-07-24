package edu.utexas.tacc.tapis.shareddb.datasource;

import javax.sql.DataSource;

public interface DataSourceProvider 
{
    DataSource getDataSource();
}
