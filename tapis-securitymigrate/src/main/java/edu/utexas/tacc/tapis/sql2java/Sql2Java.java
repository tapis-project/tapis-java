package edu.utexas.tacc.tapis.sql2java;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;
import edu.utexas.tacc.tapis.sql2java.utils.Sql2JavaUtils;

/** The goal of this utility is to generate Postgresql artifacts from an existing
 * schema that can be easily incorporated into Tapis.  For example, by invoking
 * this program with no parameters, one creates an entity class, a SQL statements
 * class and a prototype DAO class for each table in the public schema in the default
 * Postgres database running on local machine.  The output is placed in the sql2java
 * subdirectory of the user's home directory.  See Sql2JavaParameters for the default
 * values. 
 * 
 * The generated Java code is intended as a starting point for production code.
 * The code generated for the entity class does not include its setter methods,
 * but this should not present a problem since accessors can be easily generated 
 * using Eclipse or IntelliJ.  Once the entity class setter methods are generated,
 * the code should compile.
 * 
 * It's expected that the 3 classes generated for each table found in the database
 * schema will be moved to their permanent package and modified as needed.  Use
 * the -o command line parameter to choose the output directory or accept the 
 * current directory as the default output directory.
 * 
 * @author rcardone
 */
public final class Sql2Java 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(Sql2Java.class);
    
    // Initial list capacities.
    private static final int TABLE_LEN  = 70;
    private static final int COLUMN_LEN = 700;
    
    // Placeholder strings in template files that get replaced.
    // The leading backslash escapes the brace for use in a regex.
    private static final String PH_CLASS_NAME = "\\{%ClassName}";
    private static final String PH_UPPER_CLASS_NAME = "\\{%UpperClassName}";
    private static final String PH_PACKAGE_NAME = "\\{%PkgName}";
    private static final String PH_FIELD_ASSIGNMENTS = "\\{%FieldAssignments}";
    private static final String PH_DAO_UUID_FRAGMENT = "\\{%DaoUuidFragment}";
    
    // Template file and directory names.
    private static final String DAO_TEMPLATE_FILE = "DaoTemplate.java";
    private static final String DAO_UUID_FRAGMENT_FILE = "DaoUuidFragment.java";
    private static final String TEMPLATE_DIR = "templates";
 
    // -----------------------------------------------------------------------------
    // ------------ Modify Configuration Constants Below Here as Needed ------------
    // -----------------------------------------------------------------------------
    // Hikari configuration parameters.
    private static final int     MAX_POOL_SIZE = 2;
    
    // Schema selection statement components.
    private static final String SCHEMA_SELECT_PREFIX = 
       "SELECT table_name, ordinal_position, column_name, udt_name, is_nullable " + 
       "FROM information_schema.columns " +
       "WHERE table_schema = 'public' ";
            
    private static final String SCHEMA_SELECT_SUFFIX = 
       "ORDER BY table_name, ordinal_position";
    
    // The default package named used in generated files.
    private static final String PKG_PREFIX = "edu.utexas.tacc.tapis";
    
    // The number of spaces to use for indentation in generated code. 
    private static final int TAB_SIZE = 4;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Input parameters.
    private final Sql2JavaParameters _parms;
    
    // Statistics.
    private int _numTables;
    private int _numColumns;
    
    // Template code.
    private String _daoTemplate;
    private String _daoUuidFragment;
    
    // Indentation string to be initialized to tab size.
    private String _indent = "";
    private String _indent2;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public Sql2Java(Sql2JavaParameters parms){_parms = parms;}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) throws Exception 
    {
        Sql2JavaParameters parms = new Sql2JavaParameters(args);
        Sql2Java s2j = new Sql2Java(parms);
        s2j.generate();
    }

    /* ---------------------------------------------------------------------- */
    /* generate:                                                              */
    /* ---------------------------------------------------------------------- */
    public void generate() throws Exception
    {
        // Print output directory.
        File outDir = new File(_parms.outDir);
        _log.info("\n\nOutput directory:  " + outDir.getAbsolutePath() + 
                  "\nOutput package:    " + PKG_PREFIX);
        
        // Initialize the database connection pool.
        HikariDataSource ds = getDataSource();
        
        // Query the database for tables of interest.
        List<TableInfo> tableInfoList = getSchemaInfo(ds);
        
        // Close the database connections.
        ds.close();
        
        // Generate the prototype Java classes for each table.
        List<GeneratedStrings> genStringList = generateCode(tableInfoList);
        
        // Read in the template files.
        readTemplates();
        
        // Output the code.
        writeCode(genStringList, outDir);
        
        // Print summary information.
        _log.info("\n\nNumber of tables processed:  " + _numTables +
                  "\nNumber of columns processed: " + _numColumns);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDataSource:                                                         */
    /* ---------------------------------------------------------------------- */
    private HikariDataSource getDataSource()
    {
        // Get an Hikari data source using the no-args constructor.
        HikariDSGenerator dsgen = new HikariDSGenerator();
        HikariDataSource ds = dsgen.getDataSource();
        ds.setPoolName("testDS-pool");
        ds.setMaximumPoolSize(MAX_POOL_SIZE);
        ds.setJdbcUrl(getJdbcUrl());
        ds.addDataSourceProperty("user", _parms.username);
        ds.addDataSourceProperty("password", _parms.password);
        
        // Customize connections.
        dsgen.setReliabilityOptions(ds);
        
        // Return the datasource.
        return ds;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSchemaInfo:                                                         */
    /* ---------------------------------------------------------------------- */
    private List<TableInfo> getSchemaInfo(HikariDataSource ds)
     throws Exception
    {
        // ------------------------- Call SQL ----------------------------
        ArrayList<ColumnInfo> columnList = new ArrayList<>(COLUMN_LEN);
        Connection conn = null;
        try
        {
          // Create the query command using table definition field order.
          // NOTE: Any changes to the job_queues table requires maintenance
          //       here and in the populate routine below.
          String sql = getSelectStatement();
          
          // Get a database connection.
          conn = ds.getConnection();

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
                      
          // Issue the call for the row result set.
          ResultSet rs = pstmt.executeQuery();
          
          // Create a queue object for each row returned.
          ColumnInfo tableInfo = populateTableInfo(rs);
          while (tableInfo != null) {
            columnList.add(tableInfo);
            tableInfo = populateTableInfo(rs);
          }
          
          // Close the result and statement.
          rs.close();
          pstmt.close();
    
          // Commit the transaction.
          conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error("Rollback failed", e1);}
            
            String msg = "Schema query failed: " + e.getMessage();
            _log.error(msg, e);
            throw new Exception(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
            try {if (conn != null) conn.close();}
              catch (Exception e) 
              {
                // If commit worked, we can swallow the exception.  
                // If not, the commit exception will be thrown.
                String msg = "Connection close failed: " + e.getMessage();
                _log.error(msg, e);
              }
        }
        
        // Group the column objects according to table.
        List<TableInfo> tableList = getTableList(columnList);
        
        // Set the statics.
        _numTables  = tableList.size();
        _numColumns = columnList.size();
        
        return tableList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSelectStatement:                                                    */
    /* ---------------------------------------------------------------------- */
    private String getSelectStatement()
    {
        // Construct the select statement.
        String select = SCHEMA_SELECT_PREFIX;
        
        // Add table exclusions to the where clause. Make sure
        // there's a trailing space after each exclusion clause.
        String notLike = "";
        if (!StringUtils.isBlank(_parms.excludeTables)) {
            String[] tables = _parms.excludeTables.split(",");
            for (String table : tables)
                notLike += "AND table_name NOT LIKE '" + table.trim() + "' ";
        }
        
        // Complete the statement to give sorted results.
        select += notLike + SCHEMA_SELECT_SUFFIX;
        return select;
    }
    
    /* ---------------------------------------------------------------------- */
    /* populateTableInfo:                                                     */
    /* ---------------------------------------------------------------------- */
    private ColumnInfo populateTableInfo(ResultSet rs) throws Exception
    {
        // Quick check.
        if (rs == null) return null;
        
        try {
          // Return null if the results are empty or exhausted.
          // This call advances the cursor.
          if (!rs.next()) return null;
        }
        catch (Exception e) {
          String msg = "Cursor read error: " + e.getMessage();
          _log.error(msg, e);
          throw new Exception(msg, e);
        }
        
        // Populate the queue object using select statement's field order.
        ColumnInfo tableInfo = new ColumnInfo();
        try {
            tableInfo.setTableName(rs.getString(1));
            tableInfo.setOrdinalPosition(rs.getLong(2));
            tableInfo.setColumnName(rs.getString(3));
            tableInfo.setDataType(rs.getString(4));
            String isNullable = rs.getString(5);
            if (isNullable.toUpperCase().startsWith("Y")) tableInfo.setNullable(true);
        } 
        catch (Exception e) {
          String msg = "Database field type cast error: " + e.getMessage();
          _log.error(msg, e);
          throw new Exception(msg, e);
        }
          
        return tableInfo;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getTableList:                                                          */
    /* ---------------------------------------------------------------------- */
    private List<TableInfo> getTableList(List<ColumnInfo> columnList)
    {
        // Create the table list.
        ArrayList<TableInfo> tableList = new ArrayList<>(TABLE_LEN);
        
        // Iterate through the list of column info objects.
        TableInfo curTableInfo = null;
        for (ColumnInfo columnInfo : columnList) {
            
            // The column records are grouped by table, so any time we see
            // a column belonging with a new table, we can create a new
            // table info object and start filling it in.
            if (curTableInfo == null || 
                !columnInfo.getTableName().equals(curTableInfo.tableName)) 
            {
                // Create the new table info object and add it to the result list.
                curTableInfo = new TableInfo(columnInfo.getTableName());
                tableList.add(curTableInfo);
            }
            
            // Add the column object to the current table object.
            curTableInfo.columnList.add(columnInfo);
        }
        
        return tableList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateCode:                                                          */
    /* ---------------------------------------------------------------------- */
    private List<GeneratedStrings> generateCode(List<TableInfo> tableInfoList)
    {
        // Create the indentation string of the configured size.
        for (int i = 0; i < TAB_SIZE; i++) _indent += " ";
        _indent2 = _indent + _indent;
        
        // Create the output list.
        ArrayList<GeneratedStrings> genList = new ArrayList<>(tableInfoList.size());
        
        // Generate table output table by table.
        for (TableInfo tableInfo : tableInfoList) {
            
            // Create a container for intermediate results.
            GeneratedStrings genStrings = new GeneratedStrings(tableInfo.tableName);
        
            // Create the select statements.
            createSelectStatements(tableInfo, genStrings);
            
            // Generate the entity class source code.
            createEntityClass(tableInfo, genStrings);
            
            // Generate the field assignment code used in populate methods of DAOs.
            // See DaoTemplate.java resource file for details.
            createFieldAssignments(tableInfo, genStrings);
            
            // Add the newly generated strings to the result list.
            genList.add(genStrings);
        }
        
        return genList;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createSelectStatements:                                                */
    /* ---------------------------------------------------------------------- */
    private void createSelectStatements(TableInfo tableInfo, 
                                        GeneratedStrings genStrings)
    {
        // Record whether there's a uuid field.
        String selectAll = null;
        String selectOne = null;
        
        // ------------------ Select All ------------------
        // Create the select all statement.
        StringBuilder buf = new StringBuilder();
        buf.append(_indent2);
        buf.append("\"SELECT");
        
        // Add column names in ordinal order.
        for (int i = 0; i < tableInfo.columnList.size(); i++) {
            ColumnInfo columnInfo = tableInfo.columnList.get(i);
            buf.append(" ");
            buf.append(columnInfo.getColumnName());
            
            // No comma after last column name.
            if (i + 1 < tableInfo.columnList.size()) 
                buf.append(",");
            
            // Note the uuid column.
            if ("uuid".equals(columnInfo.getColumnName())) genStrings.hasUuid = true;
        }
        
        // Finish the statement.
        buf.append("\"\n");
        buf.append(_indent2);
        buf.append("+ \" FROM ");
        buf.append(tableInfo.tableName);
        buf.append("\"");
        selectAll = buf.toString();
        
        // ------------------ Select One ------------------
        // Create a select statement that will return at most 1 row.
        if (genStrings.hasUuid) {
            buf.append("\n");
            buf.append(_indent2);
            buf.append("+ \" WHERE uuid = ?\"");
            selectOne = buf.toString();
        }
        
        // ------------------ SqlStatments Class ----------
        // Create a new buffer, replacing the old one.
        buf = new StringBuilder();
        
        // Class package statement.
        buf.append("package ");
        buf.append(Sql2JavaUtils.getPackageName(PKG_PREFIX, tableInfo.tableName));
        buf.append(";\n\n");
        
        // Class definition.
        buf.append("public final class SqlStatements\n{\n");
        
        // Select all sql statement.
        buf.append(_indent);
        buf.append("public static final String SELECT_");
        buf.append(Sql2JavaUtils.getUpperCaseClassName(tableInfo.tableName));
        buf.append(" =\n");
        buf.append(selectAll);
        buf.append(";\n");
        
        // Select one sql statement.
        if (selectOne != null) {
            buf.append("\n");
            buf.append(_indent);
            buf.append("public static final String SELECT_");
            buf.append(Sql2JavaUtils.getUpperCaseClassName(tableInfo.tableName));
            buf.append("_BY_UUID =\n");
            buf.append(selectOne);
            buf.append(";\n");
        }
        
        // Close class.
        buf.append("}\n");
        genStrings.sqlStatement = buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* createEntityClass:                                                     */
    /* ---------------------------------------------------------------------- */
    private void createEntityClass(TableInfo tableInfo, 
                                   GeneratedStrings genStrings)
    {
        // Flag set during processing.
        boolean hasInstant = false;
        
        // Fill in the class name for this table.
        genStrings.className = Sql2JavaUtils.getClassName(tableInfo.tableName);
        
        // ----------------- Class body -----------------
        // Open class declaration.
        StringBuilder buf = new StringBuilder();
        buf.append("public final class ");
        buf.append(genStrings.className);
        buf.append("\n{\n");
        
        // Field definitions.
        for (ColumnInfo columnInfo : tableInfo.columnList) {
            buf.append(_indent);
            buf.append("private ");
            
            // Get the java type associated with the db type.
            String javaType = Sql2JavaUtils.getJavaFieldType(columnInfo.getDataType(), 
                                                             columnInfo.isNullable());
            if ("Instant".equals(javaType)) hasInstant = true;
            
            // Complete this field's definition.
            buf.append(javaType);
            buf.append(" ");
            buf.append(Sql2JavaUtils.spaces(7, javaType)); // the length of the largest java type
            buf.append(Sql2JavaUtils.getCamelCaseFieldName(columnInfo.getColumnName()));
            buf.append(";\n");
        }
        
        // Customize the print routine.
        buf.append("\n");
        buf.append(_indent);
        buf.append("@Override\n");
        buf.append(_indent);
        buf.append("public String toString() {return TapisUtils.toString(this);}\n");
        
        // Close class declaration.
        buf.append("}\n");
        
        // ----------------- Class header ---------------
        // Class package statement.
        StringBuilder hdr = new StringBuilder();
        hdr.append("package ");
        hdr.append(Sql2JavaUtils.getPackageName(PKG_PREFIX, tableInfo.tableName));
        hdr.append(";\n\n");
        
        // Import statements.
        if (hasInstant)
            hdr.append("import java.time.Instant;\n");
        hdr.append("import edu.utexas.tacc.tapis.shared.utils.TapisUtils;\n");
        hdr.append("\n");
        
        // Save the complete class definition.
        genStrings.entityCode = hdr.toString() + buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* createFieldAssignments:                                                */
    /* ---------------------------------------------------------------------- */
    private void createFieldAssignments(TableInfo tableInfo, 
                                        GeneratedStrings genStrings)
    {
        // Result accumulator.
        StringBuilder buf = new StringBuilder();
        
        // Bookkeeping for scratch object creation.
        boolean tsVariableCreated = false;
        
        // Field definitions.
        for (ColumnInfo columnInfo : tableInfo.columnList) {
            
            // Get the name of the entity field setter method that will be called.
            String setter = Sql2JavaUtils.getFieldSetter(columnInfo.getColumnName());
            
            // Get the result set conversion method name.
            String conMethod = Sql2JavaUtils.getConversionMethod(columnInfo.getDataType(),
                                                                 columnInfo.isNullable());
            
            // Get the optional second method required to convert timestamps to Instants.
            // If the conversion method indicates this column is a timestamp, then the
            // the conversion method with a leading period is return.  Otherwise, an empty
            // string is returned.
            String conTsMethod = Sql2JavaUtils.getTimestampConversionMethod(conMethod);
            
            // Determine if we need to create two separate statements
            // to protect against null pointer exceptions.
            if (!conTsMethod.isEmpty() && columnInfo.isNullable()) {
                
                // Indent first statement.
                buf.append("\n");
                buf.append(_indent2);
                
                // One-time declaration of timestamp variable.
                if (!tsVariableCreated) {
                    buf.append("Timestamp ");
                    tsVariableCreated = true;
                }
                
                // 1st statement.
                buf.append("ts = rs.");
                buf.append(conMethod);
                buf.append("(");
                buf.append(columnInfo.getOrdinalPosition());
                buf.append(");\n");
                
                // 2nd statement.
                buf.append(_indent2);
                buf.append("if (ts != null) obj.");
                buf.append(setter);
                buf.append("(");
                buf.append("ts");
                buf.append(conTsMethod);
                buf.append(")"); 
                buf.append(";\n\n");
            }
            else {
                // Write this columns assignment line to the buffer. Note that the 
                // "obj" and "rs" objects are defined in the DaoTemplate.java resources
                // into which this text is ultimately merged.
                buf.append(_indent2);
                buf.append("obj.");
                buf.append(setter);
                buf.append("(");
                buf.append("rs.");
                buf.append(conMethod);
                buf.append("(");
                buf.append(columnInfo.getOrdinalPosition());
                buf.append(")");
                buf.append(conTsMethod); // Empty except for timestamps
                buf.append(")");
                buf.append(";\n");
            }
        }
        
        genStrings.fieldAssignmentCode = buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* readTemplates:                                                         */
    /* ---------------------------------------------------------------------- */
    private void readTemplates() 
     throws IOException
    {
        // ---------- Read the base template file.
        String baseTemplate = TEMPLATE_DIR + "/" + DAO_TEMPLATE_FILE;
        InputStream ins = getClass().getClassLoader().getResourceAsStream(baseTemplate);
        if (ins == null) {
            String msg = "Null input stream returned when reading resource " + baseTemplate + ".";
            _log.error(msg);
            throw new IOException(msg);
        }
        
        // Put the file content into a string field.
        try {_daoTemplate = IOUtils.toString(ins, StandardCharsets.UTF_8);}
        catch (IOException e) {
            String msg = "Unable to read template from " + baseTemplate + ".";
            _log.error(msg, e);
            throw e;
        }
        finally {
            if (ins != null) try {ins.close();} catch (Exception e) {}
        }
        
        // ---------- Read the fragment template file.
        String fragment = TEMPLATE_DIR + "/" + DAO_UUID_FRAGMENT_FILE;
        ins = getClass().getClassLoader().getResourceAsStream(fragment);
        if (ins == null) {
            String msg = "Null input stream returned when reading resource " + fragment + ".";
            _log.error(msg);
            throw new IOException(msg);
        }
        
        // Put the file content into a string field.
        try {_daoUuidFragment = IOUtils.toString(ins, StandardCharsets.UTF_8);}
        catch (IOException e) {
            String msg = "Unable to read template from " + fragment + ".";
            _log.error(msg, e);
            throw e;
        }
        finally {
            if (ins != null) try {ins.close();} catch (Exception e) {}
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* writeCode:                                                             */
    /* ---------------------------------------------------------------------- */
    private void writeCode(List<GeneratedStrings> genStringList, File baseDir) 
     throws IOException
    {
        // Write each table's files to its own subdirectory.
        for (GeneratedStrings genStrings : genStringList) {
            
            // Create the table subdirectory.
            String pkgDir = PKG_PREFIX.replace(".", "/") + "/" + genStrings.tableName;
            File outDir = new File(baseDir, pkgDir);
            outDir.mkdirs();
            
            // ---- Write the sql statements file.
            File sqlFile = new File(outDir, "SqlStatements.java");
            try {
                FileUtils.writeStringToFile(sqlFile, genStrings.sqlStatement, 
                                            StandardCharsets.UTF_8);
            } catch (IOException e) {
                String msg = "Unable to write to file " + sqlFile.getAbsolutePath() + ".";
                _log.error(msg, e);
                throw e;
            }
            
            // ---- Write the entity file.
            File entityFile = new File(outDir, genStrings.className + ".java");
            try {
                FileUtils.writeStringToFile(entityFile, genStrings.entityCode, 
                                            StandardCharsets.UTF_8);
            } catch (IOException e) {
                String msg = "Unable to write to file " + entityFile.getAbsolutePath() + ".";
                _log.error(msg, e);
                throw e;
            }
            
            // ---- Replace DAO placeholders.
            // Create a local reference to the DAO template.
            String daoTemplate = _daoTemplate;
            
            // Conditionally merge fragment template into the DAO base template.  If
            // the fragment is needed, replace the placeholder with the empty string.
            if (genStrings.hasUuid) 
                daoTemplate = daoTemplate.replaceAll(PH_DAO_UUID_FRAGMENT, _daoUuidFragment);
              else
                daoTemplate = daoTemplate.replaceAll(PH_DAO_UUID_FRAGMENT, "");
            
            // Replace the remaining placeholder strings in the DAO.
            daoTemplate = daoTemplate.replaceAll(
                            PH_PACKAGE_NAME, 
                            Sql2JavaUtils.getPackageName(PKG_PREFIX, genStrings.tableName));
            daoTemplate = daoTemplate.replaceAll(PH_CLASS_NAME, genStrings.className);
            daoTemplate = daoTemplate.replaceAll(PH_UPPER_CLASS_NAME, genStrings.className.toUpperCase());
            daoTemplate = daoTemplate.replaceAll(PH_FIELD_ASSIGNMENTS, genStrings.fieldAssignmentCode);
            
            // ---- Write the DAO file.
            File daoFile = new File(outDir, genStrings.className + "Dao.java");
            try {
                FileUtils.writeStringToFile(daoFile, daoTemplate, StandardCharsets.UTF_8);
            } catch (IOException e) {
                String msg = "Unable to write to file " + daoFile.getAbsolutePath() + ".";
                _log.error(msg, e);
                throw e;
            }
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getJdbcUrl:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Create a JDBC data source url using the execution input parameters for any 
     * supported SQL database.
     * 
     * @return a url string
     */
    private String getJdbcUrl()
    {
      // This string depends on user input for most components.
      return "jdbc:" + _parms.dbmsName + "://" + _parms.host + ":" + 
             _parms.port + "/" + _parms.dbName;
    }
    
    /* ********************************************************************** */
    /*                            TableInfo Class                             */
    /* ********************************************************************** */
    /** This class holds all pertinent column information for a table. */
    private static final class TableInfo
    {
        // Fields
        private String           tableName;
        private List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        
        // Constructor
        private TableInfo(String tableName) {this.tableName = tableName;}
        
        @Override
        public String toString() {return TapisUtils.toString(this);}
    }
    
    /* ********************************************************************** */
    /*                         GeneratedStrings Class                         */
    /* ********************************************************************** */
    /** This class holds all pertinent column information for a table. */
    private static final class GeneratedStrings
    {
        // Fields
        private String  tableName;
        private String  className;
        private String  sqlStatement;
        private String  entityCode;
        private String  fieldAssignmentCode;
        private boolean hasUuid;
        
        // Constructor
        private GeneratedStrings(String tableName) {this.tableName = tableName;}
    }
}
