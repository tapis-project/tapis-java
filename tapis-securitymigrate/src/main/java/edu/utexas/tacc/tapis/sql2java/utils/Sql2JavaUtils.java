package edu.utexas.tacc.tapis.sql2java.utils;

import java.util.HashMap;

import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.ComparableRecursiveToStringStyle;

public final class Sql2JavaUtils 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(Sql2JavaUtils.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Conversion mappings from DB data types to Java data types.
    private static final HashMap<String,String> _nonNullDbTypeMap = initNonNullDbTypeMap();
    private static final HashMap<String,String> _nullDbTypeMap = initNullDbTypeMap();
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getCamelCaseFieldName:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Convert a db field name that contains underscores to a camel case
     * name appropriate for the name of a Java field.  For example,
     * 
     *      tenant_id --> tenantId
     *      
     * This method assumes no leading or trailing underscores.
     * 
     * @param fldName a name that may contain underscores
     * @return a camel case transformation of the name
     */
    public static String getCamelCaseFieldName(String fldName)
    {
        // Iterate through the field's characters.
        StringBuilder buf = new StringBuilder();
        boolean underscoreFound = false;
        for (char c : fldName.toCharArray()) {
            
            // If the last char found was an underscore,
            // uppercase the current char and iterate.
            if (underscoreFound) {
                underscoreFound = false;
                buf.append(Character.toUpperCase(c));
                continue;
            }
            
            // Slip underscores and set up uppercasing.
            if (c == '_') {
                underscoreFound = true;
                continue;
            }
            
            // No transformation.
            buf.append(c);
        }
        
        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* getClassName:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Convert a db table name that may contain underscores to an appropriate
     * name for a Java class.  For example,
     * 
     *      logical_files --> LogicalFiles
     *      
     * This method assumes no leading or trailing underscores.
     * 
     * @param tblName a DB table name that may contain underscores
     * @return a Java class name
     */
    public static String getClassName(String tblName)
    {
        // First convert to camel case then uppercase the first character.
        String clsName = getCamelCaseFieldName(tblName);
        return Character.toUpperCase(clsName.charAt(0)) + clsName.substring(1);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getUpperCaseClassName:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Convert a db table name that may contain underscores to an appropriate
     * name for a Java class that's been uppercased.  For example,
     * 
     *      logical_files --> LOGICALFILES
     *      
     * This method assumes no leading or trailing underscores.
     * 
     * @param tblName a DB table name that may contain underscores
     * @return a Java class name uppercased
     */
    public static String getUpperCaseClassName(String tblName)
    {
        // First convert to camel case then uppercase the first character.
        String clsName = getCamelCaseFieldName(tblName);
        return clsName.toUpperCase();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getPackageName:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Create the package name from the fixed prefix and the table name.
     * 
     * @param prefix the package prefix string
     * @param tblName the DB table name
     * @return a package string using and transforming the inputs
     */
    public static String getPackageName(String prefix, String tblName)
    {
        // First convert to camel case then uppercase the first character.
        String pkgName = getCamelCaseFieldName(tblName);
        return prefix + "." + pkgName.toLowerCase();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getFieldSetter:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Create the set method name for this DB field.
     * 
     * @param fldName the DB field name
     * @return the setter method name
     */
    public static String getFieldSetter(String fldName)
    {
        String javaName = getCamelCaseFieldName(fldName);
        return "set" + Character.toUpperCase(javaName.charAt(0)) + javaName.substring(1);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJavaFieldType:                                                      */
    /* ---------------------------------------------------------------------- */
    public static String getJavaFieldType(String dbType, boolean isNullable)
    {
        // Choose the Java type based on the db null attribute.
        if (isNullable) {
            String javaType = _nullDbTypeMap.get(dbType);
            if (javaType == null) return "String";
              else return javaType;
        } else {
            String javaType = _nonNullDbTypeMap.get(dbType);
            if (javaType == null) return "String";
              else return javaType;
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getConversionMethod:                                                   */
    /* ---------------------------------------------------------------------- */
    public static String getConversionMethod(String dbType, boolean isNullable)
    {
        // Get the target Java type.
        String javaType = getJavaFieldType(dbType, isNullable);
        
        String conversionMethod = null;
        switch (javaType)
        {
            case "String":
                conversionMethod = "getString";
                break;
                
            case "boolean":
                conversionMethod = "getBoolean";
                break;
                
            case "Instant":
                conversionMethod = "getTimestamp";
                break;
                
            case "int":
            case "Integer":
                conversionMethod = "getInt";
                break;
                
            case "long":
            case "Long":
                conversionMethod = "getLong";
                break;
                
            case "double":
            case "Double":
                conversionMethod = "getDouble";
                break;
                
            case "float":
            case "Float":
                conversionMethod = "getFloat";
                break;
                
           default:
               conversionMethod = "getString";
               _log.warn("*** Assigned unexpected Java type " + javaType + 
                         " the getString conversion by default.");
               break;
        }
        
        return conversionMethod;
    }

    /* ---------------------------------------------------------------------- */
    /* getTimestampConversionMethod:                                          */
    /* ---------------------------------------------------------------------- */
    public static String getTimestampConversionMethod(String conversionMethod)
    {
        // Only timestamps need to tack on another method call.
        if (conversionMethod.equals("getTimestamp")) return ".toInstant()";
          else return "";
    }

    /* ********************************************************************** */
    /*                             PrivateMethods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initNonNullDbTypeMap:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Map of DB data types to Java data types when the DB value is NOT NULL.
     * Any missing mapping should be interpreted as resulting in a Java String. 
     * 
     * @return the Java type or null for String
     */
    private static HashMap<String,String>initNonNullDbTypeMap()
    {
        // Assign the db to java type conversions when the db field cannot be null.
        HashMap<String,String> map = new HashMap<String,String>(29);
        
        // We always assume tinyints are booleans.
        // DB types not list here are assumed to
        // convert to Java String, including char, 
        // varchar, text and enum.
        map.put("timestamp", "Instant");
        map.put("datetime", "Instant");
        map.put("double", "double");
        map.put("float", "float");
        map.put("bigint", "long");
        map.put("bigserial", "long");
        map.put("int8", "long");
        map.put("int", "int");
        map.put("int4", "int");
        map.put("serial", "int");
        map.put("serial4", "int");
        map.put("smallint", "short");
        map.put("smallserial", "short");
        map.put("int2", "short");
        map.put("int1", "byte");
        map.put("boolean", "boolean");
        map.put("bit", "boolean");
        map.put("tinyint", "boolean");
        
        return map;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initNullDbTypeMap:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Map of DB data types to Java data types when the DB value can be NULL.
     * Any missing mapping should be interpreted as resulting in a Java String. 
     * 
     * @return the Java type or null for String
     */
    private static HashMap<String,String>initNullDbTypeMap()
    {
        // Assign the db to java type conversions when the db field cannot be null.
        HashMap<String,String> map = new HashMap<String,String>(29);
        
        // We always assume tinyints are booleans.
        // DB types not list here are assumed to
        // convert to Java String, including char, 
        // varchar, text and enum.
        map.put("timestamp", "Instant");
        map.put("datetime", "Instant");
        map.put("double", "Double");
        map.put("float", "Float");
        map.put("bigint", "Long");
        map.put("bigserial", "Long");
        map.put("int8", "Long");
        map.put("int", "Integer");
        map.put("int4", "Integer");
        map.put("serial", "Integer");
        map.put("serial4", "Integer");
        map.put("smallint", "Short");
        map.put("smallserial", "Short");
        map.put("int2", "Short");
        map.put("int1", "Byte");
        map.put("boolean", "Boolean");
        map.put("bit", "Boolean");
        map.put("tinyint", "Boolean");
        
        return map;
    }
    
    /* ---------------------------------------------------------------------- */
    /* spaces:                                                                */
    /* ---------------------------------------------------------------------- */
    /** Return a string of spaces that would concatenated with the pad string
     * will result in a string of len total.
     * 
     * @param total the target length of the padded string
     * @param padString the string to be padded
     * @return a string of spaces to be used as padding
     */
    public static String spaces(int total, String padString)
    {
       if (padString.length() >= total) return "";
       int padLen = total - padString.length();
       String s = "";
       for (int i = 0; i < padLen; i++) s += " ";
       return s;
    }
}
