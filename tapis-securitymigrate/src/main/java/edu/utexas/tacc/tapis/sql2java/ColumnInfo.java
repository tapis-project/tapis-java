package edu.utexas.tacc.tapis.sql2java;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** This class holds all the information need to generate code for a particular
 * column in a particular table.
 * 
 * @author rcardone
 */
final class ColumnInfo 
{
    // table_name, ordinal_position, column_name, data_type, is_nullable
    private String  tableName;
    private long    ordinalPosition;
    private String  columnName;
    private String  dataType;
    private boolean isNullable;
    
    // Accessors
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public long getOrdinalPosition() {
        return ordinalPosition;
    }
    public void setOrdinalPosition(long ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }
    public String getColumnName() {
        return columnName;
    }
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    public String getDataType() {
        return dataType;
    }
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    public boolean isNullable() {
        return isNullable;
    }
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
    
    @Override
    public String toString() {return TapisUtils.toString(this);}
}
