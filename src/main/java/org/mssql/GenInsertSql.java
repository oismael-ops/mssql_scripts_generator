package org.mssql;

import java.io.*;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GenInsertSql {
    private static Connection conn=null;
    private static Statement sm=null;
    private static String schema="a_by_a_alert";
    private static String select="SELECT * FROM";
    private static String where="WHERE 1=1 ";
    private static String insert="INSERT INTO";
    private static String values="VALUES";
    private static List<String> insertList=new ArrayList<String>();
    private static String sqlfilePath =System.getenv("HOME");
    private static Boolean isOneFile = true;
    private static String filePath = sqlfilePath + "/mssql_test1.sql";
    private static String singleFilePath = sqlfilePath;
    private static String [] table={"staging_aamc.aamc"};
    private static final String URL = "jdbc:sqlserver://dbaj1.atypon.com:2019;databaseName=staging_aamc;encrypt=true;trustServerCertificate=true;";
    private static final String NAME = "restore_user";
    private static final String PASS = "Cuspod9sloib";
    private static final String DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static void createFile() {
        File file= new File( filePath );
        if (!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                System. out .println( "Could not create file" );
                e.printStackTrace();
            }
        }
        FileWriter fw= null ;
        BufferedWriter bw= null ;
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            if ( insertList .size()>0){
                for ( int i=0;i< insertList .size();i++){
                    bw.append( insertList .get(i));
                    bw.append( "\n" );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static void createFile(String filename,List <String> insertSqls) {
        File file= new File( singleFilePath+"/"+filename+".txt" );
        if (!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                System. out .println( "Could not create file" );
                e.printStackTrace();
            }
        }
        FileWriter fw= null ;
        BufferedWriter bw= null ;
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            if ( insertSqls .size()>0){
                for ( int i=0;i< insertSqls .size();i++){
                    bw.append( insertSqls .get(i));
                    bw.append( "\n" );
                }
            }
            insertList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static List<String> createSQL() {
        List<String> listSQL= new ArrayList<String>();
        for (int i=0; i < table.length ; i++){
            StringBuffer sb= new StringBuffer();
            sb.append( select ).append( " " ).append( table [i]).append( "." ).append( schema )   ;
            listSQL.add(sb.toString());
        }
        return listSQL;
    }
    public static void connectSQL(String driver,String url,String UserName,String Password){
        try {
            Class. forName (driver).newInstance();
            conn = DriverManager. getConnection (url, UserName, Password);
            sm=conn .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    @SuppressWarnings({ "unused", "rawtypes" })
    public static void executeSQL(Connection conn,Statement sm,List listSQL)throws SQLException{
        List<String> insertSQL= new ArrayList<String>();
        ResultSet rs= null ;
        try {
            rs = getColumnNameAndColumeValue (sm, listSQL, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            rs.close();
            sm.close();
            conn.close();
        }
    }
    @SuppressWarnings("rawtypes")
    private static ResultSet getColumnNameAndColumeValue(Statement sm,List listSQL, ResultSet rs) throws SQLException {
        for (int j = 0; j < listSQL.size(); j++) {
            String sql = String.valueOf(listSQL.get(j));
            rs = sm.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuffer ColumnName = new StringBuffer();
                StringBuffer ColumnValue = new StringBuffer();
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    if (i == columnCount) {
                        ColumnName.append(rsmd.getColumnName(i));
                        if (Types.CHAR == rsmd.getColumnType(i) || Types.VARCHAR == rsmd.getColumnType(i)
                                || Types.LONGVARCHAR == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        } else if (Types.SMALLINT == rsmd.getColumnType(i) || Types.INTEGER == rsmd.getColumnType(i)
                                || Types.BIGINT == rsmd.getColumnType(i) || Types.FLOAT == rsmd.getColumnType(i)
                                || Types.DOUBLE == rsmd.getColumnType(i) || Types.NUMERIC == rsmd.getColumnType(i)
                                || Types.DECIMAL == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append(value);
                            }
                        } else if (Types.DATE == rsmd.getColumnType(i) || Types.TIME == rsmd.getColumnType(i)
                                || Types.TIMESTAMP == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append("'").append(value).append("'");
                            }
                        } else {
                            if (value == null) {
                                ColumnValue.append("null");
                            } else {
                                ColumnValue.append(value);
                            }
                        }
                    } else {
                        ColumnName.append(rsmd.getColumnName(i) + ",");
                        if (Types.CHAR == rsmd.getColumnType(i) || Types.VARCHAR == rsmd.getColumnType(i)
                                || Types.LONGVARCHAR == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        } else if (Types.SMALLINT == rsmd.getColumnType(i) || Types.INTEGER == rsmd.getColumnType(i)
                                || Types.BIGINT == rsmd.getColumnType(i) || Types.FLOAT == rsmd.getColumnType(i)
                                || Types.DOUBLE == rsmd.getColumnType(i) || Types.NUMERIC == rsmd.getColumnType(i)
                                || Types.DECIMAL == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append(value).append(",");
                            }
                        } else if (Types.DATE == rsmd.getColumnType(i) || Types.TIME == rsmd.getColumnType(i)
                                || Types.TIMESTAMP == rsmd.getColumnType(i)) {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append("'").append(value).append("',");
                            }
                        } else {
                            if (value == null) {
                                ColumnValue.append("null,");
                            } else {
                                ColumnValue.append(value).append(",");
                            }
                        }
                    }
                }
                insertSQL( ColumnName, ColumnValue, j, rs.isLast());
            }
        }
        return rs;
    }
    private static void insertSQL(StringBuffer ColumnName,StringBuffer ColumnValue,int order,Boolean isLast) {
        StringBuffer insertSQL= new StringBuffer();
        insertSQL.append( insert ).append( " " )
                .append( table [order]).append( "." ).append( schema ).append( " (" ).append(ColumnName.toString())
                .append( ") " ).append( values ).append( " (" ).append(ColumnValue.toString()).append( ");" );
        System.out.println(insertSQL);
        if(isOneFile){
            insertList .add(insertSQL.toString());
            if(order ==table.length -1 ) createFile();
        }else {
            insertList .add(insertSQL.toString());
            if(isLast){
                createFile(table [order],insertList);
            }
        }
    }
    public static void executeSelectSQLFile() throws Exception {
        List<String> listSQL= new ArrayList<String>();
        connectSQL ( DRIVER , URL , NAME ,PASS );
        listSQL= createSQL ();
        executeSQL ( conn , sm,listSQL);

        System.out.println(listSQL);
    }

    public static void main(String[] args) throws Exception {
        executeSelectSQLFile();
    }

}