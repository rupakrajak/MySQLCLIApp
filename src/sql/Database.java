/**
 * @author rupakrajak
 */
package sql;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.sql.*;

public class Database {
    /**
     * Database Driver
     */
    private static final String JDBC_DRIVER             = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL                  = "jdbc:mysql://localhost:3306/";
    private static String database                      = null;
    /**
     * Credentials
     */
    private static String username                      = null;
    private static String password                      = null;
    /**
     * Message color codes
     */
    private static final String RESET                   = "\u001B[0m";
    private static final String ERROR                   = "\u001B[31m";
    private static final String SUCCE                   = "\u001B[32m";
    private static final String UNAME                   = "\u001B[33m";
    private static final String MYSQL                   = "\u001B[34m";
    private static final String DBASE                   = "\u001B[35m";
    private static final String HEADR                   = "\u001B[36m";
    /**
     * Message symbols
     */
    private static final String TICK                    = "\u2714";
    private static final String CROS                    = "\u2718";
    /**
     * Parameter extractor regex
     */
    private static final String PARAM_REGEX             = "(\\'\\$\\{([^\\}\\']+)\\}\\')|(\\$\\{([^\\}]+)\\})";
    /**
     * Useful variables
     */
    private static Connection connection                = null;
    private static Statement statement                  = null;
    private static PreparedStatement preparedStatement  = null;
    private static ResultSet resultSet                  = null;
    private static List<String> matches                 = null;

    
    /** 
     * Giving an SQL exception, this will print the exception in a formatted way
     * @param se this is the SQL exception
     */
    private static void printSQLException(SQLException se) {
        System.out.println(ERROR + CROS + " ERROR " + RESET + se.getErrorCode() + " (" + se.getSQLState() + "): " + ERROR + se.getMessage() + RESET);
    }

    
    /** 
     * A helper method for method printResultSet() to print row separating lines
     * @param colWidth this array consists the width of columns of the current result set
     * @param sepType this specifies the line type 
     */
    private static void printRowSeparator(int[] colWidth, char sepType) {
        System.out.print("+");
        for (int i = 0; i < colWidth.length; i++) {
            int colDisSize = colWidth[i];
            for (int j = 0; j < colDisSize + 2; j++) System.out.print(sepType);
            System.out.print("+");
        }
        System.out.println("");
    }


    /**
     * This method prints the result set in a tabular format 
     */
    private static void printResultSet() {
        try {
            // extracting the metadata of the result set
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();

            // calculating the width of each column
            int[] colWidth = new int[colCount];
            for (int i = 0; i < colCount; i++) colWidth[i] = resultSetMetaData.getColumnLabel(i + 1).length();
            while (resultSet.next()) {
                for (int i = 0; i < colCount; i++) {
                    String colData = resultSet.getString(i + 1);
                    if (colData != null) colWidth[i] = Math.max(colWidth[i], colData.length());
                }
            }

            // reseting the pointer
            resultSet.beforeFirst();

            // printing the heading row
            printRowSeparator(colWidth, '=');
            System.out.print("|");
            for (int i = 0; i < colCount; i++) {
                String colValue = resultSetMetaData.getColumnLabel(i + 1).toUpperCase();
                System.out.print(" " + HEADR + colValue + RESET);
                for (int j = 0; j < colWidth[i] - colValue.length() + 1; j++)
                    System.out.print(" ");
                System.out.print("|");
            }
            System.out.println("");
            printRowSeparator(colWidth, '=');

            // printing the data rows
            int rowCount = 0; // initializing the rowCount
            while (resultSet.next()) {
                rowCount++;
                System.out.print("|");
                for (int i = 0; i < colCount; i++) {
                    String colValue = resultSet.getString(i + 1);
                    if (colValue == null) colValue = "";
                    for (int j = 0; j < colWidth[i] - colValue.length() + 1; j++)
                        System.out.print(" ");
                    System.out.print(colValue + " |");
                }
                System.out.println("");
                printRowSeparator(colWidth, '-');
            }

            // printing the SUCCESS message
            System.out.println(SUCCE + TICK + " SUCCESS" + RESET + ": " + rowCount + " row(s) in set");
        } catch (SQLException se) {
            printSQLException(se);
        }
    }

    
    /** 
     * This method performs the execution of query
     * @param sql this is the sql query string
     */
    private static void simpleQuery(String sql) {
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            boolean status = statement.execute(sql);
            if (status) {
                resultSet = statement.getResultSet();
                printResultSet();
            } else {
                int affectedRows = statement.getUpdateCount();
                System.out.println(SUCCE + TICK + " SUCCESS" + RESET + ": " + affectedRows + " row(s) affected");
            }
            preparedStatement = null;
            matches = null;
            resultSet = null;
        } catch (SQLException se) {
            printSQLException(se);
        }
        return;
    }


    /**
     * A helper method for method prepareQuery(String sql).
     * This checks for the parameter type and takes input accordingly, then sets them.
     */
    private static void getParametersAndExecute() {
        try {
            Console paramReader = System.console();
            for (int i = 0; i < matches.size(); i++) {
                if (matches.get(i).charAt(0) == '\'') {
                    String param = paramReader.readLine("Enter the value for " + matches.get(i).substring(3, matches.get(i).length() - 2) + ": ");
                    preparedStatement.setString(i + 1, param);
                } else {
                    long param = Long.parseLong(paramReader.readLine("Enter the value for " + matches.get(i).substring(2, matches.get(i).length() - 1) + ": "));
                    preparedStatement.setLong(i + 1, param);
                }
            }

            boolean status = preparedStatement.execute();
            if (status) {
                resultSet = preparedStatement.getResultSet();
                printResultSet();
            } else {
                int affectedRows = preparedStatement.getUpdateCount();
                System.out.println(SUCCE + TICK + " SUCCESS" + RESET + ": " + affectedRows + " row(s) affected");
            }
        } catch (SQLException se) {
            printSQLException(se);
        } catch (Exception e) {
            System.out.println(ERROR + CROS + " ERROR" + RESET + ":" + ERROR + " Error in setting the values of the parameters. Please check the syntax of the statement." + RESET);
            preparedStatement = null;
            matches = null;
            return;
        }
    }

    
    /** 
     * This method performs the execution of prepared query
     * @param sql this is the sql query string
     */
    private static void prepareQuery(String sql) {
        try {
            preparedStatement = connection.prepareStatement(sql.replaceAll(PARAM_REGEX, "?"));
            // extracting the parameters
            Pattern PARAM_REGEX_PATTERN = Pattern.compile(PARAM_REGEX);
            Matcher match = PARAM_REGEX_PATTERN.matcher(sql);
            matches = new ArrayList<>();
            while (match.find()) matches.add(match.group(0));
            getParametersAndExecute();
            statement = null;
        } catch (SQLException se) {
            printSQLException(se);
        }
        return;
    }
    
    /** 
     * This method identifies the query type, i.e., callable or prepared
     * @param sql this is the sql query string
     */
    private static void identifyQueryAndExecute(String sql) {
        if (sql.indexOf('$') == -1) simpleQuery(sql); else prepareQuery(sql);
    }

    
    /** 
     * The main method
     * @param args system args
     */
    public static void main(String[] args) throws Exception {
        try {
            Console console = System.console();
            username = console.readLine("Username: ");
            password = new String(console.readPassword("Password: "));
            try {
                Class.forName(JDBC_DRIVER);
                connection = DriverManager.getConnection(DB_URL, username, password);
            } catch (ClassNotFoundException e) {
                System.out.println(ERROR + "JDBC Driver not found!" + RESET);
            } catch (SQLException se) {
                System.out.println(ERROR + "Couldn't connect to the database!" + RESET);
                printSQLException(se);
            }
            if (connection == null) return;
            String s = "";
            try {
                while (true) {
                    ResultSet rs = connection.createStatement().executeQuery("SELECT DATABASE()");
                    rs.next();
                    database = rs.getString(1);
                    s = console.readLine(UNAME + username + RESET + "@" + MYSQL + "mysql" + RESET + ((database == null) ? "" : "::" + DBASE + database + RESET) + "> ").trim();
                    if (s.equals("exit")) {
                        connection.close();
                        return;
                    }
                    if (s.equals("/")) {
                        if (preparedStatement != null) getParametersAndExecute();
                        else continue;
                    } else if (s != null && !s.equals("")) identifyQueryAndExecute(s);
                    else continue;
                }
            } catch (SQLException se) {
                printSQLException(se);
                connection.close();
                return;
            }
        } catch (Exception e) {
            System.out.println("\n" + ERROR + CROS + " ERROR" + RESET + ":" + ERROR + " Some internal error occurred. Terminating the application." + RESET);
            connection.close();
            return;
        }
    }
}