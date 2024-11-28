/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package datos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author miguel
 */
public class ConexionMySQL {

    // DEBE DE CUMPLIR EL SIGUIENTE FORMATO ->
    /*

    IMPORTANT: La taula/col·lecció amb els registres haurà de crear-se si no existeix i
    haurà de cridar-se WeatherDataXXZZ on «xx» son la inicial del teu nom i la inicial del
    teu primer cognom i ZZ el teu DIA de naiximent
        
     */
    
    
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/WeatherDataMZ06?useSSL=false&useTimezone=true&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "1234"; // serpis para clase

    public static Connection getConnection() {
        
        try{
            return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        }catch(SQLException sqle){
            
            System.out.println("Error -> "+sqle.getMessage());
            
        }
        
        return null; // No deuria de plegar asi mai
        
    }

    public static void close(Connection c) throws SQLException {
        c.close();
    }

    public static void close(ResultSet rs) throws SQLException {
        rs.close();
    }

    public static void close(Statement st) throws SQLException {
        st.close();
    }

}
