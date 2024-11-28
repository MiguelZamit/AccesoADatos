/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package datos;

import domain.WeatherData;
import java.sql.*;
import java.util.Scanner;

/**
 *
 * @author miguel
 */
public class WeatherDataDAO {

    private static final String SQL_INSERT = "INSERT INTO WeatherDataMZ06 "
            + "(record_id, city, country, latitude, longitude, date, temperature_celsius, "
            + "humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updated) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_UPDATE = "update articulo set descripcion = (?), fabrica_id = (?) where idArticulo = (?)";
    private static final String SQL_DELETE = "delete from articulo where idArticulo = (?)";
    private static final String SQL_SELECT = "select count(*) as counter from WeatherDataMZ06"; //Preguntar si la tabla la tinc que disar aixina

    public static void insertWeatherData(Connection c, int recordId, String city, String country, float latitude, float longitude,
            Timestamp date, int temperatureCelsius, int humidityPercent, float precipitationMm,
            int windSpeedKmh, String weatherCondition, String forecast, Timestamp updated) {

        PreparedStatement st = null;

        try {
            st = c.prepareStatement(SQL_INSERT);

            st.setInt(1, recordId);
            st.setString(2, city);
            st.setString(3, country);
            st.setFloat(4, latitude);
            st.setFloat(5, longitude);
            st.setTimestamp(6, date);
            st.setInt(7, temperatureCelsius);
            st.setInt(8, humidityPercent);
            st.setFloat(9, precipitationMm);
            st.setInt(10, windSpeedKmh);
            st.setString(11, weatherCondition);
            st.setString(12, forecast);
            st.setTimestamp(13, updated);

            st.executeUpdate();
            System.out.println("Registro insertado correctamente.");
        } catch (SQLException e) {
            System.out.println("Error al insertar el registro: " + e.getMessage());
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch (SQLException e) {
                System.out.println("Error al cerrar el PreparedStatement: " + e.getMessage());
            }
        }
    }

//
//    public static void update(WeatherData wd, Connection c) {
//        PreparedStatement st;
//
//        try {
//            int idFabrica;
//            if ((idFabrica = encontrarFabrica(a.getFabrica_id())) == -1) {
//                throw new SQLException("El id de fabrica: " + a.getFabrica_id() + " no referencia a ninguna tabla de fabrica");
//            }
//
//            c = UtilsConexion.conexion();
//            st = c.prepareStatement(SQL_UPDATE);
//
//            st.setString(1, a.getDescripcion());
//            st.setInt(2, idFabrica);
//            st.setInt(3, a.getIdArticulo());
//
//            st.executeUpdate();
//
//            UtilsConexion.closeConexion(c, st);
//        } catch (SQLException sqle) {
//            System.out.println(sqle.getMessage());
//        }
//    }
//
//    public static void delete(int idArticulo, Connection c) {
//        
//        PreparedStatement st;
//
//        try {
//
//            if (encontrarArticuloPorId(idArticulo) == null) {
//                throw new SQLException("El id articulo: " + idArticulo + " no se encontro");
//            }
//
//            c = UtilsConexion.conexion();
//            st = c.prepareStatement(SQL_DELETE);
//
//            st.setInt(1, idArticulo);
//
//            st.executeUpdate();
//
//            UtilsConexion.closeConexion(c, st);
//
//        } catch (SQLException sqle) {
//            System.out.println(sqle.getMessage());
//        }
//    }
    public static int getMySQLDataCounter(Connection c) {

        PreparedStatement st;
        ResultSet rs;
        int counter = 0;
        try {

            st = c.prepareStatement(SQL_SELECT);
            rs = st.executeQuery();
            rs.next();
            counter = rs.getInt("counter");

        } catch (SQLException sqle) {

            System.out.println("Error -> " + sqle.getMessage());

        }

        return counter;

    }

    public static void showElements(Connection c) {

        Scanner keybord = new Scanner(System.in);
        PreparedStatement st;
        ResultSet rs;

        String sqlQuery = "select * from WeatherDataMZ06";
        int aux = 0;

        try {

            st = c.prepareStatement(sqlQuery);
            rs = st.executeQuery();

            while (rs.next()) {

                aux++;

                // funciona encara que per exemple record_id sea un enter
                System.out.println("");
                System.out.println("record_id -> " + rs.getString("record_id"));
                System.out.println("city -> " + rs.getString("city"));
                System.out.println("country -> " + rs.getString("country"));
                System.out.println("latitude -> " + rs.getString("latitude"));
                System.out.println("longitude -> " + rs.getString("longitude"));
                System.out.println("date -> " + rs.getString("date"));
                System.out.println("temperature_celcius -> " + rs.getString("temperature_celsius"));
                System.out.println("humidity_percent -> " + rs.getString("humidity_percent"));
                System.out.println("precipitation_mm -> " + rs.getString("precipitation_mm"));
                System.out.println("wind_speed_kmh -> " + rs.getString("wind_speed_kmh"));
                System.out.println("weather_condition -> " + rs.getString("weather_condition"));
                System.out.println("forecast -> " + rs.getString("forecast"));
                System.out.println("updated -> " + rs.getString("updated"));

                if (aux % 2 == 0 ) { // MOSTRARAR DE DOS EN DOS

                    System.out.println("Chafa un tecla per continuar ...");
                    keybord.nextLine();

                }

            }

        } catch (SQLException sqle) {

            System.out.println("Error -> " + sqle.getMessage());

        }

    }

}
