/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package datos;

import domain.WeatherData;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author miguel
 */
public class WeatherDataDAO {

    private static final String SQL_INSERT = "INSERT INTO WeatherDataMZ06 "
            + "(city, country, latitude, longitude, date, temperature_celsius, "
            + "humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updated) "
            + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_UPDATE = "UPDATE WeatherDataMZ06 SET city = ?, country = ?, latitude = ?, longitude = ?, date = ?, temperature_celsius = ?, humidity_percent = ?, precipitation_mm = ?, wind_speed_kmh = ?, weather_condition = ?, forecast = ?, updated = ? WHERE record_id = ?";
    private static final String SQL_DELETE = "delete from WeatherDataMZ06 where city = (?)";
    private static final String SQL_SELECT = "select count(*) as counter from WeatherDataMZ06"; //Preguntar si la tabla la tinc que disar aixina

    public static void insertWeatherData(Connection c, String city, String country, double latitude, double longitude,
            Timestamp date, int temperatureCelsius, int humidityPercent, double precipitationMm,
            int windSpeedKmh, String weatherCondition, String forecast, Timestamp updated) {

        PreparedStatement st = null;

        try {
            st = c.prepareStatement(SQL_INSERT);

            st.setString(1, city);
            st.setString(2, country);
            st.setDouble(3, latitude);
            st.setDouble(4, longitude);
            st.setTimestamp(5, date);
            st.setInt(6, temperatureCelsius);
            st.setInt(7, humidityPercent);
            st.setDouble(8, precipitationMm);
            st.setInt(9, windSpeedKmh);
            st.setString(10, weatherCondition);
            st.setString(11, forecast);
            st.setTimestamp(12, updated);

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

    // Falta mostrar mensaje si no hay nada
    public static void showElementByCity(Connection c, String city) {

        Scanner keybord = new Scanner(System.in);
        PreparedStatement st;
        ResultSet rs;

        String sqlQuery = "select * from WeatherDataMZ06 where city = (?)";
        int aux = 0;

        try {

            st = c.prepareStatement(sqlQuery);
            st.setString(1, city);
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

                if (aux % 2 == 0) { // MOSTRARAR DE DOS EN DOS

                    System.out.println("Chafa un tecla per continuar ...");
                    keybord.nextLine();

                }

            }

        } catch (SQLException sqle) {

            System.out.println("Error -> " + sqle.getMessage());

        }

    }

    public static void syncDataFromMySQL(Connection mysqlConn, List<WeatherData> mongoDataList) {

        for (WeatherData weatherData : mongoDataList) {
            try {

                String checkQuery = "SELECT * FROM WeatherDataMZ06 WHERE record_id = ?";
                PreparedStatement stmt = mysqlConn.prepareStatement(checkQuery);
                stmt.setInt(1, weatherData.getRecordId());
                ResultSet rs = stmt.executeQuery();

                String query = "INSERT INTO WeatherDataMZ06 "
                        + "(record_id, city, country, latitude, longitude, date, temperature_celsius, "
                        + "humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updated) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                if (!rs.next()) {

                    String insertQuery = query;
                    PreparedStatement insertStmt = mysqlConn.prepareStatement(insertQuery);
                    insertStmt.setInt(1, weatherData.getRecordId());
                    insertStmt.setString(2, weatherData.getCity());
                    insertStmt.setString(3, weatherData.getCountry());
                    insertStmt.setDouble(4, weatherData.getLatitude());
                    insertStmt.setDouble(5, weatherData.getLongitude());
                    insertStmt.setTimestamp(6, weatherData.getDate());
                    insertStmt.setDouble(7, weatherData.getTemperatureCelcius());
                    insertStmt.setDouble(8, weatherData.getHumidityPercent());
                    insertStmt.setDouble(9, weatherData.getPrecipitation_mm());
                    insertStmt.setDouble(10, weatherData.getWind_speed_kmh());
                    insertStmt.setString(11, weatherData.getWeather_condition());
                    insertStmt.setString(12, weatherData.getForecast());
                    insertStmt.setTimestamp(13, weatherData.getUpdated());
                    insertStmt.executeUpdate();
                } else {

                    String updateQuery = SQL_UPDATE;
                    PreparedStatement updateStmt = mysqlConn.prepareStatement(updateQuery);
                    updateStmt.setString(1, weatherData.getCity());
                    updateStmt.setString(2, weatherData.getCountry());
                    updateStmt.setDouble(3, weatherData.getLatitude());
                    updateStmt.setDouble(4, weatherData.getLongitude());
                    updateStmt.setTimestamp(5, weatherData.getDate());
                    updateStmt.setDouble(6, weatherData.getTemperatureCelcius());
                    updateStmt.setDouble(7, weatherData.getHumidityPercent());
                    updateStmt.setDouble(8, weatherData.getPrecipitation_mm());
                    updateStmt.setDouble(9, weatherData.getWind_speed_kmh());
                    updateStmt.setString(10, weatherData.getWeather_condition());
                    updateStmt.setString(11, weatherData.getForecast());
                    updateStmt.setTimestamp(12, weatherData.getUpdated());
                    updateStmt.setInt(13, weatherData.getRecordId());
                    updateStmt.executeUpdate();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static void insert(Connection c, WeatherData w) {

        PreparedStatement st;

        String query = "INSERT INTO WeatherDataMZ06 "
                + "(record_id, city, country, latitude, longitude, date, temperature_celsius, "
                + "humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updated) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            st = c.prepareStatement(query);

            st.setInt(1, w.getRecordId());
            st.setString(2, w.getCity());
            st.setString(3, w.getCountry());
            st.setDouble(4, w.getLatitude());
            st.setDouble(5, w.getLongitude());
            st.setTimestamp(6, w.getDate());
            st.setInt(7, w.getTemperatureCelcius());
            st.setInt(8, w.getHumidityPercent());
            st.setDouble(9, w.getPrecipitation_mm());
            st.setInt(10, w.getWind_speed_kmh());
            st.setString(11, w.getWeather_condition());
            st.setString(12, w.getForecast());
            st.setTimestamp(13, w.getUpdated());

            st.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Error -> " + e.getMessage());
        }

    }

    public static List<WeatherData> getMySQLData(Connection c) {

        PreparedStatement st;
        ResultSet rs;
        List<WeatherData> dataList = new ArrayList<>();

        String sqlQuery = "select * from WeatherDataMZ06";

        try {

            st = c.prepareStatement(sqlQuery);
            rs = st.executeQuery();

            while (rs.next()) {

                // funciona encara que per exemple record_id sea un enter
                int record_id = rs.getInt("record_id");
                String city = rs.getString("city");
                String country = rs.getString("country");
                double latitude = rs.getDouble("latitude");
                double longitude = rs.getDouble("longitude");
                Timestamp date = rs.getTimestamp("date");
                int temperature_celsius = rs.getInt("temperature_celsius");
                int humidity_percent = rs.getInt("humidity_percent");
                double precipitation_mm = rs.getDouble("precipitation_mm");
                int wind_speed_kmh = rs.getInt("wind_speed_kmh");
                String weather_condition = rs.getString("weather_condition");
                String forecast = rs.getString("forecast");
                Timestamp updated = rs.getTimestamp("updated");

                WeatherData data = new WeatherData(record_id, city, country, latitude, longitude, date, temperature_celsius, humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updated);

                dataList.add(data);

            }

        } catch (SQLException sqle) {

            System.out.println("Error -> " + sqle.getMessage());

        }

        return dataList;

    }

    // FALTA MOSTRAR MISSATGE SI ESTA BUIT
    public static void showElementsByCities(Connection c, String[] city) {
        String sqlQuery = "select * from WeatherDataMZ06 where city = (?)";

        Scanner keyboard = new Scanner(System.in);
        PreparedStatement st;
        ResultSet rs;

        int aux = 0;

        try {

            st = c.prepareStatement(sqlQuery);
            st.setString(1, city[0]);

            if (city.length == 1) {
                rs = st.executeQuery();
                while (rs.next()) {
                    aux++;

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

                    if (aux % 2 == 0) {
                        System.out.println("Presione una tecla para continuar ...");
                        keyboard.nextLine();
                    }
                }
            } else {

                StringBuilder queryBuilder = new StringBuilder("select * from WeatherDataMZ06 where city = (?)");
                for (int i = 1; i < city.length; i++) {
                    queryBuilder.append(" or city = (?)");
                }

                st = c.prepareStatement(queryBuilder.toString());

                for (int i = 0; i < city.length; i++) {
                    st.setString(i + 1, city[i]);
                }

                // System.out.println(st.toString());  
                rs = st.executeQuery();
                while (rs.next()) {
                    aux++;

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

                    if (aux % 2 == 0) {
                        System.out.println("Presione una tecla para continuar ...");
                        keyboard.nextLine();
                    }
                }
            }

        } catch (SQLException sqle) {
            System.out.println("Error -> " + sqle.getMessage());
        }
    }

    public static void deleteElementsByCities(Connection c, String[] city) {

        showElementsByCities(c, city);
        String sqlQuery = "delete from WeatherDataMZ06 where city = (?)";

        PreparedStatement st;

        int rowsAffected;
        try {

            st = c.prepareStatement(sqlQuery);
            st.setString(1, city[0]);

            if (city.length == 1) {

                rowsAffected = st.executeUpdate();
                System.out.println("Se han esborrat " + rowsAffected + " elements");

            } else {

                StringBuilder queryBuilder = new StringBuilder("delete from WeatherDataMZ06 where city = (?)");
                for (int i = 1; i < city.length; i++) {
                    queryBuilder.append(" or city = (?)");
                }

                st = c.prepareStatement(queryBuilder.toString());
                System.out.println(queryBuilder.toString());

                for (int i = 0; i < city.length; i++) {
                    st.setString(i + 1, city[i]);
                }

                rowsAffected = st.executeUpdate();

                System.out.println("Se han esborrat " + rowsAffected + " elements");
            }

        } catch (SQLException sqle) {
            System.out.println("Error -> " + sqle.getMessage());
        }

    }

    public static void deleteElementByCity(Connection conn, String city) {

        showElementByCity(conn, city);
        PreparedStatement st;

        try {

            st = conn.prepareStatement(SQL_DELETE);
            st.setString(1, city);
            int affectedElements = st.executeUpdate();

            System.out.println("Se han borrat un quantitat de " + affectedElements + " elements");

        } catch (SQLException e) {

            System.out.println("Error -> " + e.getMessage());

        }

    }

    public static void deleteAll(Connection c) {

        String query = "delete from WeatherDataMZ06";
        PreparedStatement st;
        int rowsAffected = 0;

        try {

            st = c.prepareStatement(query);
            rowsAffected = st.executeUpdate();

            System.out.println("Nombre de elements afectats: " + rowsAffected);

        } catch (SQLException e) {

            System.out.println("Error -> " + e.getMessage());

        }

    }

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
        Scanner keyboard = new Scanner(System.in);
        PreparedStatement st;
        ResultSet rs;

        String sqlQuery = "select * from WeatherDataMZ06 ORDER BY city ASC"; // 
        int aux = 0;

        try {
            st = c.prepareStatement(sqlQuery);
            rs = st.executeQuery();

            while (rs.next()) {
                aux++;

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

                if (aux % 2 == 0) {
                    System.out.println("Chafa una tecla per continuar ...");
                    keyboard.nextLine();
                }
            }

        } catch (SQLException sqle) {
            System.out.println("Error -> " + sqle.getMessage());
        }
    }

}
