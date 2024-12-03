/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package datos;

import domain.WeatherData;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
            
            if (aux == 0){ // Ja que si aux no aumenta pues es com un indicar de que si hi han registres
                
                System.out.println("No se han trobat cap resultats per la ciutat insertada");
                
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
                
                if (aux == 0){
                        
                        System.out.println("No se han trobat cap resultats per la ciutat insertada");
                        
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
                
                if (aux == 0){
                    
                    System.out.println("No se han trobat cap resultats per la ciutat insertada");
                    
                }
                
            }

        } catch (SQLException sqle) {
            System.out.println("Error -> " + sqle.getMessage());
        }
    }
    
    public static int averangeWeatherMySQL(String city){
        
        Connection c = ConexionMySQL.getConnection();
            PreparedStatement st;
            ResultSet rs;
            String query = "select temperature_celsius from WeatherDataMZ06 where city like (?)";
            
            int aux = 0;
            int averange = 0;
            try{
                
                st = c.prepareStatement(query);
                st.setString(1, city);
                rs = st.executeQuery();
                
                while(rs.next()){
                    aux++;
                    averange+= rs.getInt("temperature_celsius");
                    
                }
                
                if (aux == 0){
                    
                    System.out.println("No se ha trobat cap registre de temperatura en MySQL");
                    return 0;
                    
                }
                
                return averange / aux;
                
            }catch(Exception e){
                
                System.out.println(e.getMessage()+"Error weatherData mySQL");
                return 0;
                
            }
            
            
        
    }
    
    
    
    // FUNCIO PER A IMPORTAR IMTEMS DE UN XML
    public static void importFromXML(Connection connection) {
    String xmlFilePath = "/home/miguel/NetBeansProjects/Practica_JDBC_Mes_Mongo/src/main/java/main/XMLWeatherDataMySQL.xml"; 
    try {
        
        File xmlFile = new File(xmlFilePath);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        org.w3c.dom.Document xmlDoc = builder.parse(xmlFile);

        
        xmlDoc.getDocumentElement().normalize();

       
        NodeList itemList = xmlDoc.getElementsByTagName("Item");

        
        String sql = "INSERT INTO WeatherDataMZ06 (city, country, latitude, longitude, date, "
                   + "temperature_celsius, humidity_percent, precipitation_mm, wind_speed_kmh, "
                   + "weather_condition, forecast, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        
        for (int i = 0; i < itemList.getLength(); i++) {
            Node itemNode = itemList.item(i);

            if (itemNode.getNodeType() == Node.ELEMENT_NODE) {
                Element itemElement = (Element) itemNode;

               
                String city = itemElement.getElementsByTagName("city").item(0).getTextContent();
                String country = itemElement.getElementsByTagName("country").item(0).getTextContent();
                double latitude = Double.parseDouble(itemElement.getElementsByTagName("latitude").item(0).getTextContent());
                double longitude = Double.parseDouble(itemElement.getElementsByTagName("longitude").item(0).getTextContent());
                String date = itemElement.getElementsByTagName("date").item(0).getTextContent();
                int temperatureCelsius = Integer.parseInt(itemElement.getElementsByTagName("temperature_celsius").item(0).getTextContent());
                int humidityPercent = Integer.parseInt(itemElement.getElementsByTagName("humidity_percent").item(0).getTextContent());
                double precipitationMm = Double.parseDouble(itemElement.getElementsByTagName("precipitation_mm").item(0).getTextContent());
                int windSpeedKmh = Integer.parseInt(itemElement.getElementsByTagName("wind_speed_kmh").item(0).getTextContent());
                String weatherCondition = itemElement.getElementsByTagName("weather_condition").item(0).getTextContent();
                String forecast = itemElement.getElementsByTagName("forecast").item(0).getTextContent();
                String updated = itemElement.getElementsByTagName("updated").item(0).getTextContent();

                preparedStatement.setString(1, city);
                preparedStatement.setString(2, country);
                preparedStatement.setDouble(3, latitude);
                preparedStatement.setDouble(4, longitude);
                preparedStatement.setString(5, date);
                preparedStatement.setInt(6, temperatureCelsius);
                preparedStatement.setInt(7, humidityPercent);
                preparedStatement.setDouble(8, precipitationMm);
                preparedStatement.setInt(9, windSpeedKmh);
                preparedStatement.setString(10, weatherCondition);
                preparedStatement.setString(11, forecast);
                preparedStatement.setString(12, updated);

                
                preparedStatement.executeUpdate();

               
                System.out.println("Dades insertades del XML: ");
                System.out.println("City: " + city);
                System.out.println("Country: " + country);
                System.out.println("Latitude: " + latitude);
                System.out.println("Longitude: " + longitude);
                System.out.println("Date: " + date);
                System.out.println("Temperature (Celsius): " + temperatureCelsius);
                System.out.println("Humidity (%): " + humidityPercent);
                System.out.println("Precipitation (mm): " + precipitationMm);
                System.out.println("Wind Speed (km/h): " + windSpeedKmh);
                System.out.println("Weather Condition: " + weatherCondition);
                System.out.println("Forecast: " + forecast);
                System.out.println("Updated: " + updated);
                
            }
        }

      

    } catch (Exception e) {
        System.out.println("Error al importar datos desde XML: " + e.getMessage());
        e.printStackTrace();
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
            
            if (aux == 0){
                
                System.out.println("No queda cap registres açi");
                
            }

        } catch (SQLException sqle) {
            System.out.println("Error -> " + sqle.getMessage());
        }
    }
    
    public static void showCities(Connection c){
        
        PreparedStatement st;
        ResultSet rs;
        
        String query = "select city from WeatherDataMZ06";
        
        try{
            
            st = c.prepareStatement(query);
            rs = st.executeQuery();
            
            System.out.println("Ciuatats disponibles: ");
            while(rs.next()){
                
                System.out.println("-> "+rs.getString("city"));
                
            }
            
        }catch(SQLException sqle){
            
            System.out.println(sqle.getMessage());
            
        }
        
    }

}
