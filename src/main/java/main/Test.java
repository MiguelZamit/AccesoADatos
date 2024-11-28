/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package main;

// Paquetes MongoDB
import datos.ConexionMongoDB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

// Paquetes MySQL
import datos.ConexionMySQL;
import datos.WeatherDataDAO;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Scanner;
import org.bson.conversions.Bson;
import org.bson.Document;

/**
 *
 * @author 2DAM
 */
public class Test {

    private static final Scanner TECLADO = new Scanner(System.in);

    public static void main(String[] args) {

        ConexionMongoDB.disableMongoLogging();
        MongoClient conn = ConexionMongoDB.connectToMongoClient(); // Esta conexion es para la de UserData

        String DNI;
        boolean validDNI;
        boolean validUser = false;
        boolean user;

        do {

            // AQUI HAY HAY QUE PONER UNA FUNCION CON TODOS LOS USUARIOS REGISTRADOS
            showUsersDNI(conn);
            System.out.println("\nDime el teu DNI: ");
            DNI = TECLADO.nextLine();

            validDNI = validDNI(DNI);
            if (!validDNI) {
                do {

                    System.out.println("Deus de introduir un DNI valid. Torna a intentarlo.");
                    DNI = TECLADO.nextLine();
                    validDNI = validDNI(DNI);

                } while (!validDNI);
            }

            user = findUserByDNI(DNI, conn);
            if (!user) {

                System.out.println("Usuari no trobat. Torna a intentarlo");

            } else {

                validUser = true;

            }

        } while (!validUser || !user);

        // Sacar media de los grados a partir de una funcion
        showGreetingUser(conn, DNI);

        int optionDatabase = -1; // Por defecto un numero no valido
        do {

            System.out.println("Dime que base de datos desea conectarse: \n\t1) MySQL \n\t2) MongoDB");

            try {

                optionDatabase = Integer.parseInt(TECLADO.nextLine());
                if (!validNumber(optionDatabase)) {

                    System.out.println("Debes de introducir un numero valido entre las opciones");

                }

            } catch (NumberFormatException nfe) {

                System.out.println("Error -> " + nfe.getMessage());

            }

        } while (!validNumber(optionDatabase));

        Connection conMySQL = null;
        MongoDatabase conMongo;

        conMySQL = ConexionMySQL.getConnection();
        conMongo = ConexionMongoDB.useDBMongo(conn, "WeatherData", "WeatherDataMZ06");

        int counterMongo = ConexionMongoDB.getMongoCounter(conn);
        int counterMySQL = WeatherDataDAO.getMySQLDataCounter(conMySQL);

        // LO que anem a fer asi es tancar la conexio de la base de dades que no anem a utilitzar
        if (optionDatabase == 1) {

            System.out.println("Conectant a la base de dades de MySQL...");
            try {

                Thread.sleep(1500); // Simule una carga de dades

            } catch (InterruptedException ie) {
                System.out.println("Error -> " + ie.getMessage());
            }

            System.out.println("Conectant correctament a MySQL, a la base de dades WeatherData");

        } else {

            // Desactive misstages de loggin
            ConexionMongoDB.disableMongoLogging();
            System.out.println("Conectant a la base de dades de MongoDB...");

            try {

                Thread.sleep(1500); // Simulacio de carga de dades

            } catch (InterruptedException ie) {

                System.out.println("Error -> " + ie.getMessage());

            }

            try {
                conMongo = ConexionMongoDB.useDBMongo(conn, "WeatherData", "sfsfd"); // Me el invente per utilitzar el metode de createCollectionIfNotExists
                System.out.println("Conexion a " + conMongo.getName() + " exitosa");

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        }

        System.out.println("Contador de les dos bases de dades\nMySQL -> " + counterMySQL + "\nMongo -> " + counterMongo);

        // Açi el menu per el crud de MySQL
        menu(optionDatabase, conMySQL, conn);

    }

    // FUNCIONS
    // Retornare int per poder saber a quina base de dades me conectare fent la conexio en el main
    private static boolean validNumber(int database) {

        switch (database) {
            case 1:
            case 2:

                return true;

            default:
                return false;
        }

    }

    private static boolean validDNI(String dni) {

        if (dni == null || !dni.matches("\\d{8}[A-Z]")) {
            return false;
        }

        String numeros = dni.substring(0, 8);
        char letra = dni.charAt(8);

        String letrasValidas = "TRWAGMYFPDXBNJZSQVHLCKE";

        int indice = Integer.parseInt(numeros) % 23;
        char letraCorrecta = letrasValidas.charAt(indice);

        return letra == letraCorrecta;

    }

    // En un futuro le esta funcion un Mongodatabase de una funcion
    private static boolean findUserByDNI(String dni, MongoClient conn) {

        ConexionMongoDB.disableMongoLogging();

        try {
            MongoDatabase database = ConexionMongoDB.selectDBMongo(conn, "UserData"); // Nombre de base de datos y coleccion

            MongoCollection<Document> col = database.getCollection("Users");

            Bson projection = Projections.fields(
                    Projections.include("dni"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = col.find().projection(projection).iterator();

            // Mas adelante crear una funcion que se le pase el cursor y muestre los usuarios
            while (cursor.hasNext()) {

                Document item = cursor.next();
                String DNI = item.getString("dni");
                // Dni en minuscula es del Scanner que preguntem
                if (DNI.equalsIgnoreCase(dni)) {

                    return true;

                }

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        ConexionMongoDB.closeMongoClient(conn);

        return false; // Si no troba res tornem null

    }

    private static void showUsersDNI(MongoClient conn) {

        ConexionMongoDB.disableMongoLogging();

        try {
            MongoDatabase database = ConexionMongoDB.selectDBMongo(conn, "UserData"); // Nombre de base de datos y coleccion

            MongoCollection<Document> col = database.getCollection("Users");

            Bson projection = Projections.fields(
                    Projections.include("dni"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = col.find().projection(projection).iterator();

            // Mas adelante crear una funcion que se le pase el cursor y muestre los usuarios
            System.out.println("DNI de usuaris registrats: ");
            while (cursor.hasNext()) {

                Document item = cursor.next();
                String DNI = item.getString("dni");

                System.out.println("DNI -> " + DNI);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        ConexionMongoDB.closeMongoClient(conn);

    }

    // Aqui falta  devolver la funcion que calcula la media de Temperatura de esa ciudad
    private static void showGreetingUser(MongoClient conn, String userDNI) {

        ConexionMongoDB.disableMongoLogging();

        try {
            MongoDatabase database = ConexionMongoDB.selectDBMongo(conn, "UserData"); // Nombre de base de datos y coleccion

            MongoCollection<Document> col = database.getCollection("Users");

            Bson projection = Projections.fields(
                    Projections.include("dni"),
                    Projections.include("name"),
                    Projections.include("city"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = col.find().projection(projection).iterator();

            while (cursor.hasNext()) {

                Document item = cursor.next();
                String DNI = item.getString("dni");
                String city = item.getString("city");
                String name = item.getString("name");

                if (DNI.equalsIgnoreCase(userDNI)) {

                    int averange = averangeWeather(city, conn);

                    System.out.println("Benvingut " + name + ", la teua ciutat " + city + " hi ha una temperatura mitja de " + averange + " graus centigrads");

                }

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        ConexionMongoDB.closeMongoClient(conn);

    }

    private static int averangeWeather(String userCity, MongoClient conn) {

        ConexionMongoDB.disableMongoLogging();
        int numOfCity = 0;
        int sumatory = 0;

        try {
            MongoDatabase database = ConexionMongoDB.selectDBMongo(conn, "WeatherData"); // Nombre de base de datos y coleccion

            MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

            Bson projection = Projections.fields(
                    Projections.include("city"),
                    Projections.include("temperature_celsius"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = col.find().projection(projection).iterator();

            while (cursor.hasNext()) {

                Document item = cursor.next();
                int temperature_celsius = item.getInteger("temperature_celsius");
                String city = item.getString("city");

                if (city.equalsIgnoreCase(userCity)) {

                    ++numOfCity;
                    sumatory += temperature_celsius;

                }

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        ConexionMongoDB.closeMongoClient(conn);

        return sumatory / numOfCity; // Aquesta es la mitja

    }

    private static void menu(int optionDatabase, Connection conMySQL, MongoClient conMongo) {

        String database;

        switch (optionDatabase) {
            case 1:
                database = "MySQL";
                break;
            default:
                database = "MongoDB";
                break;
        }

        System.out.println("Dades de " + database);

        if (database.equalsIgnoreCase("MySQL")) {

            WeatherDataDAO.showElements(conMySQL);

        } else {

            ConexionMongoDB.showElement(conMongo);

        }

// Segons el enunciat tinc que primer ensenyar dades hasta que se pulse EXIR
        boolean exit = false;
        boolean validOpt;
        do {

            try {

                showCounterOfDataBases(conMySQL, conMongo);
                System.out.println("");
                System.out.println("Menu per a la base de dades de " + database + "\n1) Inserir element \n2) Llistar elements \n3) Esborrar elements \n4) Sincronitzar elements \n5) Importar elements \n6) EXIR");
                int opt = Integer.parseInt(TECLADO.nextLine());

                validOpt = validOptions(opt);

                if (!validOpt) {

                    System.out.println("Deus de ficar una opcio valida. Proba altra vegada");

                } else {

                    switch (opt) {
                        case 1:
                            insert(optionDatabase, conMySQL, conMongo);
                            break;

                        case 6:
                            exit = true;

                    }

                }

            } catch (NumberFormatException nfe) {

                System.out.println("Error -> " + nfe.getMessage());

            }

        } while (!exit);

    }

    private static boolean validOptions(int optionDatabase) {

        switch (optionDatabase) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
                return true;

            default:
                return false;
        }

    }

    private static void showCounterOfDataBases(Connection conMySQL, MongoClient conMongo) {

        int a = WeatherDataDAO.getMySQLDataCounter(conMySQL);
        int b = ConexionMongoDB.getMongoCounter(conMongo);

        System.out.println("\nContador de elements:\nMySQL -> " + a + "\nMongoDB -> " + b);

    }

    private static void insert(int optionDatabase, Connection c, MongoClient conn) {

        boolean stop = false;

        do {

                // LAS FECHAS NO FUNCIONAN BIEN
            
            System.out.println("Introdueix dades fins a introduir 0 en recordId");
            System.out.println("");

            System.out.print("record_id -> ");
            int record_id = Integer.parseInt(TECLADO.nextLine());
            
            if (record_id == 0){
                break;
            }

            System.out.print("city -> ");
            String city = TECLADO.nextLine();

            System.out.print("country -> ");
            String country = TECLADO.nextLine();

            float latitude;

            try {
                System.out.print("latitude -> ");
                latitude = Float.parseFloat(TECLADO.nextLine()); // SI SE INERTA UN LLETRA O RES SE ASIGNA A 0
            } catch (NumberFormatException e) {
                latitude = 0;
            }

            float longitude;
            try {
                System.out.print("longitude -> ");
                longitude = Float.parseFloat(TECLADO.nextLine());
            } catch (Exception e) {
                longitude = 0;
            }

            System.out.print("date -> ");
            String date = TECLADO.nextLine(); // Hay que pasarlo a TimeStamp

            Timestamp dateTimeStamp = stringToTimestamp(date); // COJER ESTE

            int temperatureCelcius;
            try {
                System.out.print("temperatureCelcius -> ");
                temperatureCelcius = Integer.parseInt(TECLADO.nextLine());
            } catch (Exception e) {
                temperatureCelcius = 0;
            }

            int humidityPercent;
            try {
                System.out.print("humidityPercent -> ");
                humidityPercent = Integer.parseInt(TECLADO.nextLine());
            } catch (Exception e) {
                humidityPercent = 0;
            }

            float precipitation_mm;
            try {
                System.out.print("precipitation_mm -> ");
                precipitation_mm = Float.parseFloat(TECLADO.nextLine());
            } catch (NumberFormatException e) {
                precipitation_mm = 0;
            }

            int wind_speed_kmh;
            try {
                System.out.print("wind_speed_kmh -> ");
                wind_speed_kmh = Integer.parseInt(TECLADO.nextLine());
            } catch (NumberFormatException e) {
                wind_speed_kmh = 0;
            }

            System.out.print("weather_condition -> ");
            String weather_condition = TECLADO.nextLine();

            System.out.print("forecast -> ");
            String forecast = TECLADO.nextLine();

            System.out.print("updated -> ");
            String updated = TECLADO.nextLine(); // Pasarlo a timeStamp

            Timestamp updatedTimeStamp = stringToTimestamp(updated); // Cojer este

            if (optionDatabase == 1) { // Insertem en MySQL

                WeatherDataDAO.insertWeatherData(c, record_id, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

            } else {

                ConexionMongoDB.insertWeatherData(conn, record_id, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

            }

        } while (!stop);

    }

    public static Timestamp stringToTimestamp(String dateString) {
        Timestamp timestamp = null;
        try {
            // Define el formato de la fecha según el formato del string
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Convierte el String a un objeto java.util.Date
            java.util.Date parsedDate = dateFormat.parse(dateString);

            // Convierte el java.util.Date a java.sql.Timestamp
            timestamp = new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            System.out.println("Error al convertir el String a Timestamp: " + e.getMessage());
        }
        return timestamp;
    }

}

// PREGUNTAR SI TINC QUE TORNAR A PREGUNTAR A
//        System.out.println("\nAbans de continuar en la menu desitges cambiar la base de dades? \n1) Si\n2) No");
//
//        boolean validNumber = false;
//        do {
//
//            try {
//
//                optionDatabase = Integer.parseInt(TECLADO.nextLine());
//                validNumber = validNumber(optionDatabase);
//                if (!validNumber) {
//                    System.out.println("Opcio no valida");
//                }else{
//                    validNumber = true;
//                }
//
//            } catch (NumberFormatException nfe) {
//
//                System.out.println("Deus de introduir un numero");
//
//            }
//
//        } while (!validNumber);
//        
//        
//        if (optionDatabase == 1){
//            
//            
//            
//        }
