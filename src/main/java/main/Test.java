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
import domain.WeatherData;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import java.util.Scanner;
import org.bson.conversions.Bson;
import org.bson.Document;

/**
 *
 * @author 2DAM
 */
public class Test {

    private static final Scanner TECLADO = new Scanner(System.in);

    private static final List<String> userMenuOptions = new ArrayList<>();
    private static boolean synchronizeOption = false;

    public static void main(String[] args) {

        // Desactive misstages de loggin
        ConexionMongoDB.disableMongoLogging();

        Connection conMySQL;
        conMySQL = ConexionMySQL.getConnection();

        MongoClient conn = ConexionMongoDB.connectToMongoClient();

        MongoDatabase conMongo = ConexionMongoDB.useDBMongo(conn, "WeatherData", "WeatherDataMZ06");

        int counterMongo = ConexionMongoDB.getMongoCounter(conn);
        int counterMySQL = WeatherDataDAO.getMySQLDataCounter(conMySQL);

        if (counterMongo != counterMySQL) {
            synchronizeOption = true;
        }

        // refreshUserOptionList(); // CAMBIADO
        ConexionMongoDB.disableMongoLogging();
        // Esta conexion es para la de UserData

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

            System.out.println("Conexion a " + conMongo.getName() + " exitosa");

        }

        // Açi el menu per el crud de MySQL
        menu(optionDatabase, conMySQL, conn); // En un futur soles pasar una conexio

    }

    private static void refreshUserOptionList(String database) {

        userMenuOptions.clear();

        if (synchronizeOption) {

            if (database.equalsIgnoreCase("MongoDB")) {

                userMenuOptions.add("Inserir element");
                userMenuOptions.add("Llistar elements");
                userMenuOptions.add("Esborrar elements");
                userMenuOptions.add("Sincronitzar Base de Dades");
                userMenuOptions.add("Upsert de un element");
                userMenuOptions.add("Importar elements");
                userMenuOptions.add("EIXIR");

            } else {

                userMenuOptions.add("Inserir element");
                userMenuOptions.add("Llistar elements");
                userMenuOptions.add("Esborrar elements");
                userMenuOptions.add("Sincronitzar Base de Dades");
                userMenuOptions.add("Importar elements");
                userMenuOptions.add("EIXIR");

            }

        } else {

            if (database.equalsIgnoreCase("MongoDB")) {

                userMenuOptions.add("Inserir element");
                userMenuOptions.add("Llistar elements");
                userMenuOptions.add("Esborrar elements");
                userMenuOptions.add("Upsert de un element");
                userMenuOptions.add("Importar elements");
                userMenuOptions.add("EIXIR");

            } else {

                userMenuOptions.add("Inserir element");
                userMenuOptions.add("Llistar elements");
                userMenuOptions.add("Esborrar elements");
                userMenuOptions.add("Importar elements");
                userMenuOptions.add("EIXIR");

            }

        }

        for (int i = 0; i < userMenuOptions.size(); i++) {

            userMenuOptions.set(i, (i + 1) + ") " + userMenuOptions.get(i));

        }

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

        // ConexionMongoDB.closeMongoClient(conn);
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

        // ConexionMongoDB.closeMongoClient(conn);
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

        // ConexionMongoDB.closeMongoClient(conn);
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

        // ConexionMongoDB.closeMongoClient(conn);
        return sumatory / numOfCity; // Aquesta es la mitja

    }

    private static void menu(int optionDatabase, Connection conMySQL, MongoClient conMongo) {

        String database;

        // Determinar la base de datos actual
        switch (optionDatabase) {
            case 1:
                database = "MySQL";
                break;
            default:
                database = "MongoDB";
                break;
        }

        boolean exit = false;

        do {
            // Obtener contadores de ambas bases de datos
            int counterMongo = ConexionMongoDB.getMongoCounter(conMongo);
            int counterMySQL = WeatherDataDAO.getMySQLDataCounter(conMySQL);

            // Determinar si las bases de datos están desincronizadas
            synchronizeOption = (counterMongo != counterMySQL);

            // Actualizar las opciones del menú dinámicamente
            refreshUserOptionList(database);

            try {
                // Mostrar información de ambas bases de datos
                showCounterOfDataBases(conMySQL, conMongo);
                System.out.println("");

                // Mostrar el menú dinámico
                System.out.println("Menu per a la base de dades de " + database + ": ");
                for (String o : userMenuOptions) {
                    System.out.println(o);
                }

                // Leer la opción del usuario
                int opt = Integer.parseInt(TECLADO.nextLine());

                // Validar la opción seleccionada
                if (opt < 1 || opt > userMenuOptions.size()) {
                    System.out.println("Deus de ficar una opció vàlida. Prova altra vegada.");
                    continue;
                }

                // Procesar la opción seleccionada
                switch (opt) {
                    case 1: // Insertar elemento
                        insert(database, conMySQL, conMongo);
                        break;

                    case 2: // Listar elementos
                        menuLlistatElements(database, conMySQL, conMongo);
                        break;

                    case 3: // Borrar elementos
                        menuBorrarElements(database, conMySQL, conMongo);
                        break;

                    case 4: // Sincronizar bases de datos
                        if (synchronizeOption) {
                            System.out.println("Sincronitzant bases de dades...");
                            try {
                                Thread.sleep(4000);
                            } catch (InterruptedException ie) {
                                System.out.println("Error -> " + ie.getMessage());
                            }
                            // Sincronizar datos entre MongoDB y MySQL
                            
                            
                            // Agafe primer les dades de les dueps bases de dades
                            List<WeatherData> listFromMongo = ConexionMongoDB.getMongoDBData(conMongo);
                            List<WeatherData> listFromMySQL = WeatherDataDAO.getMySQLData(conMySQL);
                            
                            // Elimine le dades
                            
                            ConexionMongoDB.deleteAll(conMongo);
                            WeatherDataDAO.deleteAll(conMySQL);
                            
                            // Insert les dades que hem guardat
                            
                            
                            for (WeatherData w: listFromMySQL){
                                
                                ConexionMongoDB.insertWeatherData(w);
                                
                            }
                            
                            for (WeatherData w: listFromMongo){
                                
                                WeatherDataDAO.insert(conMySQL, w);
                                
                            }
                            // Creo que el MySQL funciona bien
                            // WeatherDataDAO.syncDataFromMySQL(conMySQL, listFromMongo);
                            
                            

                            System.out.println("Sincronització completada!");
                            
                            
                        } else {

                            if (database.equalsIgnoreCase("MongoDB")) {

                                System.out.println("Upsert de un element:");
                                
                                System.out.println("Dime el record_id que vols modificar: ");
                                System.out.println("records_id disponibles: \n");
                                ConexionMongoDB.showRecordsIds();
                                
                                int record_id = Integer.parseInt(TECLADO.nextLine());
                                System.out.println("");
                                
                                System.out.print("city -> ");
                                String city = TECLADO.nextLine();

                                if (city.equalsIgnoreCase("0")) {
                                    break;
                                }

                                System.out.print("country -> ");
                                String country = TECLADO.nextLine();

                                double latitude;

                                try {
                                    System.out.print("latitude -> ");
                                    latitude = Double.parseDouble(TECLADO.nextLine()); // SI SE INERTA UN LLETRA O RES SE ASIGNA A 0
                                } catch (NumberFormatException e) {
                                    latitude = 0;
                                }

                                double longitude;
                                try {
                                    System.out.print("longitude -> ");
                                    longitude = Double.parseDouble(TECLADO.nextLine());
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

                                double precipitation_mm;
                                try {
                                    System.out.print("precipitation_mm -> ");
                                    precipitation_mm = Double.parseDouble(TECLADO.nextLine());
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

                                Timestamp updatedTimeStamp = Timestamp.from(Instant.now());

                                // Pregunte noves dades y les cambie
                                WeatherData w = new WeatherData(record_id, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

                                ConexionMongoDB.upsert(w); 
                                System.out.println("\nElement actualitzat amb exit");
                                ConexionMongoDB.showRecordById(conMongo, record_id);
                                
                            }

                            System.out.println("Importar elements MySQL");

                            // funcionImportrElements // NO se si secesite condicio de una base de dades
                        }
                        break;

                    case 5: // Upsert o importar elementos
                        if (synchronizeOption) {

                            if (database.equalsIgnoreCase("MongoDB")) {

                                System.out.println("Upsert de un element:");
                                
                                System.out.println("Dime el record id que vols actualizar");
                                
                                System.out.println("record_id disponibles -> ");
                                ConexionMongoDB.showRecordsIds();
                                
                                int record_id = Integer.parseInt(TECLADO.nextLine());

                                System.out.print("city -> ");
                                String city = TECLADO.nextLine();

                                if (city.equalsIgnoreCase("0")) {
                                    break;
                                }

                                System.out.print("country -> ");
                                String country = TECLADO.nextLine();

                                double latitude;

                                try {
                                    System.out.print("latitude -> ");
                                    latitude = Double.parseDouble(TECLADO.nextLine()); // SI SE INERTA UN LLETRA O RES SE ASIGNA A 0
                                } catch (NumberFormatException e) {
                                    latitude = 0;
                                }

                                double longitude;
                                try {
                                    System.out.print("longitude -> ");
                                    longitude = Double.parseDouble(TECLADO.nextLine());
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

                                double precipitation_mm;
                                try {
                                    System.out.print("precipitation_mm -> ");
                                    precipitation_mm = Double.parseDouble(TECLADO.nextLine());
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

                                Timestamp updatedTimeStamp = Timestamp.from(Instant.now());

                                // Pregunte noves dades y les cambie
                                WeatherData w = new WeatherData(record_id, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

                                ConexionMongoDB.upsert(w); // Implementa esta función // Me ix el metode should be open que es com que se perd la conexio

                                System.out.println("Element actualitzat amb exit");
                                
                                ConexionMongoDB.showRecordById(conMongo, record_id);
                            } else {

                                System.out.println("Importar elements a MYSQL ...");

                            }

                        } else {

                            if (database.equalsIgnoreCase("MongoDB")) {

                                System.out.println("Importar elements de MongoDB ...");

                            } else {

                                exit = true;
                                break;
                            }

                        }
                        break;

                    case 6: // Importar elementos o salir
                        if (synchronizeOption) {

                            if (database.equalsIgnoreCase("MongoDB")) {

                                System.out.println("Importar elements Mongo...");
                                // importElements(conMySQL, conMongo); // Implementa esta función si aplica

                            } else {

                                exit = true;
                                break;

                            }
                            
                        } else {

                            if (database.equalsIgnoreCase("MongoDB")) {

                                exit = true;
                                break;

                            } else {

                                System.out.println("Opcio no valida");

                            }

                        }
                        break;

                    case 7: 

                        if (synchronizeOption && database.equalsIgnoreCase("MongoDB")) {

                            exit = true;
                            break;

                        }

                    default:
                        System.out.println("Opció no vàlida.");
                        break;
                }

            } catch (NumberFormatException nfe) {
                System.out.println("Error -> " + nfe.getMessage());
            }
        } while (!exit);

    }

//    private static void menu(int optionDatabase, Connection conMySQL, MongoClient conMongo) {
//
//        String database;
//
//        switch (optionDatabase) {
//            case 1:
//                database = "MySQL";
//                break;
//            default:
//                database = "MongoDB";
//                break;
//        }
//
//        boolean exit = false;
//        boolean validOpt;
//
//        do {
//
//            int counterMongo = ConexionMongoDB.getMongoCounter(conMongo);
//            int counterMySQL = WeatherDataDAO.getMySQLDataCounter(conMySQL);
//
//            if (counterMongo != counterMySQL) {
//                synchronizeOption = true;
//            }
//
//            refreshUserOptionList(database);
//
//            try {
//
//                showCounterOfDataBases(conMySQL, conMongo);
//                System.out.println("");
//                // System.out.println("Menu per a la base de dades de " + database + "\n1) Inserir element \n2) Llistar elements \n3) Esborrar elements \n4) Sincronitzar elements \n5) Importar elements \n6) EXIR");
//                System.out.println("Menu per a la base de dades de " + database + ": ");
//
//                for (String o : userMenuOptions) {
//
//                    System.out.println(o);
//
//                }
//
//                int opt = Integer.parseInt(TECLADO.nextLine());
//
//                validOpt = validOptions(opt);
//
//                if (!validOpt) {
//
//                    System.out.println("Deus de ficar una opcio valida. Proba altra vegada");
//
//                } else {
//
//                    // El torne a fer per si insertem i tornem a ser els mateixos elements
//                    switch (opt) {
//                        case 1:
//
//                            insert(database, conMySQL, conMongo);
//                            break;
//
//                        case 2: // Llistat de elements
//
//                            menuLlistatElements(database, conMySQL, conMongo); // Pasaem el nom de la base de dades per a condicionarla en el menu
//
//                            break;
//
//                        case 3:
//
//                            menuBorrarElements(database, conMySQL, conMongo);
//
//                            break;
//
//                        case 4:
//
//                            if (synchronizeOption) {
//
//                                System.out.println("Sincronitzant bases de dades ...");
//
//                                try {
//
//                                    Thread.sleep(4000);
//
//                                } catch (InterruptedException ie) {
//
//                                    System.out.println("Error -> " + ie.getMessage());
//
//                                }
//
//                                List<WeatherData> listFromMySQL;
//                                List<WeatherData> listFromMongo;
//
//                                listFromMongo = ConexionMongoDB.getMongoDBData(conMongo);
//                                WeatherDataDAO.syncDataFromMySQL(conMySQL, listFromMongo);
//
//                                listFromMySQL = WeatherDataDAO.getMySQLData(conMySQL);
//                                ConexionMongoDB.upsert(conMongo, listFromMySQL);
//
//  
//                                break;
//                            }
//
//                            // importarElements();
//                            break;
//                        case 5:
//
//                            
//                            if (database.equalsIgnoreCase("MongoDB")){
//                                
//                                
//                                
//                            }
//                            if (synchronizeOption) {
//
//                                // importarElements();
//                                break;
//
//                            }
//
//                            exit = true;
//                            break;
//                        case 6:
//
//                            if (synchronizeOption) {
//
//                                exit = true;
//
//                            } else {
//
//                                System.out.println("Opcion no valida");
//
//                            }
//
//                    }
//
//                    refreshUserOptionList(database);
//
//                }
//
//            } catch (NumberFormatException nfe) {
//
//                System.out.println("Error -> " + nfe.getMessage());
//
//            }
//
//        } while (!exit);
//
//    }
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

        if (a != b) {
            synchronizeOption = true;
        }

        System.out.println("\nContador de elements:\nMySQL -> " + a + "\nMongoDB -> " + b);

    }

    private static void insert(String DBName, Connection c, MongoClient conn) {

        boolean stop = false;

        do {

            // LAS FECHAS NO FUNCIONAN BIEN
            System.out.println("Introdueix dades fins a introduir 0 en recordId");
            System.out.println("");

            System.out.print("city -> ");
            String city = TECLADO.nextLine();

            if (city.equalsIgnoreCase("0")) {
                break;
            }

            System.out.print("country -> ");
            String country = TECLADO.nextLine();

            double latitude;

            try {
                System.out.print("latitude -> ");
                latitude = Double.parseDouble(TECLADO.nextLine()); // SI SE INERTA UN LLETRA O RES SE ASIGNA A 0
            } catch (NumberFormatException e) {
                latitude = 0;
            }

            double longitude;
            try {
                System.out.print("longitude -> ");
                longitude = Double.parseDouble(TECLADO.nextLine());
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

            double precipitation_mm;
            try {
                System.out.print("precipitation_mm -> ");
                precipitation_mm = Double.parseDouble(TECLADO.nextLine());
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

            Timestamp updatedTimeStamp = Timestamp.from(Instant.now());

            if (DBName.equalsIgnoreCase("MySQL")) { // Insertem en MySQL

                WeatherDataDAO.insertWeatherData(c, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

            } else { // Si no Mongo

                if (dateTimeStamp == null) {
                    dateTimeStamp = new Timestamp(0);
                }

                ConexionMongoDB.insertWeatherData(conn, city, country, latitude, longitude, dateTimeStamp, temperatureCelcius, humidityPercent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, updatedTimeStamp);

            }

        } while (!stop);

        int counterMongo = ConexionMongoDB.getMongoCounter(conn);
        int counterMySQL = WeatherDataDAO.getMySQLDataCounter(c);

        if (counterMongo == counterMySQL) {
            synchronizeOption = false;
        }

        refreshUserOptionList(DBName);

    }

    public static Timestamp stringToTimestamp(String dateString) {
        Timestamp timestamp;
        try {
            // Define el formato de la fecha según el formato del string
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Convierte el String a un objeto java.util.Date
            java.util.Date parsedDate = dateFormat.parse(dateString);

            // Convierte el java.util.Date a java.sql.Timestamp
            timestamp = new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            return null;
        }
        return timestamp;
    }

    private static void menuLlistatElements(String DBName, Connection conMySQL, MongoClient conMongo) {

        System.out.println("Dime que opcio vols elegir a l'hora de llistar els elements: \n1) Llistar per nom de ciutat \n2) Llistar per diverses ciutats \n3) ALL (Llista tot alfibeticament)");
        int option = Integer.parseInt(TECLADO.nextLine());

        switch (option) {
            case 1:

                System.out.println("Dime la ciutat que vols solicitar: ");
                String city = TECLADO.nextLine();

                System.out.println("Dades de " + DBName);
                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.showElementByCity(conMySQL, city);

                } else {

                    ConexionMongoDB.showElementByCity(conMongo, city);

                }

                break;

            case 2:

                System.out.println("Introduiex les ciutats separades per , :");
                String ipt = TECLADO.nextLine();

                String[] cities = ipt.split(",");
                for (int i = 0; i < cities.length; i++) {
                    cities[i] = cities[i].trim();
                }

                System.out.println("Dades de " + DBName);
                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.showElementsByCities(conMySQL, cities);

                } else {

                    ArrayList<String> list = new ArrayList<>();

                    for (int i = 0; i < cities.length; i++) {

                        list.add(cities[i]);

                    }

                    ConexionMongoDB.showElementsByCities(list); // No pase la conexio asi perque se perd

                }

                break;

            case 3:

                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.showElements(conMySQL);

                } else {

                    ConexionMongoDB.showElements(conMongo);

                }

                break;
            default:
                System.out.println("Opcio no valida");
        }

    }

    public static void menuBorrarElements(String DBName, Connection conMySQL, MongoClient conMongo) {

        System.out.println("Dime que opcio vols elegir a l'hora de esborrar elements: \n1) Esborrar per nom de ciutat \n2) Esborrar diverses ciutats \n3) ALL ");
        int option = Integer.parseInt(TECLADO.nextLine());

        switch (option) {
            case 1:

                System.out.println("Dime la ciutat que vols eliminar: ");
                String city = TECLADO.nextLine();

                System.out.println("Ciutat que se van a eliminar de " + DBName);
                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.deleteElementByCity(conMySQL, city);

                } else {

                    ConexionMongoDB.deleteElementByCity(conMongo, city);

                }

                break;

            case 2:

                System.out.println("Introduiex les ciutats separades per , :");
                String ipt = TECLADO.nextLine();

                String[] cities = ipt.split(",");
                for (int i = 0; i < cities.length; i++) {
                    cities[i] = cities[i].trim();
                }

                System.out.println("Elements que se van a esborrar de " + DBName);
                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.deleteElementsByCities(conMySQL, cities);

                } else {

                    ArrayList<String> list = new ArrayList<>();

                    for (int i = 0; i < cities.length; i++) {

                        list.add(cities[i]);

                    }

                    ConexionMongoDB.deleteElementsByCities(conMongo, list); // No pase la conexio asi perque se perd

                }

                break;

            case 3:

                System.out.println("Estas segur que vols eliminar tots el elements?: \n1) Si \n2) No");
                int opt = Integer.parseInt(TECLADO.nextLine());

                switch (opt) {
                    case 1:

                        if (DBName.equalsIgnoreCase("MySQL")) {

                            WeatherDataDAO.deleteAll(conMySQL);

                        } else {

                            ConexionMongoDB.deleteAll(conMongo);

                        }

                        break;

                    case 2:

                        return;

                    default:
                        System.out.println("Opcion no valida");
                }

                if (DBName.equalsIgnoreCase("MySQL")) {

                    WeatherDataDAO.showElements(conMySQL);

                } else {

                    ConexionMongoDB.showElements(conMongo);

                }

                break;
            default:
                System.out.println("Opcio no valida");
        }

        int counterMongo = ConexionMongoDB.getMongoCounter(conMongo);
        int counterMySQL = WeatherDataDAO.getMySQLDataCounter(conMySQL);

        if (counterMongo == counterMySQL) {
            synchronizeOption = false;
        }

        refreshUserOptionList(DBName);

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
