package datos;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

// Imports per a desactivar els missatges de loggin
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.sql.Timestamp;
import java.util.Scanner;
import org.bson.Document;
import org.slf4j.LoggerFactory;

/**
 *
 * @author 2DAM
 */
public class ConexionMongoDB {

    public static MongoClient connectToMongoClient() {
        /*
    * Static method that connects to client
         */
        try {

            MongoClient dbClient = new MongoClient();
            return dbClient;
        } catch (Exception ex) {
            System.out.println("Something wrong connecting!");
            ex.printStackTrace(System.out);
        }
        return null;
    }

    public static void closeMongoClient(MongoClient c) {
        /*
    * Static method that closes connnection with client
         */
        try {
            c.close();
        } catch (Exception ex) {
            System.out.println("Unable to close!");
            ex.printStackTrace(System.out);
        }
    }

    public static MongoDatabase useDBMongo(MongoClient conn, String database, String collectionSelected) {

        conn = connectToMongoClient();
        MongoDatabase db = null;

        try {
            db = conn.getDatabase(database);
            createCollectionIfNotExists(db);
        } catch (Exception ex) {
            System.out.println("Something wrong accesing!");
            ex.printStackTrace(System.out);
        }
        return db;
    }

    // Preguntar si hi ha que utilitzar el dos metodes de conexio a mongoDB De moment no utilitzare aquest
    public static MongoDatabase selectDBMongo(MongoClient conn, String database) {
        // Getting a connection
        conn = connectToMongoClient();
        MongoDatabase db = null;

        try {
            db = conn.getDatabase(database);
        } catch (Exception ex) {
            System.out.println("Something wrong accesing!");
            ex.printStackTrace(System.out);
        }
        return db;
    }

    public static boolean collectionExists(String collectionName, MongoDatabase database) {
        /*
* Static method: Check if collection exists
         */
        MongoIterable<String> collection = database.listCollectionNames();
        for (String s : collection) {
            if (s.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    public static void createCollectionIfNotExists(MongoDatabase database) {
        /*
* Static method: Create collection if not exists
         */

        try {
            if (!(collectionExists("WeatherDataMZ06", database))) { // Creara aquesta coleccio si no existeix
                System.out.println("Collection does not exist");
                database.createCollection("WeatherDataMZ06");
                System.out.println("Created collection WeatherDataMZ06...");
            }
        } catch (Exception e) {
            System.out.println("Something wrong creating collection!");
            e.printStackTrace(System.out);
        }
    }

    // Metod per a desactivar els loggins
    public static void disableMongoLogging() {
        /*
* Static method: Disable annoying mongo log messages
* This method require add some code to POM file
* https://stackoverflow.com/questions/30137564/how-to-disable-mongodb-java-driver-logging
         */
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
    }

    public static int getMongoCounter(MongoClient conn) {
        int counter = 0;
        ConexionMongoDB.disableMongoLogging();

        try {

            MongoDatabase database = selectDBMongo(conn, "WeatherData");
            MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

            // Cuenta directamente los documentos de la colección
            counter = (int) col.countDocuments();
        } catch (Exception e) {
            System.out.println("Error al contar los documentos: " + e.getMessage());
        } finally {
            ConexionMongoDB.closeMongoClient(conn);
        }

        return counter;
    }

    public static void showElement(MongoClient conn) {
    MongoDatabase database = selectDBMongo(conn, "WeatherData");
    MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

    MongoCursor<Document> cursor = col.find().iterator();
    Scanner keyboard = new Scanner(System.in);
    try{

        int aux = 0;

        while (cursor.hasNext()) {
            Document d = cursor.next();
            System.out.println(d.toJson());
            System.out.println("");
            aux++;

            // Pausa después de cada dos documentos
            if (aux % 2 == 0 && cursor.hasNext()) {
                System.out.println("Chafa una tecla per continuar ...");
                keyboard.nextLine();
            }
        }
    } catch (Exception e) {
        System.out.println("Error al mostrar los elementos: " + e.getMessage());
    }
}


    public static void insertWeatherData(MongoClient conn, int recordId, String city, String country, float latitude, float longitude,
            Timestamp date, int temperatureCelsius, int humidityPercent, float precipitationMm,
            int windSpeedKmh, String weatherCondition, String forecast, Timestamp updated) {

        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        try {
            // Crear un documento con los datos
            Document doc = new Document("record_id", recordId)
                    .append("city", city)
                    .append("country", country)
                    .append("latitude", latitude)
                    .append("longitude", longitude)
                    .append("date", date.toInstant().toString()) // Convertir Timestamp a ISO 8601
                    .append("temperature_celsius", temperatureCelsius)
                    .append("humidity_percent", humidityPercent)
                    .append("precipitation_mm", precipitationMm)
                    .append("wind_speed_kmh", windSpeedKmh)
                    .append("weather_condition", weatherCondition)
                    .append("forecast", forecast)
                    .append("updated", updated.toInstant().toString()); // Convertir Timestamp a ISO 8601

            // Insertar el documento en la colección
            col.insertOne(doc);

            System.out.println("Documento insertado correctamente en MongoDB.");
        } catch (Exception e) {
            System.out.println("Error al insertar el documento en MongoDB: " + e.getMessage());
        }

    }

}
