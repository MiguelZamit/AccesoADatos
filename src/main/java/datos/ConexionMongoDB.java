package datos;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

// Imports per a desactivar els missatges de loggin
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import domain.WeatherData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.bson.Document;
import org.bson.conversions.Bson;
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

    public static void showElements(MongoClient conn) { // Ordenat alfabeticamente
        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        MongoCursor<Document> cursor = col.find().sort(new Document("city", 1)).iterator();
        Scanner keyboard = new Scanner(System.in);
        try {
            int aux = 0;

            while (cursor.hasNext()) {
                Document d = cursor.next();
                System.out.println(d.toJson());
                System.out.println("");
                aux++;

                if (aux % 2 == 0 && cursor.hasNext()) {
                    System.out.println("Chafa una tecla per continuar ...");
                    keyboard.nextLine();
                }
            }
        } catch (Exception e) {
            System.out.println("Error al mostrar los elementos: " + e.getMessage());
        }
    }

    // Preguntar si aquesta forma conta igual que el del pdf de CRUD
    // Falta mostrar mensaje si no hay nada
    // Este metode me dona error per la versio del controlador de mongo 
    /*
     */
    public static void showElementByCity(MongoClient conn, String city) {
        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        MongoCursor<Document> cursor = col.find().iterator();
        Scanner keyboard = new Scanner(System.in);
        try {

            int aux = 0;

            while (cursor.hasNext()) {

                Document d = cursor.next();

                if (d.getString("city").equalsIgnoreCase(city)) {

                    System.out.println(d.toJson());
                    System.out.println("");
                    aux++;

                    // Pausa después de cada dos documentos
                    if (aux % 2 == 0 && cursor.hasNext()) {
                        System.out.println("Chafa una tecla per continuar ...");
                        keyboard.nextLine();
                    }

                }

            }
        } catch (Exception e) {
            System.out.println("Error al mostrar los elementos: " + e.getMessage());
        }

    }

    public static void upsert(MongoClient mongo, List<WeatherData> data) {
        List<WeatherData> mysqlDataList = data;
        MongoCollection<Document> col = mongo.getDatabase("WeatherData").getCollection("WeatherDataMZ06");

        for (WeatherData weatherData : mysqlDataList) {
            Document filter = new Document("record_id", weatherData.getRecordId());

            Document updateDoc = new Document("$set", new Document("city", weatherData.getCity())
                    .append("country", weatherData.getCountry())
                    .append("latitude", weatherData.getLatitude())
                    .append("longitude", weatherData.getLongitude())
                    .append("date", weatherData.getDate())
                    .append("temperature_celsius", weatherData.getTemperatureCelcius())
                    .append("humidity_percent", weatherData.getHumidityPercent())
                    .append("precipitation_mm", weatherData.getPrecipitation_mm())
                    .append("wind_speed_kmh", weatherData.getWind_speed_kmh())
                    .append("weather_condition", weatherData.getWeather_condition())
                    .append("forecast", weatherData.getForecast())
                    .append("updated", weatherData.getUpdated()));

            // Si el element no existeix el actualitza i el fica
            col.updateOne(filter, updateDoc, new UpdateOptions().upsert(true));
        }

    }

    public static void upsert(MongoClient mongo, WeatherData data) {
        WeatherData mysqlDataList = data;
        MongoCollection<Document> col = mongo.getDatabase("WeatherData").getCollection("WeatherDataMZ06");

        Document filter = new Document("record_id", data.getRecordId());

        Document updateDoc = new Document("$set", new Document("city", data.getCity())
                .append("country", data.getCountry())
                .append("latitude", data.getLatitude())
                .append("longitude", data.getLongitude())
                .append("date", data.getDate())
                .append("temperature_celsius", data.getTemperatureCelcius())
                .append("humidity_percent", data.getHumidityPercent())
                .append("precipitation_mm", data.getPrecipitation_mm())
                .append("wind_speed_kmh", data.getWind_speed_kmh())
                .append("weather_condition", data.getWeather_condition())
                .append("forecast", data.getForecast())
                .append("updated", data.getUpdated()));

        // Si el element no existeix el actualitza i el fica
        col.updateOne(filter, updateDoc, new UpdateOptions().upsert(true));

    }

    public static List<WeatherData> getMongoDBData(MongoClient conn) {

        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        List<WeatherData> weatherDataList = new ArrayList<>();

        try (
                 MongoCursor<Document> cursor = col.find().iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();

                String dateString = document.getString("date");
                String updatedString = document.getString("updated");

                Timestamp date = dateString != null ? Timestamp.valueOf(dateString.replace("T", " ").substring(0, 19)) : null;
                Timestamp updated = updatedString != null ? Timestamp.valueOf(updatedString.replace("T", " ").substring(0, 19)) : null;

                WeatherData weatherData = new WeatherData(
                        document.getInteger("recordId"),
                        document.getString("city"),
                        document.getString("country"),
                        document.getDouble("latitude"),
                        document.getDouble("longitude"),
                        date, // Convertido a Timestamp
                        document.getInteger("temperature_celsius"),
                        document.getInteger("humidity_percent"),
                        document.getDouble("precipitation_mm"),
                        document.getInteger("wind_speed_kmh"),
                        document.getString("weather_condition"),
                        document.getString("forecast"),
                        updated // Convertido a Timestamp
                );

                weatherDataList.add(weatherData);
            }
        } catch (Exception e) {
            System.out.println("Error al obtener datos: " + e.getMessage());
            e.printStackTrace();
        }

        return weatherDataList;
    }

    // Aquest metodes es la valid para el driver de mongo
    public static void showElementsByCities(List<String> cities) {

        MongoClient mongo = connectToMongoClient();

        if (mongo == null) {
            System.out.println("La conexión a MongoDB no está abierta.");
            return;
        }

        MongoDatabase database = mongo.getDatabase("WeatherData");
        MongoCollection<Document> collection = database.getCollection("WeatherDataMZ06");

        if (cities == null || cities.isEmpty()) {
            System.out.println("No se han proporcionado ciudades.");
            return;
        }

        Document query = new Document("city", new Document("$in", cities));

        try {
            // Intentar realizar una operación sencilla para verificar la conexión
            MongoIterable<String> databases = mongo.listDatabaseNames();
            databases.first(); // Si la conexión falla, esta línea lanzará una excepción

            // Si la conexión es exitosa, continuamos con la consulta
            for (Document doc : collection.find(query)) {
                System.out.println("");
                System.out.println("record_id -> " + doc.getInteger("record_id"));
                System.out.println("city -> " + doc.getString("city"));
                System.out.println("country -> " + doc.getString("country"));
                System.out.println("latitude -> " + doc.getDouble("latitude"));
                System.out.println("longitude -> " + doc.getDouble("longitude"));
                System.out.println("date -> " + doc.getString("date"));
                System.out.println("temperature_celsius -> " + doc.getInteger("temperature_celsius"));
                System.out.println("humidity_percent -> " + doc.getInteger("humidity_percent"));

                // No se preque pero asi hiha un dada que la reconeix como a enter i altra como a double // Cuan fique Valencia i Madrid
                try {
                    System.out.println("precipitation_mm -> " + doc.getDouble("precipitation_mm"));
                } catch (Exception e) {
                    System.out.println("precipitation_mm -> " + doc.getInteger("precipitation_mm"));
                }

                System.out.println("wind_speed_kmh -> " + doc.getInteger("wind_speed_kmh"));
                System.out.println("weather_condition -> " + doc.getString("weather_condition"));
                System.out.println("forecast -> " + doc.getString("forecast"));
                System.out.println("updated -> " + doc.getString("updated"));
            }
        } catch (Exception e) {
            System.out.println("Error al ejecutar la consulta: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void insertWeatherData(MongoClient conn, String city, String country, double latitude, double longitude,
            Timestamp date, int temperatureCelsius, int humidityPercent, double precipitationMm,
            int windSpeedKmh, String weatherCondition, String forecast, Timestamp updated) {

        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        try {
            // Crear un documento con los datos
            // System.out.println(getLastId());

            Document doc = new Document("record_id", getLastId())
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

    public static int getLastId() {

        MongoClient mongo = connectToMongoClient();
        MongoDatabase database = selectDBMongo(mongo, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        Bson projection = Projections.fields(
                Projections.include("record_id"),
                Projections.excludeId());

        MongoCursor<Document> cursor = col.find().projection(projection).iterator();
        int max = Integer.MIN_VALUE;
        while (cursor.hasNext()) {

            Document item = cursor.next();
            int id = item.getInteger("record_id");
            if (id > max) {

                max = id;

            }

        }

        return max + 1;

    }

    public static void deleteElementByCity(MongoClient mongo, String city) {

        MongoDatabase database = selectDBMongo(mongo, "WeatherData");
        MongoCollection col = database.getCollection("WeatherDataMZ06");

        BasicDBObject filter = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<>();

        obj.add(new BasicDBObject("city", city));

        col.deleteOne(filter);

    }

    public static void deleteElementsByCities(MongoClient mongo, List<String> cities) {

        MongoDatabase database = selectDBMongo(mongo, "WeatherData");
        MongoCollection col = database.getCollection("WeatherDataMZ06");

        if (cities == null || cities.isEmpty()) {
            System.out.println("Ciutats no trobades");
            return;
        }

        BasicDBObject filter = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<>();

        for (String city : cities) {

            obj.add(new BasicDBObject("city", city));

        }

        filter.put("$or", obj);

        col.deleteMany(filter);

    }

    public static void deleteAll(MongoClient mongo) {

        MongoDatabase database = selectDBMongo(mongo, "WeatherData");
        MongoCollection col = database.getCollection("WeatherDataMZ06");

        if (col.countDocuments() == 0) {
            System.out.println("No hi han ciutats per esborrar");
            return;
        }

        BasicDBObject filter = new BasicDBObject();
        col.deleteMany(filter);

    }

}
