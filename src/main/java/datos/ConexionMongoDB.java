package datos;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

// Imports per a desactivar els missatges de loggin
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import domain.WeatherData;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

            counter = (int) col.countDocuments();
        } catch (Exception e) {
            System.out.println("Error al contar los documentos: " + e.getMessage());
        } finally {
            ConexionMongoDB.closeMongoClient(conn);
        }

        return counter;
    }

    public static void showElements(MongoClient conn) { // Ordenat alfabeticament
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
            
            if (aux == 0){
                
                System.out.println("No queda cap registres açi");
                
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

            if (aux == 0) {

                System.out.println("No se han trobat cap resultats per la ciutat insertada");

            }
        } catch (Exception e) {
            System.out.println("Error al mostrar los elementos: " + e.getMessage());
        }

    }


    public static void upsert(WeatherData data) {

        MongoClient mongo = connectToMongoClient();

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

        MongoDatabase database = ConexionMongoDB.selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");
        List<WeatherData> list = new ArrayList<>();

        MongoCursor<Document> cursor = col.find().iterator();

        while (cursor.hasNext()) {

            Document doc = cursor.next();

            int record_id;
            try {
                record_id = doc.getInteger("record_id");
            } catch (NullPointerException e) {
                record_id = 0;
            }

            String city = doc.getString("city");
            String country = doc.getString("country");
            double latitude = doc.getDouble("latitude");
            double longitude = doc.getDouble("longitude");

            String date = "";
            try {

                date = doc.getString("date");

            } catch (Exception e) {
                System.out.println(e.getMessage() + " Error al obtener la fecha");
            }

            Timestamp dateToTimestamp = null;
            try {

                dateToTimestamp = stringToTimestamp(date);
            } catch (Exception e) {

                dateToTimestamp = null;
            }

            int temperature_celcius = doc.getInteger("temperature_celcius", 0);
            int humidity_percent = doc.getInteger("humidity_percent", 0);

            double precipitation_mm;
            try {
                precipitation_mm = doc.getDouble("precipitation_mm");
            } catch (Exception e) {
                int aux = doc.getInteger("precipitation_mm");
                precipitation_mm = (int) aux;
            }

            int wind_speed_kmh = doc.getInteger("wind_speed_kmh", 0);
            String weather_condition = doc.getString("weather_condition");
            String forecast = doc.getString("forecast");

            // Obtener la fecha actual para el campo `update`
            Timestamp update = new Timestamp(System.currentTimeMillis());

            WeatherData weatherData = new WeatherData(record_id, city, country, latitude, longitude, dateToTimestamp, temperature_celcius, humidity_percent, precipitation_mm, wind_speed_kmh, weather_condition, forecast, update);

            list.add(weatherData);
        }

        return list;
    }

    public static String convertDateToString(Date date) {
        // Define el formato deseado para la fecha
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Convierte el Date a String usando el formato
        return formatter.format(date);
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

//    public static List<WeatherData> getMongoDBData(MongoClient conn) {
//
//        MongoDatabase database = selectDBMongo(conn, "WeatherData");
//        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");
//
//        List<WeatherData> weatherDataList = new ArrayList<>();
//
//        try {
//
//            MongoCursor<Document> cursor = col.find().iterator();
//
//            while (cursor.hasNext()) {
//                Document document = cursor.next();
//                Date date;
//
//                String dateString;
//                try {
//                    date = document.getDate("date");
//                } catch (Exception e) {
//                    dateString = document.getString("date");
//                }
//
//                Date updated = document.getDate("updated");
//
//                Timestamp dateToTimeStamp;
//                if (date != null) {
//                    dateToTimeStamp = new Timestamp(date.getTime());
//                } else {
//                    dateToTimeStamp = null;
//                }
//
//                Timestamp updatedToTimeStamp;
//                if (updated != null) {
//                    updatedToTimeStamp = new Timestamp(updated.getTime());
//                } else {
//                    updatedToTimeStamp = null;
//                }
//
//                WeatherData weatherData = new WeatherData(
//                        document.getInteger("record_id"),
//                        document.getString("city"),
//                        document.getString("country"),
//                        document.getDouble("latitude"),
//                        document.getDouble("longitude"),
//                        dateToTimeStamp, // Convertido a Timestamp
//                        document.getInteger("temperature_celsius"),
//                        document.getInteger("humidity_percent"),
//                        document.getDouble("precipitation_mm"),
//                        document.getInteger("wind_speed_kmh"),
//                        document.getString("weather_condition"),
//                        document.getString("forecast"),
//                        updatedToTimeStamp // Convertido a Timestamp
//                );
//
//                weatherDataList.add(weatherData);
//            }
//        } catch (Exception e) {
//            System.out.println("Error al obtener datos: " + e.getMessage());
//            e.printStackTrace();
//        }
//
//        return weatherDataList;
//    }
    // Aquest metodes es la valid para el driver de mongo
    public static void showElementsByCities(List<String> cities) {

        MongoClient mongo = connectToMongoClient();

        MongoDatabase database = mongo.getDatabase("WeatherData");
        MongoCollection<Document> collection = database.getCollection("WeatherDataMZ06");

        if (cities.isEmpty()) {
            System.out.println("No has insertat cap ciutat");
            return;
        }

        Document query = new Document("city", new Document("$in", cities));
        int aux = 0;
        try {

//            MongoIterable<String> databases = mongo.listDatabaseNames();
//            databases.first();

            for (Document doc : collection.find(query)) {
                aux++;
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
            
            if (aux == 0){
                
                System.out.println("No se han trobat cap dades de les ciutats insertades");
                
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

            String aux = timestampToString(date);
            String aux2 = timestampToString(updated);

            Document doc = new Document("record_id", getLastId())
                    .append("city", city)
                    .append("country", country)
                    .append("latitude", latitude)
                    .append("longitude", longitude)
                    .append("date", aux)
                    .append("temperature_celsius", temperatureCelsius)
                    .append("humidity_percent", humidityPercent)
                    .append("precipitation_mm", precipitationMm)
                    .append("wind_speed_kmh", windSpeedKmh)
                    .append("weather_condition", weatherCondition)
                    .append("forecast", forecast)
                    .append("updated", aux2);

            // Insertar el documento en la colección
            col.insertOne(doc);

            System.out.println("Documento insertado correctamente en MongoDB.");
        } catch (Exception e) {
            System.out.println("Error al insertar el documento en MongoDB: " + e.getMessage());
        }

    }

    // Tinc que borrar el altre
    public static String timestampToString(Timestamp timestamp) {
        if (timestamp == null) {
            return null; // Retorna null si el timestamp es nulo
        }

        // Define el formato de fecha que deseas
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

        // Convierte el Timestamp a String
        return sdf.format(timestamp);
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

    public static void showRecordsIds() {

        MongoClient mongo = connectToMongoClient();
        MongoDatabase database = selectDBMongo(mongo, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        Bson projection = Projections.fields(
                Projections.include("record_id"),
                Projections.excludeId());

        MongoCursor<Document> cursor = col.find().projection(projection).iterator();
        while (cursor.hasNext()) {

            Document item = cursor.next();
            System.out.println("record_id -> " + item.getInteger("record_id"));

        }

    }
    
    public static void showCities(MongoClient conn) {

        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        Bson projection = Projections.fields(
                Projections.include("city"),
                Projections.excludeId());
        
        MongoCursor<Document> cursor = col.find().projection(projection).iterator();
        
        System.out.println("Ciutats disponibles: ");
        int aux = 0;
        while (cursor.hasNext()) {            
            aux++;
            Document doc = cursor.next();
            System.out.println("-> "+doc.getString("city"));
            
        }
            
        

    }

    public static void showRecordById(MongoClient conn, int record_id) {

        MongoDatabase database = selectDBMongo(conn, "WeatherData");
        MongoCollection<Document> col = database.getCollection("WeatherDataMZ06");

        MongoCursor<Document> cursor = col.find().iterator();
        try {

            while (cursor.hasNext()) {

                Document d = cursor.next();

                if (d.getInteger("record_id") == record_id) {

                    System.out.println(d.toJson());

                }

            }
        } catch (Exception e) {
            System.out.println("Error al mostrar los elementos: " + e.getMessage());
        }

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

    public static void insertWeatherData(WeatherData w) {
        MongoClient mongo = connectToMongoClient();
        MongoDatabase database = mongo.getDatabase("WeatherData");
        MongoCollection<Document> collection = database.getCollection("WeatherDataMZ06");

        Timestamp temp = w.getDate();
        Timestamp temp2 = w.getUpdated();

        String aux = timestampToString(temp);
        String aux2 = timestampToString(temp2);

        Document document = new Document()
                .append("recordId", w.getRecordId())
                .append("city", w.getCity())
                .append("country", w.getCountry())
                .append("latitude", w.getLatitude())
                .append("longitude", w.getLongitude())
                .append("date", aux) // Per si me dona el problema de String o Date 2023-11-10 00:00:00
                .append("temperature_celsius", w.getTemperatureCelcius())
                .append("humidity_percent", w.getHumidityPercent())
                .append("precipitation_mm", w.getPrecipitation_mm())
                .append("wind_speed_kmh", w.getWind_speed_kmh())
                .append("weather_condition", w.getWeather_condition())
                .append("forecast", w.getForecast())
                .append("updated", aux2); // Per si me dona el problema de String o Date

        collection.insertOne(document);

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
