/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package domain;

import java.sql.Timestamp;

/**
 *
 * @author miguel
 */
public class WeatherData {
    
    private int recordId; // Deu ser autoincremental
    private String city;
    private String country;
    private double latitude;
    private double longitude;
    private Timestamp date;
    private int temperatureCelcius;
    private int humidityPercent;
    private double precipitation_mm;
    private int wind_speed_kmh;
    private String weather_condition;
    private String forecast;
    private Timestamp updated;

    public WeatherData(int recordId, String city, String country, double latitude, double longitude, Timestamp date, int temperatureCelcius, int humidityPercent, double precipitation_mm, int wind_speed_kmh, String weather_condition, String forecast, Timestamp updated) {
        this.recordId = recordId;
        this.city = city;
        this.country = country;
        this.latitude = latitude;
        this.longitude = longitude;
        this.date = date;
        this.temperatureCelcius = temperatureCelcius;
        this.humidityPercent = humidityPercent;
        this.precipitation_mm = precipitation_mm;
        this.wind_speed_kmh = wind_speed_kmh;
        this.weather_condition = weather_condition;
        this.forecast = forecast;
        this.updated = updated;
    }

    public WeatherData(String city, String country, double latitude, double longitude, Timestamp date, int temperatureCelcius, int humidityPercent, double precipitation_mm, int wind_speed_kmh, String weather_condition, String forecast, Timestamp updated) {
        this.city = city;
        this.country = country;
        this.latitude = latitude;
        this.longitude = longitude;
        this.date = date;
        this.temperatureCelcius = temperatureCelcius;
        this.humidityPercent = humidityPercent;
        this.precipitation_mm = precipitation_mm;
        this.wind_speed_kmh = wind_speed_kmh;
        this.weather_condition = weather_condition;
        this.forecast = forecast;
        this.updated = updated;
    }

    public int getRecordId() {
        return recordId;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Timestamp getDate() {
        return date;
    }

    public int getTemperatureCelcius() {
        return temperatureCelcius;
    }

    public int getHumidityPercent() {
        return humidityPercent;
    }

    public double getPrecipitation_mm() {
        return precipitation_mm;
    }

    public int getWind_speed_kmh() {
        return wind_speed_kmh;
    }

    public String getWeather_condition() {
        return weather_condition;
    }

    public String getForecast() {
        return forecast;
    }

    public Timestamp getUpdated() {
        return updated;
    }
    
    
    
}
