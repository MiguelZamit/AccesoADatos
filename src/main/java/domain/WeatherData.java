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
    private float latitude;
    private float longitude;
    private Timestamp date;
    private int temperatureCelcius;
    private int humidityPercent;
    private float precipitation_mm;
    private int wind_speed_kmh;
    private String weather_condition;
    private String forecast;
    private Timestamp updated;

    public WeatherData(int recordId, String city, String country, float latitude, float longitude, Timestamp date, int temperatureCelcius, int humidityPercent, float precipitation_mm, int wind_speed_kmh, String weather_condition, String forecast, Timestamp updated) {
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

    public WeatherData(String city, String country, float latitude, float longitude, Timestamp date, int temperatureCelcius, int humidityPercent, float precipitation_mm, int wind_speed_kmh, String weather_condition, String forecast, Timestamp updated) {
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
    
    
    
}
