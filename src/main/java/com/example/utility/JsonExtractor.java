package com.example.utility;

import com.jayway.jsonpath.JsonPath;

public class JsonExtractor {

    public static Long bookingId(String json) {
        return JsonPath.read(json, "$.message.data.BookingID");
    }

    public static Long passengerId(String json) {
        return JsonPath.read(json, "$.message.data.PassengerID");
    }

    public static Long inventoryLegId(String json) {
        return JsonPath.read(json, "$.message.data.InventoryLegID");
    }

    public static String firstName(String json) {
        return JsonPath.read(json, "$.message.data.FirstName");
    }

    public static String lastName(String json) {
        return JsonPath.read(json, "$.message.data.LastName");
    }
}