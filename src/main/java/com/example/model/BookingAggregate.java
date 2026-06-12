package com.example.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class BookingAggregate {


        private Long bookingId;

        private JsonNode booking;

        private JsonNode passenger;

}