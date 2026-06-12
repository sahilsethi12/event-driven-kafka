package com.example.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BookingPassenger {

    private Long passengerId;
    private Long bookingId;

    private String firstName;
    private String lastName;
    private String paxType;

    // getters/setters
}