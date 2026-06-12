package com.example.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PassengerJourneyLeg {

    private Long passengerId;
    private Long inventoryLegId;
    private Integer legNumber;

}