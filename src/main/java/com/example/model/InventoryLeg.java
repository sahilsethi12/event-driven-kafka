package com.example.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InventoryLeg {

    private Long inventoryLegId;

    private String carrierCode;
    private String flightNumber;

    private String departureStation;
    private String arrivalStation;

}