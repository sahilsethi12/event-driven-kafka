package com.example.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Booking {
        private Long bookingId;
        private String recordLocator;
        private String currencyCode;
        private Integer status;

}
