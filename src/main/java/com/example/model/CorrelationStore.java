package com.example.model;


import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class CorrelationStore {

    private final ConcurrentHashMap<String, Booking> bookings =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Payment> payments =
            new ConcurrentHashMap<>();

    public void saveBooking(Booking booking) {
        bookings.put(booking.getBookingId(), booking);
    }

    public void savePayment(Payment payment) {
        payments.put(payment.getBookingId(), payment);
    }

    public Booking getBooking(String bookingId) {
        return bookings.get(bookingId);
    }

    public Payment getPayment(String bookingId) {
        return payments.get(bookingId);
    }
}
