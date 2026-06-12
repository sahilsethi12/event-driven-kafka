package com.example.model;


public class BookingPayment {

    private String bookingId;
    private String customerName;
    private Integer amount;

    public BookingPayment() {
    }

    public BookingPayment(String bookingId,
                          String customerName,
                          Integer amount) {

        this.bookingId = bookingId;
        this.customerName = customerName;
        this.amount = amount;
    }

    public String getBookingId() {
        return bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }
}

