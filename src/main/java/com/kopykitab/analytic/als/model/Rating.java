package com.kopykitab.analytic.als.model;

import java.io.Serializable;

public class Rating implements Serializable {
    private int userId;
    private int bookId;
    private float rating;
    private long timestamp;

    public Rating() {}

    public Rating(int userId, int bookId, float rating, long timestamp) {
        this.userId = userId;
        this.bookId = bookId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public int getbookId() {
        return bookId;
    }

    public float getRating() {
        return rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Rating parseRating(String str) {
        String[] fields = str.split(",");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int bookId = Integer.parseInt(fields[1]);
        float rating = Float.parseFloat(fields[2]);
        long timestamp = Long.parseLong(fields[3]);
        return new Rating(userId, bookId, rating, timestamp);
    }
}