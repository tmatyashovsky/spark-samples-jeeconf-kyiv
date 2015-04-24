package com.lohika.morning.spark.presentation.spark.driver.reader;

public interface DataReader<T> {

    T readAllParticipants(final boolean includeOnlyPresentParticipants);

}
