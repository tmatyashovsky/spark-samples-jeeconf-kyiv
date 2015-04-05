package com.lohika.morning.spark.presentation.spark.distributed.library.type;

import java.io.Serializable;

public class Participant implements Serializable {

    private String firstName;
    private String lastName;
    private String companyName;
    private String position;
    private String email;
    private boolean present;

    public Participant(String firstName, String lastName, String companyName, String position, String email, boolean present) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.companyName = companyName;
        this.position = position;
        this.email = email;
        this.present = present;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getPosition() {
        return position;
    }

    public String getEmail() {
        return email;
    }

    public Boolean getPresent() {
        return present;
    }

}
