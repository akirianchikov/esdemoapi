package com.akirianchikov.restclient.service;

import java.io.IOException;

import com.akirianchikov.restclient.model.Person;

public interface PersonService {

    String createIndex(String index) throws IOException;

    void save(Person person) throws IOException;

    Person findPersonInIndexById(String id) throws IOException;

}
