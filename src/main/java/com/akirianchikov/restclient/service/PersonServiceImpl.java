package com.akirianchikov.restclient.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.akirianchikov.restclient.model.Person;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PersonServiceImpl implements PersonService {

    private String index;
    private final ElasticsearchClient client;

    public PersonServiceImpl(String index, ElasticsearchClient client) {
        this.index = index;
        this.client = client;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String createIndex(String index) throws IOException {

        CreateIndexResponse response = client.indices().create(c -> c
                .index(index));

        log.info("Index created: " + response.index());

        return response.index();
    }

    @Override
    public void save(Person person) throws IOException {
        save(Collections.singletonList(person));
    }

    public void save(List<Person> persons) throws IOException {
        final BulkResponse response = client.bulk(builder -> {
            for (Person person : persons) {
                builder.index(index)
                        .operations(ob -> {
                            if (person.getIdNumber() != null) {
                                ob.index(ib -> ib.document(person).id(person.getIdNumber()));
                                log.info("Document created with provided id: " + person.getIdNumber());
                            } else {
                                ob.index(ib -> ib.document(person));
                                log.info("Document created with generated id: " + person.getIdNumber());
                            }
                            return ob;
                        });
            }
            return builder;
        });

        final int size = persons.size();
        for (int i = 0; i < size; i++) {
            persons.get(i).setIdNumber(response.items().get(i).id());
        }
    }

    @Override
    public Person findPersonInIndexById(String id) throws IOException {
        final GetResponse<Person> getResponse = client.get(builder -> builder.index(index).id(id),
                Person.class);

        return getResponse.source();
    }

}
