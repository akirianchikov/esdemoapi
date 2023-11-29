package com.akirianchikov.restclient;

import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import com.akirianchikov.restclient.model.Person;
import com.akirianchikov.restclient.service.PersonServiceImpl;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.var;
import lombok.extern.log4j.Log4j2;

import static org.assertj.core.api.Assertions.assertThat;

@Log4j2
@TestMethodOrder(OrderAnnotation.class)
public class esServicesTest {
    private static final String INDEX = "person";
    private static final String serverUrl = "my-deployment-e2fae1.es.eu-west-1.aws.found.io";
    // TODO put a passford before running tests, should be moved to secrets
    private static final String pass = "";

    private static ElasticsearchClient client;
    private static RestClient restClient;
    private static PersonServiceImpl personService;

    @BeforeAll
    public static void createCloudEsClient() throws Exception {

        HttpHost host = new HttpHost(serverUrl, 9243, "https");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", pass));
        final RestClientBuilder builder = RestClient.builder(host);

        builder.setHttpClientConfigCallback(clientBuilder -> {
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return clientBuilder;
        });

        final ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));
        client = new ElasticsearchClient(transport);
        personService = new PersonServiceImpl(INDEX, client);
    }

    @AfterAll
    public static void closeResources() throws Exception {
        restClient.close();
    }

    @AfterEach
    public void deleteProductIndex() throws Exception {
        client.indices().delete(b -> b.index(INDEX));
    }

    @Test
    @Order(1)
    public void createIndexTest() throws Exception {
        long currentTimeMillis = System.currentTimeMillis();
        var indexBuilder = INDEX + "_" + String.valueOf(currentTimeMillis);
        var indexCreated = personService.createIndex(indexBuilder);
        assertThat(indexCreated).isNotNull();
        personService.setIndex(indexCreated);
    }

    @Test
    @Order(2)
    public void createDocInIndexTest() throws Exception {
        Person person = createPersons(1).get(0);
        long currentTimeMillis = System.currentTimeMillis();
        person.setIdNumber(String.valueOf(currentTimeMillis));
        personService.save(person);

        var person_id = person.getIdNumber();
        assertThat(person_id).isNotNull();
        log.debug("Saved document: {}", person);

        Person personOfIndex = personService.findPersonInIndexById(person_id);
        assertThat(personOfIndex.getIdNumber()).isEqualTo(person_id);
        log.debug("Person in {} index found : {}", personService.getIndex(), personOfIndex);

    }

    @Test
    @Order(3)
    @Disabled
    public void indexPersonWithId() throws Exception {
        Person person = createPersons(1).get(0);
        long currentTimeMillis = System.currentTimeMillis();
        person.setIdNumber(String.valueOf(currentTimeMillis));
        personService.save(person);

        assertThat(person.getIdNumber()).isNotNull();
        log.debug("Saved document: {}", person);
    }

    @Test
    @Order(4)
    @Disabled
    public void indexPersonWithoutId() throws Exception {
        Person person = createPersons(1).get(0);
        person.setIdNumber(null);
        assertThat(person.getIdNumber()).isNull();
        personService.save(person);

        assertThat(person.getIdNumber()).isNotNull();
        log.debug("Saved document: {}", person);
    }

    private List<Person> createPersons(int count) {
        ArrayList<Person> persons = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Person person = new Person();
            person.setIdNumber(String.valueOf(i));
            person.setFullName("Name " + i + " Surname " + i);
            person.setAge(18 + i * 2);
            persons.add(person);

            log.debug("Created person: {}", person);
        }

        return persons;
    }

}
