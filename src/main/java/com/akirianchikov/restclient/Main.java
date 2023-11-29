package com.akirianchikov.restclient;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.akirianchikov.restclient.model.Person;
import com.akirianchikov.restclient.service.PersonServiceImpl;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {
        public static void main(String[] args) {

                // index to create
                final String INDEX = "person";

                // get props from application file
                String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
                String appConfigPath = rootPath + "application.properties";

                Properties appProps = new Properties();
                try {
                        appProps.load(new FileInputStream(appConfigPath));
                } catch (IOException e) {
                        log.error("Can't get props from file", e.getMessage());
                }

                String serverUrl = appProps.getProperty("serverUrl").trim();
                String apiKeyID = appProps.getProperty("apiKeyID").trim();
                String apiKeySecret = appProps.getProperty("apiKey").trim();

                // get encoded api cey for the connection
                /*
                 * String apiKeyAuth = Base64.getEncoder().encodeToString(
                 * (apiKeyID + ":" + apiKeySecret)
                 * .getBytes(StandardCharsets.UTF_8))
                 * .trim();
                 */

                // create the low-level client
                RestClientBuilder builder = RestClient.builder(
                                new HttpHost(serverUrl, 9243, "https"));

                /*
                 * Header[] defaultHeaders = new Header[] { new BasicHeader("Authorization",
                 * "ApiKey " + apiKeyAuth) };
                 */
                Header[] defaultHeaders = new Header[] {
                                new BasicHeader("Authorization", "ApiKey " + apiKeySecret) };

                builder.setDefaultHeaders(defaultHeaders);
                RestClient restClient = builder.build();

                // create the transport with a Jackson mapper
                final ObjectMapper mapper = new ObjectMapper();
                mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

                ElasticsearchTransport transport = new RestClientTransport(
                                restClient, new JacksonJsonpMapper(mapper));

                // create the elasticsearch API client
                ElasticsearchClient esClient = new ElasticsearchClient(transport);

                //
                PersonServiceImpl personService = new PersonServiceImpl(INDEX, esClient);

                // requests
                try {
                        // index
                        personService.createIndex(INDEX);
                        // document: Person
                        Person person = createPersons(1).get(0);
                        personService.save(person);
                        // get document from index
                        var id = person.getIdNumber();
                        Person personOfIndex = personService.findPersonInIndexById(id);
                        log.debug("Person in {} index found : {}", personService.getIndex(), personOfIndex);

                } catch (IOException e) {
                        log.error("Something went wrong", e.getMessage());
                }
        }

        private static List<Person> createPersons(int count) {
                ArrayList<Person> persons = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                        Person person = new Person();
                        person.setIdNumber(String.valueOf(i));
                        person.setFullName("Name " + i + " Surname " + i);
                        person.setAge(18 + i * 2);
                        persons.add(person);

                        log.info("Created person: {}", person);
                }

                return persons;
        }
}