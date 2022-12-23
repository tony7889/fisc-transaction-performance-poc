package com.mongodb.test.configuration;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

@Configuration
public class DBConfig {
    @Value("${settings.dbName}")
    private String dbName;
    
    @Autowired
    private MongoClient mongoClient;

    @Bean
    public CodecRegistry codecRegistry() {
		CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        return CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(pojoCodecProvider));
	}

    @Bean
    public MongoDatabase database() {
        return mongoClient.getDatabase(dbName).withCodecRegistry(codecRegistry());
    }
}
