package com.example.data.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;

@Configuration
public class DataSourceConfiguration {
    public static final TypeReference<List<SourceData>> typeRef = new TypeReference<>() {
    };

    private ApplicationContext applicationContext;
    private ObjectMapper objectMapper;
    private List<SourceData> sources;

    public DataSourceConfiguration(ApplicationContext applicationContext, ObjectMapper objectMapper, List<SourceData> sources) {
        this.applicationContext = applicationContext;
        this.objectMapper = new ObjectMapper();
        this.sources = sources;
    }

    @PostConstruct
    public void init() throws IOException {
        ConfigurableListableBeanFactory beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        sources = objectMapper.readValue(Resources.getResource("data-sources.json"), typeRef);
        for (SourceData source : sources) {
            DataSource dataSource = DataSourceBuilder
                    .create()
                    .username(source.getUsername())
                    .password(source.getPassword())
                    .url("jdbc:oracle:thin:@//" + source.getHost() + ":" + source.getPort() + "/" + source.getServiceId())
                    .driverClassName(source.getDriverClassName())
                    .type(HikariDataSource.class)
                    .build();

            beanFactory.registerSingleton(source.getName() + "_DS", dataSource);

            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            beanFactory.registerSingleton(source.getName() + "_JdbcTemplate", jdbcTemplate);

            NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
            beanFactory.registerSingleton(source.getName() + "_NamedParameterJdbcTemplate", namedParameterJdbcTemplate);
        }
    }

    @PreDestroy
    public void cleanup() {

    }
}
