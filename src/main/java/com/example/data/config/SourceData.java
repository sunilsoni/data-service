package com.example.data.config;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SourceData {
    private String name;
    private String host;
    private int port;
    private String username;
    private String password;
    private String serviceId;
    private String driverClassName;
}
