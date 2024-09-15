package com.bilik.ditto;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DittoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DittoApplication.class, args);
	}

}
