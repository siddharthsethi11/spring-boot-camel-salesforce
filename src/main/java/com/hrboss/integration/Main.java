package com.hrboss.integration;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.spring.CamelBeanPostProcessor;
import org.apache.camel.spring.SpringCamelContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = { "com.hrboss" })
@EnableAutoConfiguration
public class Main {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Main.class, args);
	}
	
    // ==== Required for Camel beans in Spring context ====
	
    @Bean
    CamelContext camelContext(ApplicationContext context) {
        CamelContext camelContext = new SpringCamelContext(context);
        //SpringCamelContext.setNoStart(true);
        camelContext.disableJMX();
        return camelContext;
    }
    @Bean
    ProducerTemplate producerTemplate(CamelContext camelContext) {
        return camelContext.createProducerTemplate(100);
    }
    @Bean
    ConsumerTemplate consumerTemplate(CamelContext camelContext) {
        return camelContext.createConsumerTemplate(100);
    }
    @Bean
    CamelBeanPostProcessor camelBeanPostProcessor(ApplicationContext context) {
        CamelBeanPostProcessor processor = new CamelBeanPostProcessor();
        processor.setApplicationContext(context);
        return processor;
    }
    
}
