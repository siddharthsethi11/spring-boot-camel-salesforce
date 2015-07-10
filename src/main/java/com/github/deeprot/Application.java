package com.github.deeprot;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.spring.CamelBeanPostProcessor;
import org.apache.camel.spring.SpringCamelContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@Configuration
@EnableAsync
@EnableScheduling
public class Application {

	private static ApplicationContext context;
	
    public static void main(String[] args) {
        context = SpringApplication.run(Application.class, args);
    }

    // ==== Required for Camel beans in Spring context ====
    private static int PRODUCER_CACHE_SIZE = 100;
    private static int CONSUMER_CACHE_SIZE = 100;
    @Bean
    CamelContext camelContext() {
        CamelContext camelContext = new SpringCamelContext(context);
        SpringCamelContext.setNoStart(true);
        camelContext.disableJMX();
        return camelContext;
    }
    @Bean
    ProducerTemplate producerTemplate(CamelContext camelContext) {
        return camelContext.createProducerTemplate(PRODUCER_CACHE_SIZE);
    }
    @Bean
    ConsumerTemplate consumerTemplate(CamelContext camelContext) {
        return camelContext.createConsumerTemplate(CONSUMER_CACHE_SIZE);
    }
    @Bean
    CamelBeanPostProcessor camelBeanPostProcessor() {
        CamelBeanPostProcessor processor = new CamelBeanPostProcessor();
        processor.setApplicationContext(context);
        return processor;
    }
}
