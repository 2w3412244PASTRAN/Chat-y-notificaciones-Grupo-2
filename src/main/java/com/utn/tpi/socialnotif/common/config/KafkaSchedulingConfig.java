package com.utn.tpi.socialnotif.common.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@Profile("kafka")
public class KafkaSchedulingConfig {
}
