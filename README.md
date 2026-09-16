# social-notifications-service

Microservicio encargado de las capacidades de Chat, Notificaciones, Mailing Transaccional y Moderacion de la plataforma.

## Estado

Proyecto en desarrollo.

Actualmente se encuentra configurada la estructura base del microservicio. Las capacidades funcionales seran incorporadas progresivamente.

## Stack Base

- Java 21
- Spring Boot
- Maven

## Requisitos

- Java 21
- Maven
- Git

## Estructura Del Proyecto

```text
social-notifications-service/
|-- .github/                  # Automatizacion
|-- docker/                   # Contenedorizacion
|-- docs/                     # Documentacion y contratos
|-- src/
|   |-- main/
|   |   |-- java/
|   |   |   `-- com/utn/tpi/socialnotif/
|   |   |       |-- common/
|   |   |       `-- modules/
|   |   `-- resources/
|   `-- test/
|-- .env.example
|-- .gitignore
|-- pom.xml
`-- README.md
```

## Organizacion Del Codigo

El codigo Java utiliza como package raiz:

```text
com.utn.tpi.socialnotif
```

La aplicacion se divide conceptualmente en:

- `common`: infraestructura transversal reutilizable.
- `modules`: capacidades funcionales del microservicio.

Los modulos previstos son:

- `notification`
- `mail`
- `chat`
- `moderation`

## Compilacion

```bash
mvn clean compile
```

## Build

```bash
mvn clean package
```

El artefacto generado se encuentra dentro de `target/`.

## Ejecucion

Para iniciar la aplicacion durante desarrollo:

```bash
mvn spring-boot:run
```

Tambien puede ejecutarse el JAR generado:

```bash
java -jar target/social-notifications-service-0.0.1-SNAPSHOT.jar
```

El perfil por defecto no requiere Kafka ni PostgreSQL.

Para iniciar con infraestructura Kafka/PostgreSQL habilitada:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=kafka
```

Tambien puede activarse el perfil sobre el JAR generado:

```bash
java -jar target/social-notifications-service-0.0.1-SNAPSHOT.jar --spring.profiles.active=kafka
```

El perfil `kafka` requiere las variables documentadas en `.env.example` y servicios externos disponibles.

## Infraestructura Local

Para levantar PostgreSQL y el Event Bus localmente:

```bash
docker compose up -d
```

Luego iniciar la aplicacion con el perfil de infraestructura:

```bash
java -jar target/social-notifications-service-0.0.1-SNAPSHOT.jar --spring.profiles.active=kafka
```

Para probar el publicador outbox, insertar un evento pendiente:

```bash
docker exec -i social-notifications-postgres psql -U social_notifications -d social_notifications -c "INSERT INTO outbox_event (outbox_id, event_id, event_type, aggregate_type, aggregate_id, topic_destino, message_key, payload, status, intentos, created_at) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'TestEvent', 'test', 'test-1', 'notification-events', 'test-1', 'hello-world', 'PENDIENTE', 0, now());"
```

Consumir el mensaje publicado en el Event Bus:

```bash
docker exec -it event-bus /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notification-events --from-beginning --timeout-ms 10000
```

Verificar que el evento quedo publicado:

```bash
docker exec -i social-notifications-postgres psql -U social_notifications -d social_notifications -c "SELECT status, intentos, published_at FROM outbox_event WHERE outbox_id = '00000000-0000-0000-0000-000000000001';"
```

## Configuracion

Las variables de entorno necesarias se documentan mediante `.env.example`.

Los archivos `.env` locales no deben versionarse.

## Documentacion

La documentacion tecnica adicional se encuentra o sera incorporada bajo `docs/`.
