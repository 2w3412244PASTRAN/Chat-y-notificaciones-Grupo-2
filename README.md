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

## Configuracion

Las variables de entorno necesarias se documentan mediante `.env.example`.

Los archivos `.env` locales no deben versionarse.

## Documentacion

La documentacion tecnica adicional se encuentra o sera incorporada bajo `docs/`.
