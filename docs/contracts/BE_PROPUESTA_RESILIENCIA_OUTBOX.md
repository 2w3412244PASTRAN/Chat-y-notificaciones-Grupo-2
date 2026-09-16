# Propuesta de Resiliencia: Transactional Outbox Pattern
### Tema 11 — Notificaciones (MS-13) y Chat (MS-14)
### Responsabilidad transversal: contrato del Bus de Eventos

---

## 1. Objetivo

Este documento responde a la consigna de investigar **resiliencia ante fallas de propagación de eventos**: qué pasa cuando una operación que debería actualizar la base de datos y publicar un evento en Kafka **queda a la mitad**. Proponemos adoptar el **patrón Outbox (Transactional Outbox Pattern)** como mecanismo estándar para todo microservicio que publique eventos al bus, y lo dejamos formalizado como parte del contrato transversal que ya venimos definiendo en `INDICE_TEMAS.md`.

---

## 2. El problema: Dual Write Problem

Cualquier microservicio que necesite **cambiar su estado interno y avisarle al resto de la plataforma** (publicando un evento en Kafka) está, en los hechos, escribiendo en **dos sistemas distintos**: su propia base de datos y el broker de Kafka. No existe una transacción nativa que abarque ambos sistemas a la vez.

**Ejemplo aplicado a MS-14 (Chat):** cuando el agente moderador de IA bloquea un mensaje con severidad Alta (G02), el servicio tiene que:
1. Guardar el incidente en la tabla `chat_moderation_incident`.
2. Publicar un evento al topic `sistema.moderacion` para que MS-13 dispare la notificación al profesor y al ADMIN.

Si estas dos operaciones se ejecutan como pasos separados, sin protección, pueden fallar de dos formas:

| Escenario | Qué pasa | Consecuencia |
|---|---|---|
| **A. Se guarda en BD, falla la publicación** (Kafka caído, timeout de red) | El incidente queda registrado en `chat_moderation_incident`, pero el evento nunca sale | El profesor y el ADMIN **nunca se enteran** de un incidente de severidad Alta. Silencio total. |
| **B. Se publica el evento, falla el guardado en BD** (rollback tardío, error de validación) | MS-13 genera una notificación sobre un incidente que **no quedó persistido** | Se notifica algo que "no existe" del lado de Chat — inconsistencia entre lo que dice el evento y la fuente de la verdad. |

Ninguno de los dos escenarios es aceptable, y menos con la severidad Alta de por medio (justo el caso donde la resiliencia importa más).

---

## 3. La solución: Transactional Outbox Pattern

**Idea central:** en vez de publicar directamente a Kafka desde el código de negocio, el evento se escribe en una **tabla `outbox_event` dentro de la misma base de datos y la misma transacción SQL** que el cambio de negocio. Un proceso aparte (el *Outbox Poller* o *Message Relay*) es el único responsable de leer esa tabla y publicarla a Kafka.

Como las dos escrituras (negocio + outbox) van a la **misma base de datos relacional**, el motor de BD garantiza atomicidad de fábrica: **o se guardan las dos, o no se guarda ninguna**. Ya no hay forma de que una pase sin la otra.

```
┌─────────────────────────────────────────────────┐
│  TRANSACCIÓN SQL ÚNICA (todo o nada)             │
│                                                   │
│   1) INSERT/UPDATE chat_moderation_incident      │
│   2) INSERT INTO outbox_event (...)              │
│                                                   │
└─────────────────────────────────────────────────┘
                      │
                      │ commit
                      ▼
        ┌──────────────────────────┐
        │  Outbox Poller            │  (proceso @Scheduled, corre cada X seg)
        │  lee status = PENDIENTE   │
        └──────────────────────────┘
                      │
                      │ publica
                      ▼
        ┌──────────────────────────┐
        │  Kafka — topic            │
        │  sistema.moderacion       │
        └──────────────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
  consumer group           consumer group
  "notificaciones-group"   "otro-servicio-group"
  (MS-13)                  (si aplica)
```

Si Kafka está caído en el momento de publicar, el evento simplemente **queda pendiente** en la tabla — el poller reintenta en la siguiente corrida. No se pierde nada, y nunca se publica un evento que no corresponda a un cambio real, porque el `INSERT` en `outbox_event` solo ocurre si la transacción de negocio también se confirmó.

---

## 4. Modelo de datos: tabla `outbox_event`

Tabla que se agrega **dentro de la base de datos de cada microservicio productor** (no es una tabla compartida — cada servicio tiene la suya).

| Campo | Tipo | Descripción |
|---|---|---|
| `outbox_id` | UUID (PK) | Identificador interno de la fila en la tabla outbox |
| `event_id` | UUID | El mismo `eventId` que va dentro del envelope del evento (idempotencia del lado del consumidor) |
| `event_type` | VARCHAR | Ej: `CHAT_INCIDENTE_ALTA`, `GlobalConfigurationChanged` |
| `aggregate_type` | VARCHAR | Entidad de negocio afectada, ej: `chat_moderation_incident` |
| `aggregate_id` | UUID | ID de esa entidad, ej: `mensaje_id` o `incident_id` |
| `topic_destino` | VARCHAR | Topic de Kafka al que debe publicarse, ej: `sistema.moderacion` |
| `payload` | JSONB / TEXT | El evento completo serializado, según el contrato común (`eventId`, `eventType`, `timestamp`, `producer`, `payload`) |
| `status` | ENUM | `PENDIENTE`, `PUBLICADO`, `FALLIDO` |
| `intentos` | INT | Contador de reintentos de publicación |
| `created_at` | TIMESTAMP | Cuándo se generó el evento (misma transacción que el negocio) |
| `published_at` | TIMESTAMP, nullable | Cuándo se confirmó la publicación en Kafka |

---

## 5. Componentes del patrón

### 5.1 Productor (código de negocio)

Escribe la entidad de negocio **y** la fila de outbox en la misma transacción `@Transactional`. Nunca llama a Kafka directamente desde acá.

```java
package com.utn.tpi.chat.service;

import com.utn.tpi.common.dto.EventoDTO;
import com.utn.tpi.chat.model.ChatModerationIncident;
import com.utn.tpi.chat.model.OutboxEvent;
import com.utn.tpi.chat.repository.ChatModerationIncidentRepository;
import com.utn.tpi.chat.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Service
public class ModerationIncidentService {

    private final ChatModerationIncidentRepository incidentRepository;
    private final OutboxEventRepository outboxRepository;

    public ModerationIncidentService(ChatModerationIncidentRepository incidentRepository,
                                      OutboxEventRepository outboxRepository) {
        this.incidentRepository = incidentRepository;
        this.outboxRepository = outboxRepository;
    }

    @Transactional
    public void registrarIncidenteSeveridadAlta(String mensajeId, String remitenteId,
                                                  String cursoId, String categoria) {

        // 1) Persiste el incidente de negocio (MS-14)
        ChatModerationIncident incidente = new ChatModerationIncident(
                mensajeId, remitenteId, cursoId, categoria, "ALTA");
        incidentRepository.save(incidente);

        // 2) Arma el evento según el contrato común del bus
        String eventId = UUID.randomUUID().toString();
        EventoDTO evento = new EventoDTO(
                eventId,
                "CHAT_INCIDENTE_ALTA",
                LocalDateTime.now(),
                "tema-11-chat",
                Map.of(
                        "mensajeId", mensajeId,
                        "cursoId", cursoId,
                        "categoria", categoria,
                        "severidad", "ALTA"
                )
        );

        // 3) Inserta en la tabla outbox — MISMA transacción que el paso 1
        OutboxEvent outboxEvent = new OutboxEvent(
                eventId, "CHAT_INCIDENTE_ALTA", "chat_moderation_incident",
                incidente.getId(), "sistema.moderacion", evento, "PENDIENTE");
        outboxRepository.save(outboxEvent);

        // Si algo de esto falla, TODO se revierte (incluido el incidente). Nunca queda a la mitad.
    }
}
```

### 5.2 Outbox Poller (proceso aparte)

Corre en segundo plano, lee lo pendiente, publica a Kafka, y marca como publicado solo si Kafka confirma.

```java
package com.utn.tpi.chat.outbox;

import com.utn.tpi.chat.model.OutboxEvent;
import com.utn.tpi.chat.repository.OutboxEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class OutboxPoller {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final int MAX_INTENTOS = 5;

    public OutboxPoller(OutboxEventRepository outboxRepository,
                         KafkaTemplate<String, Object> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 3000) // corre cada 3 segundos
    public void publicarPendientes() {
        List<OutboxEvent> pendientes = outboxRepository.findByStatus("PENDIENTE");

        for (OutboxEvent evento : pendientes) {
            try {
                kafkaTemplate.send(evento.getTopicDestino(), evento.getAggregateId(), evento.getPayload())
                        .get(); // espera confirmación (ack) del broker

                evento.setStatus("PUBLICADO");
                evento.setPublishedAt(LocalDateTime.now());
            } catch (Exception e) {
                evento.setIntentos(evento.getIntentos() + 1);
                if (evento.getIntentos() >= MAX_INTENTOS) {
                    evento.setStatus("FALLIDO"); // pasa a revisión manual / alerta interna
                }
                // si no llegó al máximo, queda en PENDIENTE y se reintenta en la próxima corrida
            }
            outboxRepository.save(evento);
        }
    }
}
```

### 5.3 Kafka — Topic y Consumer Groups

Sin cambios respecto al contrato ya definido: el evento sigue el mismo formato común (`eventId`, `eventType`, `timestamp`, `producer`, `payload`), viaja por el topic correspondiente al dominio (`sistema.moderacion`, `sistema.notificaciones`), y cada microservicio interesado lo consume con su propio `groupId`.

### 5.4 Consumidor — idempotencia (ya definida, se reutiliza tal cual)

El poller garantiza **"al menos una entrega" (at-least-once)**, no "exactamente una". Por eso el consumidor sigue necesitando descartar duplicados usando el `eventId` — el mismo mecanismo que ya está definido en la arquitectura de MS-13 (control de idempotencia por `eventId` en la etapa de Ingesta del pipeline).

---

## 6. Qué garantías aporta el patrón

✅ **No se pierde ningún evento**: si Kafka está caído, el evento queda pendiente y se reintenta — nunca desaparece.
✅ **No se publican eventos "fantasma"**: el evento solo se genera si la transacción de negocio se confirmó.
✅ **Atomicidad real**: usa la transacción del propio motor de base de datos, sin necesidad de un coordinador de transacciones distribuidas (2PC), que sería mucho más pesado y complejo de operar.
✅ **Desacople**: el productor no depende de que Kafka esté disponible en el momento exacto de la operación — responde rápido al usuario y la propagación es asincrónica (consistencia eventual).

## 7. Qué NO resuelve, y cómo se cubre

⚠️ **No garantiza entrega única** (puede duplicar): se cubre con idempotencia por `eventId` del lado del consumidor (ya definido).
⚠️ **No garantiza orden entre topics distintos**: solo garantiza orden dentro de una misma partición de un mismo topic — no es un problema nuevo para nuestro caso, ya que cada tipo de evento va a su topic de dominio.
⚠️ **Fallas persistentes de publicación** (Kafka caído por mucho tiempo, o un evento con payload corrupto): se cubre con el contador de `intentos` + estado `FALLIDO`, que se puede resolver escalando a una **Dead Letter Queue** o alerta interna al equipo de infraestructura, en línea con el mecanismo de DLQ ya contemplado en el diseño de MS-13.

---

## 8. Checklist de implementación

- [ ] Crear tabla `outbox_event` en cada base de datos de microservicio productor (MS-13, MS-14, y comunicar el patrón al resto de los equipos que publican al bus).
- [ ] Reemplazar cualquier llamada directa a `kafkaTemplate.send(...)` dentro de lógica de negocio por un `INSERT` en `outbox_event` dentro de la misma transacción.
- [ ] Implementar el `OutboxPoller` como componente `@Scheduled`, con manejo de reintentos y límite máximo.
- [ ] Definir política de `MAX_INTENTOS` y qué pasa con un evento `FALLIDO` (alerta a ADMIN, panel de eventos fallidos, reintento manual).
- [ ] Confirmar con el resto de los equipos (vía `INDICE_TEMAS.md`) que van a adoptar el mismo patrón, para que la resiliencia sea pareja en todo el bus y no dependa de qué tan prolijo fue cada equipo por separado.

---

## 9. Riesgos y mitigación

| Riesgo | Mitigación |
|---|---|
| El poller se cae y no hay quien publique lo pendiente | Correrlo como parte del propio microservicio (no como proceso externo separado) para que escale junto con las réplicas del servicio; múltiples instancias del poller pueden coexistir si se usa un lock o `SELECT ... FOR UPDATE SKIP LOCKED` al leer pendientes. |
| La tabla `outbox_event` crece indefinidamente | Job de limpieza periódico que borra filas `PUBLICADO` con más de N días de antigüedad. |
| Un evento queda `FALLIDO` y nadie lo revisa | Exponer un endpoint/panel simple para ADMIN con los eventos fallidos, similar al panel de incidentes ya propuesto para moderación de chat. |
