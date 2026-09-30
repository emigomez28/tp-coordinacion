# Informe TP Coordinación

Implementación en **Go** del sistema de control de stock de la verdulería. Este informe explica cómo se coordinan las instancias de `Sum` y `Aggregation`, y cómo escala el sistema frente a más clientes, mayores volúmenes de datos y mayor cantidad de nodos.

## 1. Arquitectura y topología

| Recurso | Tipo | Nombre (por configuración) |
|---|---|---|
| Gateway -> Sum | cola durable | `INPUT_QUEUE` (`input_queue`) |
| Sum -> Sum | la misma cola | `input_queue` |
| Sum -> Aggregation | exchange `direct` | `AGGREGATION_PREFIX`, keys `<prefix>_0..A-1` |
| Aggregation -> Join | cola durable | `join_queue` |
| Join -> Gateway | cola durable | `results_queue` |

Se utilizan prefijos, colas y cardinalidades que salen del entorno.

## 2. Middleware

La interfaz `Middleware` (encontrada en el archivo `common/middleware/middleware.go`) es la misma utilizada en el TP MOM y las implementaciones concretas de la interfaz tambien.

Se agregó un nuevo método a la interfaz:

```go
SendTo(msg Message, routingKey string) error
```

Esto se debe a que `Send` publica en **todas** las keys que el middleware alcanza, mientras que `SendTo` publica en una y falla con `ErrMessageMiddlewareMessage` si esa key no es alcanzable.

El contrato se comporta de igual manera para ambas implementaciones. Por un lado, para el exchange el conjunto alcanzable es `em.keys`, mientras que para una cola es de tamaño uno (su propio nombre), así que `SendTo` es el mismo `Send` con la key explícita.

## 3. Protocolo interno de mensajes

El esqueleto distinguía los mensajes con un booleano (`IsEOF`) dentro de un struct plano. Se reemplazó por implementaciones concretas de mensajes y una interfaz general `ProtocolMessage` utilizando el patrón `Visitor` para resolver cada uno (`common/messageprotocol/inner/`):

```go
type ProtocolMessage interface {
    ClientID() string
    Accept(visitor Visitor) error
    messageType() string
    encodePayload() ([]byte, error)
}

type Visitor interface {
    VisitData(message *DataMessage) error
    VisitEndOfRecords(message *EndOfRecordsMessage) error
    VisitEmitTotals(message *EmitTotalsMessage) error
}
```

| Tipo | Tag | Origen y significado |
|---|---|---|
| `DataMessage` | `data` | Registros, totales, parciales o un top |
| `EndOfRecordsMessage` | `eof` | El cliente terminó de enviar |
| `EmitTotalsMessage` | `emit_totals` | Una réplica de `Sum` pide a sus pares que emitan |


La idea de implementar un visitor se da por que Go no tiene estructura `switch-case` esxhaustiva, es decir, dentro de un bloque switch-case sobre un enum, si por algún motivo se agrega una variante esto compila y cae en `default`, mientras que la implementación de este patrón obliga a handlear todas las variantes.

`Aggregation` y `Join` implementan `VisitEmitTotals` devolviendo `NewUnexpectedMessageError` ya que ese mensaje nunca sale de `input_queue`, y si aparece allí implica que hay un error en la configuración.

## 4. Coordinación

### 4.1 Aislamiento por cliente

El `clientID` se asigna en el borde de entrada al sistema (el gateway) y viaja en **todos** los mensajes del protocolo interno. Cada nodo mantiene estado indexado por cliente:

- `itemsByClient` en `Sum` y `Aggregation` como mapa que mantiene el estado del cliente, teniendo como clave `clientID`.
- `topsByClient` en `Join` para guardar los tops por cliente, tambien siendo un mapa que tiene como clave `clientID`.

`Aggregation` y `Join` borran la entrada al emitir su resultado.


### 4.2 Coordinación entre instancias de `Sum`

Como las réplicas son consumidoras en competencia de `input_queue`, cada una acumulaba una parte de los registros. Luego, como el fin de archivo entra al sistema como **un solo** mensaje `eof` por cliente, que consume **una sola** réplica, las otras nunca vaciaban su estado y se perdían sus totales parciales. Entonces, por esto se devolvían tops incompletos.

**La solución implementada consiste en enviar una notificación por el mismo camino ordenado que los datos.**

1. La réplica que recibe ese `eof` emite sus totales y publica `S-1` (sabiendo que hay `S` instancias de `Sum`) mensajes `emit_totals` de vuelta a `input_queue`.
2. Cada réplica recibe uno por su propia goroutine de `input_queue` y emite lo suyo.
3. Si un `emit_totals` cae en una réplica que ya emitió, lo **reencola** en vez de consumirlo, para conservar la cantidad de avisos en vuelo.

Una versión anterior hacía el fan-out por un exchange `sum` dedicado. Funcionaba casi siempre, pero el aviso llegaba por una **conexión AMQP distinta** de la de los datos, y RabbitMQ sólo garantiza orden FIFO dentro de una misma cola sobre un mismo canal. Una réplica podía vaciar mientras todavía tenía registros sin sumar en su buffer local, obteniendo así una condición de carrera.

Al poner el aviso **dentro de `input_queue`**, se publica necesariamente después de todos los registros del cliente, y RabbitMQ entrega en orden de publicación.

### 4.3 Coordinación entre instancias de `Sum` y `Aggregation`

Las instancias de `Aggregation` necesitan dos políticas de ruteo **sobre el mismo exchange**:

- **Datos particionados por fruta:** Utilizo una función de hash para poder hacer distintos shards calculados como `hash % A` siendo `A` la cantidad de replicas de `Aggregation`. Luego, se publica con el nuevo método implementado en los middlewares `SendTo` a una routing key construida a partir de `<prefix>_<shard>`.
- **Fin de archivo por broadcast:** El método `Send` publica el `eof` a todas las keys, esto se debe a que todas las instancias de `Aggregation` tienen que emitir su top parcial para que `Join` pueda hacer el conteo.

Antes de esto, `Sum` publicaba todo con `Send` y cada `Aggregation` recibía el dataset completo. Las `A` instancias calculaban el mismo top y `Join` los sumaba: todos los totales salían multiplicados exactamente por `AGGREGATION_AMOUNT`. Con `AGGREGATION_AMOUNT=1` el broadcast era un no-op, por eso los escenarios 1 a 3 no lo detectaban.

Cada `Aggregation` espera `SUM_AMOUNT` mensajes `eof` por cliente antes de armar su top parcial. Esto es lo que la hace **independiente del orden** entre las réplicas de `Sum`, que publican por conexiones distintas y entre las cuales no hay ninguna garantía de orden.

En cambio, los datos y el `eof` de **una misma** réplica salen por la misma instancia de `ExchangeMiddleware`, así que llegan a la cola de cada `Aggregation` en ese orden.

Agrupar por shard trajo mejoras ya que `sendTotals` pasó de publicar un mensaje AMQP **por fruta** a uno **por shard** lo que redujo significativamente la cantidad de mensajes debido a que la cantidad de frutas es mucho mayor que la cantidad de shards.

### 4.4 Coordinación entre instancias de `Aggregation` y `Join`

`Join` espera `AGGREGATION_AMOUNT` mensajes `eof` por cliente, acumula sumando por fruta y emite el top final. La decisión de diseño es que este borde **reusa el mismo mecanismo** que el anterior, una etapa sabe que su entrada está completa cuando contó tantos `eof` como instancias tiene la etapa que la precede. El único parámetro que cambia es la cardinalidad.

Eso mantiene a `Aggregation` y a `Join` con la misma forma y hace que ninguna de las dos dependa del orden en que llegan sus predecesoras.

## 5. Escalabilidad

### 5.1 Respecto de los clientes

El `clientID` viaja en **todos** los mensajes del protocolo interno y cada nodo mantiene su estado indexado por cliente. Ninguna consulta comparte estado con otra, así que se resuelven concurrentemente sin coordinación adicional, la concurrencia la da el middleware y los nodos sólo tienen que no mezclar.

Luego, el estado de un cliente se libera en cuanto se emite su resultado, de modo que el costo en memoria acompaña a los clientes **activos**, no a los atendidos.

El fin de archivo también es por cliente, cada `eof` y cada conteo están atados a un `clientID`, así que un cliente que termina no interfiere con otro que todavía está enviando.

### 5.2 Respecto de grandes volúmenes de datos

La propiedad importante es que **la agregación es incremental**. `Sum`  guarda un acumulador por fruta y `Aggregation` hace lo mismo sobre su partición, entonces, la memoria de cada nodo es proporcional a la cantidad de **frutas distintas** por cliente activo. Esto implica que un dataset diez veces más grande no cambia la memoria de ningún nodo.

Como consecuencia, el tráfico interno tampoco crece con el volumen:

| Tramo | Mensajes por cliente | Depende del volumen |
|---|---|---|
| `input_queue` -> `Sum` | uno por registro | sí |
| `Sum` -> `Aggregation` | cantidad de Aggregation de datos y de `eof`, por réplica | no |
| `Aggregation` -> `Join` | 1 top + 1 `eof`, por réplica | no |
| `Join` ->  salida | 1 top | no |


### 5.3 Respecto de la cantidad de nodos 

Cada nodo conoce lo mínimo indispensable del resto:

| Nodo | Qué necesita saber | Para qué |
|---|---|---|
| `Sum` | `SUM_AMOUNT` | a cuántos pares avisar el fin de archivo |
| `Sum` | `AGGREGATION_AMOUNT`, `AGGREGATION_PREFIX` | en cuántas particiones repartir y cómo nombrarlas |
| `Aggregation` | `SUM_AMOUNT` | cuántos `eof` esperar |
| `Aggregation` | `ID`, `AGGREGATION_PREFIX` | qué partición le toca |
| `Join` | `AGGREGATION_AMOUNT` | cuántos `eof` esperar |

Ninguno conoce la identidad de sus pares, sólo **cuántos** son. Ningún nombre de cola, exchange o routing key está escrito en el código, todos se derivan de las variables de entorno.

Los mecanismos además resuelven correctamente el caso trivial, con una sola instancia no hay avisos que
publicar y el particionamiento tiene una sola partición.

## 6. Manejo de señales

Los tres nodos registran `SIGINT`/`SIGTERM` en una goroutine dedicada y responden llamando a
`StopConsuming()`, **no a `Close()`**.

`StopConsuming` cancela la suscripción pero deja terminar el mensaje que está siendo procesado, que alcanza a hacer su `ack`. Recién cuando el consumo termina se cierran las conexiones. Cerrar sin antes llamar a este método mataría el canal con un mensaje a medio procesar.
