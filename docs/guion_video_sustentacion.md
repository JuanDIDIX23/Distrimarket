# Guion del video de sustentación — DistriMarket

**Duración objetivo:** 10–12 minutos · **Formato:** grabación de pantalla con voz (OBS Studio, gratuito) ·
**Material:** presentación de 13 diapositivas (con notas del orador) + terminal + navegador en `http://localhost:8000`.

> Antes de grabar: `docker compose up -d --build` (o `scripts/stack.sh start`), abrir el navegador
> con la sesión cerrada, una terminal con letra grande y la presentación en la diapositiva 1.

| Tiempo | Diapositiva / pantalla | Qué decir (resumen) | Qué mostrar |
|---|---|---|---|
| 0:00–0:40 | 1. Portada | Saludo, nombre, asignatura, docente. «Presento DistriMarket, una plataforma de pedidos distribuida.» | Diapositiva |
| 0:40–1:40 | 2. Problemática | Tres problemas de los monolitos en comercio en línea: sobreventa bajo concurrencia, punto único de fallo, pedidos duplicados o perdidos. Pregunta guía. | Diapositiva |
| 1:40–2:20 | 3. Objetivos | Objetivo general y los objetivos específicos en una frase cada uno. | Diapositiva |
| 2:20–3:50 | 4. Arquitectura | Recorrer el diagrama de izquierda a derecha: cliente → gateway → 5 servicios → 4 bases de datos; Redis Streams para eventos. Explicar por qué la reserva es síncrona y la notificación asíncrona. | Diagrama |
| 3:50–4:40 | 5. Flujo de un pedido | Los pasos: idempotencia, reserva atómica, outbox, evento, notificación. Mostrar el `UPDATE ... WHERE stock >= cantidad`. | Diapositiva con código |
| 4:40–6:10 | 6. Demostración → **demo 1 en vivo** | Iniciar sesión, mostrar que el subtítulo del catálogo alterna entre inventory-1 y inventory-2 al recargar (balanceo), crear un pedido, ver stock descontado y la notificación que llega por evento. | Navegador |
| 6:10–7:40 | 6. Demostración → **demo 2 en vivo** | En la terminal: `docker compose stop inventory-1` → recargar catálogo: sigue funcionando y el panel muestra la réplica caída. `docker compose stop notifications` → crear pedido: se confirma. `docker compose start notifications` → la notificación aparece. Volver a encender inventory-1. | Terminal + navegador |
| 7:40–8:40 | 7–9. Pruebas, tolerancia a fallos y rendimiento | 36/36 pruebas, 0 sobreventas con 100 compras simultáneas, 7/7 escenarios de fallo, circuit breaker de 157 ms a 4 ms, rendimiento con 10 usuarios (p95 33 ms y 73 ms). Mencionar los dos errores reales que encontraron las pruebas. | Diapositivas + gráfica |
| 8:40–9:20 | 10. Seguridad | JWT, bcrypt, autorización por objeto, límite de intentos, un solo puerto expuesto. | Diapositiva |
| 9:20–10:00 | 11. Presupuesto | COP 7.035.000 de desarrollo (costo de oportunidad), COP 1.186.052 de operación el primer año, desembolso real COP 0. | Diapositiva |
| 10:00–11:00 | 12. Conclusiones y trabajo futuro | 3 aprendizajes: atomicidad local basta; elegir bien síncrono/asíncrono; idempotencia obligatoria. Limitaciones: PostgreSQL y Redis únicos, rendimiento limitado por CPU. Trabajo futuro. | Diapositiva |
| 11:00–11:20 | 13. Cierre | Agradecimiento y enlace al repositorio. | Diapositiva |

## Texto sugerido para los momentos clave

**Arquitectura (2:20).** «El cliente solo conoce un punto de entrada: el API Gateway en el puerto 8000. Detrás hay cinco
servicios independientes, cada uno con su propia base de datos. El inventario corre en dos réplicas. Cuando un cliente
compra, la reserva de stock es síncrona, porque necesita saber al instante si su pedido se confirmó; la notificación es
asíncrona, por eventos en Redis Streams, porque puede esperar unos segundos. Gracias a esa separación, si el servicio de
notificaciones cae, la tienda sigue vendiendo.»

**Demo de fallos (6:10).** «Ahora voy a apagar una réplica de inventario mientras uso la aplicación… El catálogo sigue
funcionando y el panel de estado marca la réplica como caída. El gateway detectó el error y envió el tráfico a la otra
réplica. Ahora apago el servicio de notificaciones y hago un pedido: se confirma igual. El evento quedó guardado en el
stream. Al volver a encender el servicio, la notificación aparece sola.»

**Hallazgo de pruebas (7:40).** «Las pruebas de concurrencia encontraron un error real: con 20 peticiones simultáneas con
la misma clave de idempotencia, las que perdían la carrera ejecutaban la compensación y devolvían el stock del pedido
ganador. Lo corregí tratando ese caso como una repetición, sin compensar. Este tipo de error no aparece en pruebas
secuenciales.»

## Lista de verificación antes de entregar

- [ ] Audio claro, sin ruido; letra de la terminal ≥ 16 pt.
- [ ] Se ven las dos demos completas, incluida la recuperación.
- [ ] Duración ≤ 12 minutos.
- [ ] Exportar en MP4 1080p y subir al enlace indicado por el docente.
