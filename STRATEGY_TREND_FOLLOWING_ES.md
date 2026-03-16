# Estrategia Trend Following (implementación actual)

## Objetivo
Esta estrategia automatiza entradas/salidas en acciones de EE. UU. en paper trading con Alpaca, con un control de riesgo basado en:

- tendencia de corto plazo (medias móviles),
- take profit / stop loss,
- confirmaciones de señales,
- monitoreo en tiempo real.

---

## 1) Universo y modo de ejecución

- **Modo activo**: paper trading real (sin simulación interna).
- **Universo**:
  - manual (lista de símbolos),
  - o dinámico a partir de las posiciones actuales de Alpaca.
- **Actualización del universo**: periódica (parámetro `universe_refresh_seconds`).

---

## 2) Parámetros principales (valores por defecto actuales)

- `capital_usd`: `10000`
- `short_window`: `5`
- `long_window`: `20`
- `poll_seconds`: `30`
- `take_profit_pct`: `0.5`
- `stop_loss_pct`: `0.4`
- `stop_loss_confirmations`: `2`
- `reentry_after_loss_enabled`: `true`
- `reentry_delay_minutes`: `30`

Nota: los límites de seguridad siguen aplicándose en backend.

---

## 3) Regla de entrada (BUY)

Para cada símbolo monitoreado:

1. Cálculo de la media móvil corta (`short_window`) y larga (`long_window`).
2. **Entrada** cuando `short_ma > long_ma`.
3. Tamaño de posición según presupuesto disponible (capital invertido fijo).

Eventos registrados:

- `entry` (compra ejecutada)
- `entry_skipped_budget` (presupuesto insuficiente)

---

## 4) Reglas de salida (SELL)

Una posición puede salir por:

- `take_profit`
- `stop_loss`
- `trend_reversal` (cambio de tendencia)

### 4.1 Confirmación dinámica de señales negativas

Para salidas negativas (`stop_loss` / `trend_reversal`), se exigen confirmaciones consecutivas:

- si **tendencia diaria > 0%**: **10 confirmaciones**
- en caso contrario: **3 confirmaciones**

Evento de seguimiento:

- `negative_sell_candidate` con `hits/required_hits`.

### 4.2 Salida parcial si la tendencia diaria es positiva

Cuando una señal de venta negativa se confirma **y** la tendencia diaria es positiva:

- 1ª salida: venta del **50%** (`..._partial_1`)
- si la señal continúa y se vuelve a confirmar: venta del **50% restante** (`..._partial_2`)

Si la tendencia diaria no es positiva: salida completa directa.

---

## 5) Recompra automática tras una salida ganadora

Después de una **salida completa** con `pnl_usd > 0`:

- la ganancia se conserva en una reserva interna `savings_usd`,
- la recompra automática (`positive_exit_rebuy`) reinvierte solo el **coste de entrada original** de la parte vendida,
- la cantidad recomprada puede ser distinta de la cantidad vendida si el precio cambió.

Importante:

- no hay recompra en salida perdedora,
- no hay recompra en salida parcial.
- el estado expone ahora `realized_margin_usd` y `savings_usd`.

### 5.1 Reentrada diferida tras salida perdedora (RE-ENTRY)

Después de una **salida completa** con `pnl_usd < 0` (ej.: `stop_loss_partial_2`), la estrategia:

1. guarda el ticket en `pending_loss_reentries`,
2. espera `reentry_delay_minutes` (30 min por defecto),
3. recalcula la tendencia desde el precio de venta,
4. recompra la **misma cantidad** solo si la tendencia es positiva,
5. descarta el ticket en caso contrario.

Eventos asociados:

- `loss_reentry_scheduled`
- `loss_reentry_executed`
- `loss_reentry_skipped_negative_trend`
- `loss_reentry_deferred`
- `loss_reentry_error`

---

## 6) Cantidad de venta real

En una salida, la estrategia intenta usar la **cantidad real del broker** (Alpaca) para evitar remanentes por desalineación del estado interno.
Si no está disponible, usa fallback con la cantidad local.

---

## 7) Monitoreo y logs

### Monitoreo principal

- ticks, latencia de ciclo, órdenes enviadas/errores,
- entradas/salidas,
- TP/SL/reversal,
- recompra `OK/KO`,
- reentrada tras pérdida en espera (`pending_loss_reentries`),
- margen realizada global,
- ahorro acumulado (`savings_usd`),
- PnL acumulado por símbolo,
- verificaciones de venta en curso (`hits/required` por símbolo).

### Registro de decisiones

El log muestra eventos de decisión (no solo órdenes):

- inicio/parada,
- candidatos de venta con contador,
- entradas/salidas y motivo,
- skips (presupuesto/cantidad),
- errores.

Los eventos `exit` y `rebuy_after_positive_exit` incluyen también datos útiles de capital (`entry_cost_usd`, `savings_usd`).

Los tickets de reentrada tras pérdida también exponen datos de seguimiento (`sell_price`, `due_at_epoch`, `current_price`, `trend_since_exit_pct`).

Un mensaje de vida periódico confirma que el loop sigue activo incluso sin trades.

---

## 8) Endpoints útiles

- `POST /api/v1/strategy/trend-following/start`
- `POST /api/v1/strategy/trend-following/stop`
- `GET /api/v1/strategy/trend-following/status`
- `WS /api/v1/strategy/ws/trend-following`

---

## 9) Lectura rápida de motivos (reason)

- `trend_entry`: entrada BUY
- `take_profit`: salida por TP
- `stop_loss`: salida por SL
- `trend_reversal`: salida por reversión de tendencia
- `*_partial_1`, `*_partial_2`: salidas parciales 50% / 50%
- `positive_exit_rebuy`: recompra automática tras salida ganadora
- `loss_reentry_after_delay`: recompra tras pérdida y espera

---

## 10) Limitaciones conocidas

- Los fills de Alpaca pueden llegar fragmentados (varias líneas para una orden lógica).
- Un cashflow positivo de `SELL` en Alpaca no implica necesariamente PnL positivo de la estrategia.
- El comportamiento depende de la frecuencia de `poll_seconds` (aquí 30s), no tick a tick.
- Las actividades Alpaca (`/activities`) pueden venir vacías o con retraso; la UI de Trading puede entonces usar eventos de estrategia (`status.events`) para mostrar movimientos.

---

## 11) Comportamiento UI Trading (implementado)

- La lista de posiciones conserva localmente las líneas vendidas y las muestra en *itálica*.
- El clic en una línea muestra los movimientos del símbolo con P&L por movimiento y P&L acumulado.
- La vista se mantiene consistente tras refrescar el navegador mediante persistencia local (posiciones vendidas + símbolo seleccionado).
