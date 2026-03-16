# Stratégie Trend Following (implémentation actuelle)

## Objectif
Cette stratégie automatise des entrées/sorties sur actions US en paper trading via Alpaca, avec un contrôle du risque basé sur:

- tendance court terme (moyennes mobiles),
- take profit / stop loss,
- confirmations de signaux,
- monitoring temps réel.

---

## 1) Univers et mode d’exécution

- **Mode actif**: paper trading réel (pas de simulation interne).
- **Univers**:
  - soit manuel (liste de symboles),
  - soit dynamique depuis les positions actuelles Alpaca.
- **Refresh univers**: périodique (paramètre `universe_refresh_seconds`).

---

## 2) Paramètres principaux (défauts actuels)

- `capital_usd`: `10000`
- `short_window`: `5`
- `long_window`: `20`
- `poll_seconds`: `30`
- `take_profit_pct`: `0.5`
- `stop_loss_pct`: `0.4`
- `stop_loss_confirmations`: `2`
- `reentry_after_loss_enabled`: `true`
- `reentry_delay_minutes`: `30`

Note: les bornes de sécurité restent appliquées côté backend.

---

## 3) Règle d’entrée (BUY)

Pour chaque symbole surveillé:

1. Calcul de la moyenne mobile courte (`short_window`) et longue (`long_window`).
2. **Entrée** si `short_ma > long_ma`.
3. Taille de position selon budget disponible (capital investi fixe).

Événements loggés:

- `entry` (achat déclenché)
- `entry_skipped_budget` (budget insuffisant)

---

## 4) Règles de sortie (SELL)

Une position peut sortir sur:

- `take_profit`
- `stop_loss`
- `trend_reversal` (retournement de tendance)

### 4.1 Confirmation dynamique des signaux négatifs

Pour les sorties négatives (`stop_loss` / `trend_reversal`), on exige des confirmations consécutives:

- si **tendance journalière > 0%**: **10 confirmations**
- sinon: **3 confirmations**

Événement de suivi:

- `negative_sell_candidate` avec `hits/required_hits`.

### 4.2 Sortie partielle si tendance journalière positive

Quand un signal de vente négatif est confirmé **et** que la tendance journalière est positive:

- 1ère sortie: vente de **50%** (`..._partial_1`)
- si le signal continue et se re-confirme: vente des **50% restants** (`..._partial_2`)

Si la tendance journalière n’est pas positive: sortie complète directe.

---

## 5) Rebuy automatique après sortie gagnante

Après une **sortie complète** avec `pnl_usd > 0`:

- le profit est conservé dans une réserve interne `savings_usd`,
- le rachat automatique (`positive_exit_rebuy`) réinvestit uniquement le **coût d'entrée initial** de la portion vendue,
- la quantité rachetée peut donc être différente de la quantité vendue si le prix a évolué.

Important:

- pas de rebuy sur sortie perdante,
- pas de rebuy sur sortie partielle.
- le statut expose désormais `realized_margin_usd` et `savings_usd`.

### 5.1 Déclenchement différé après sortie perdante (RE-ENTRY)

Après une **sortie complète** avec `pnl_usd < 0` (ex: `stop_loss_partial_2`), la stratégie:

1. mémorise le ticket dans `pending_loss_reentries`,
2. attend `reentry_delay_minutes` (30 min par défaut),
3. recalcule la tendance depuis le prix de vente,
4. rachète la **même quantité** seulement si la tendance est positive,
5. abandonne sinon.

Événements associés:

- `loss_reentry_scheduled`
- `loss_reentry_executed`
- `loss_reentry_skipped_negative_trend`
- `loss_reentry_deferred`
- `loss_reentry_error`

---

## 6) Quantité vendue réelle

Lors d’une sortie, la stratégie tente d’utiliser la **quantité live broker** (Alpaca) pour éviter les reliquats liés à l’état interne.
Si indisponible, fallback sur quantité locale.

---

## 7) Monitoring et logs

### Monitoring principal

- ticks, latence cycle, ordres envoyés/erreurs,
- entrées/sorties,
- TP/SL/reversal,
- rebuy `OK/KO`,
- re-entry après perte en attente (`pending_loss_reentries`),
- marge réalisée globale,
- épargne cumulée (`savings_usd`),
- PnL cumulé par symbole,
- vérifications de vente en cours (`hits/required` par symbole).

### Journal de décisions

Le log affiche les événements décisionnels (pas seulement les ordres):

- démarrage/arrêt,
- candidats de vente avec compteur,
- entrées/sorties et motif,
- skips (budget/qty),
- erreurs.

Les événements `exit` et `rebuy_after_positive_exit` incluent aussi des informations de capital utiles (`entry_cost_usd`, `savings_usd`).

Les tickets de re-entry après perte exposent aussi des données de suivi (`sell_price`, `due_at_epoch`, `current_price`, `trend_since_exit_pct`).

Un message de vie périodique confirme que la boucle tourne même sans trade.

---

## 8) Endpoints utiles

- `POST /api/v1/strategy/trend-following/start`
- `POST /api/v1/strategy/trend-following/stop`
- `GET /api/v1/strategy/trend-following/status`
- `WS /api/v1/strategy/ws/trend-following`

---

## 9) Lecture rapide des motifs (reason)

- `trend_entry`: entrée BUY
- `take_profit`: sortie sur TP
- `stop_loss`: sortie sur SL
- `trend_reversal`: sortie sur inversion de tendance
- `*_partial_1`, `*_partial_2`: sorties partielles 50% / 50%
- `positive_exit_rebuy`: rachat auto après sortie gagnante
- `loss_reentry_after_delay`: rachat après perte et délai

---

## 10) Limites connues

- Les fills Alpaca peuvent être fragmentés (plusieurs lignes pour un ordre logique).
- Le cashflow positif d’un `SELL` côté Alpaca n’implique pas forcément un PnL positif.
- Le comportement dépend de la fréquence de `poll_seconds` (ici 30s), pas tick-par-tick.
- Les activités Alpaca (`/activities`) peuvent être vides ou retardées; l’UI Trading peut alors basculer sur les événements stratégie (`status.events`) pour afficher les mouvements.

---

## 11) Comportement UI Trading (implémenté)

- La liste des positions conserve localement les lignes vendues et les affiche en *italique*.
- Le clic sur une ligne affiche les mouvements du symbole avec P&L mouvement et P&L cumulé.
- L’affichage reste cohérent après refresh navigateur via persistance locale (positions vendues + symbole sélectionné).
