# Telematics Insurance RTA Project

Projekt demonstruje architekture usage-based insurance / pay-how-you-drive z Kafka, Spark Structured Streaming, Pythonem i modelem GLM. Dane sa syntetyczne i sluza do pokazania przeplywu real-time analytics, a nie do prawdziwej taryfikacji.

Projekt zawiera tez rozszerzenie `real_time_decision_extension`, ktore dodaje realna decyzje biznesowa podejmowana w czasie rzeczywistym: ostrzezenie kierowcy, sprawdzenie statusu po potencjalnie groznym zdarzeniu albo przyznanie punktow za bezpieczna jazde. To rozszerzenie nie zmienia skladki po pojedynczym evencie; jest osobna warstwa operacyjna.

## Cel biznesowy

System generuje zdarzenia telematyczne kierowcow, wykrywa ryzykowne zachowania, agreguje cechy w oknach czasowych, trenuje interpretowalny Poisson GLM i okresowo aktualizuje demonstracyjna skladke techniczna.

W rozszerzeniu real-time decisioning system moze rowniez podjac natychmiastowa decyzje operacyjna:

```text
surowe zdarzenia telematyczne
-> flagi ryzyka i sliding window 5 minut
-> decyzja: ostrzec, sprawdzic status, nagrodzic albo nie robic nic
-> Kafka topic driver_interventions
```

Poprawna logika projektu:

```text
surowe zdarzenia telematyczne
-> reguly i flagi ryzyka
-> agregaty w oknach czasowych
-> tabela cech kierowcy
-> batchowe trenowanie GLM
-> scoring kierowcy
-> okresowa aktualizacja technicznej skladki
```

Pojedyncze zdarzenie nie zmienia skladki mechanicznie. Zdarzenia sa tylko sygnalem do agregatow i modelu.

## Struktura

```text
telematics_insurance_project/
|-- producer_telematics.py
|-- spark_streaming_features.py
|-- train_glm.py
|-- score_premiums.py
|-- app.py
|-- real_time_decision_extension/
|   |-- realtime_decision_engine.py
|   |-- intervention_consumer.py
|   |-- offline_intervention_demo.py
|   |-- create_extension_topics.sh
|   `-- README_extension.md
|-- notebooks/
|   `-- 01_telematics_project.ipynb
|-- data/
|   |-- historical_features/
|   |-- model_outputs/
|   |-- premium_history/
|   `-- checkpoints/
|-- models/
|-- scripts/
|   `-- create_topics.sh
|-- README.md
`-- requirements.txt
```

## Technologie

- Python, pandas, numpy
- Apache Kafka przez `kafka-python`
- PySpark / Spark Structured Streaming
- Spark-Kafka connector
- statsmodels Poisson GLM z offsetem ekspozycji
- opcjonalnie FastAPI do serwowania ostatniej skladki

## Kafka topics

```bash
kafka-topics.sh --create --if-not-exists --topic telematics_raw --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic telematics_alerts --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic driver_features --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic premium_updates --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic driver_interventions --bootstrap-server broker:9092
kafka-topics.sh --list --bootstrap-server broker:9092
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic telematics_raw --from-beginning --max-messages 5
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic telematics_alerts --from-beginning --max-messages 5
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic driver_interventions --from-beginning --max-messages 5
```

Mozna tez uzyc:

```bash
bash scripts/create_topics.sh
```

## Uruchomienie

1. Uruchom srodowisko Docker/JupyterLab z Kafka dostepna pod `broker:9092`.
2. Zainstaluj zaleznosci:

```bash
pip install -r requirements.txt
```

3. Utworz tematy Kafka:

```bash
bash scripts/create_topics.sh
```

4. Uruchom producenta:

```bash
python producer_telematics.py --drivers 50 --events-per-second 10 --duration-seconds 300 --seed 2026
```

5. Uruchom Spark Structured Streaming.

Dla Spark 3.5 / Scala 2.12:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_features.py
```

Dla Spark 4.0 preview / Scala 2.13:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 spark_streaming_features.py
```

6. Obserwuj alerty:

```bash
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic telematics_alerts --from-beginning
```

7. Po zebraniu agregatow wytrenuj GLM:

```bash
python train_glm.py
```

Jesli dane parquet ze streamingu jeszcze nie istnieja, skrypt wygeneruje demonstracyjne historyczne agregaty syntetyczne.

8. Policz skladki:

```bash
python score_premiums.py
```

9. Opcjonalnie opublikuj aktualizacje skladek do Kafka:

```bash
python score_premiums.py --publish-kafka
```

10. Opcjonalnie uruchom API:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

11. Opcjonalnie uruchom rozszerzenie real-time decisioning:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 real_time_decision_extension/realtime_decision_engine.py --show-console
```

Podglad decyzji:

```bash
python real_time_decision_extension/intervention_consumer.py --from-beginning
```

Szczegoly sa opisane w `real_time_decision_extension/README_extension.md`.

## Dane

Producent wysyla JSON do `telematics_raw`. Przykladowe pola:

```json
{
  "event_id": "EV000001",
  "driver_id": "D001",
  "vehicle_id": "V001",
  "driver_profile": "aggressive",
  "event_time": "2026-05-21T12:31:10.123Z",
  "speed_kmh": 72.4,
  "speed_limit_kmh": 50,
  "acceleration_ms2": 1.8,
  "braking_ms2": -3.5,
  "cornering_g": 0.42,
  "road_type": "urban",
  "weather": "rain",
  "is_night": false,
  "distance_delta_km": 0.12,
  "phone_usage": false
}
```

Profile `safe`, `average`, `aggressive`, `night_driver`, `urban_driver` zmieniaja rozklady predkosci, hamowania, przyspieszenia, jazdy noca i uzycia telefonu.

## Streaming

`spark_streaming_features.py`:

- czyta `telematics_raw`,
- parsuje JSON przez jawny schema,
- stabilnie konwertuje timestamp ISO z koncowka `Z`,
- tworzy flagi: `is_speeding`, `is_hard_braking`, `is_harsh_acceleration`, `is_sharp_cornering`, `is_night_risk`, `is_bad_weather`, `is_phone_usage`, `risk_event`,
- wysyla alerty do `telematics_alerts`,
- liczy tumbling window 1 minuta i sliding window 5 minut / krok 1 minuta,
- uzywa watermarka 30 sekund,
- zapisuje historyczne cechy do `data/historical_features/tumbling_1m`.

`real_time_decision_extension/realtime_decision_engine.py`:

- czyta ten sam topic `telematics_raw`,
- tworzy flagi ryzyka dla pojedynczego eventu,
- liczy aktywne sliding window 5 minut / krok 1 minuta,
- publikuje decyzje operacyjne do `driver_interventions`,
- rozroznia m.in. `SEND_SAFETY_NUDGE`, `CHECK_DRIVER_STATUS`, `GRANT_SAFE_DRIVING_POINTS`.

Ta czesc jest wlasciwym uzasadnieniem real-time analytics w projekcie: system podejmuje decyzje podczas aktywnej jazdy, zamiast tylko zbierac dane pod pozniejszy scoring.

## Model GLM

Model:

```text
claim_count ~ speeding_ratio
            + hard_braking_count_per_100km
            + harsh_acceleration_count_per_100km
            + night_event_ratio
            + bad_weather_event_ratio
            + phone_usage_count
            + offset(log(exposure_km))
```

Preferowany jest Poisson GLM `statsmodels` z linkiem logarytmicznym. Offset `log(exposure_km)` oznacza, ze modeluje sie czestosc szkod wzgledem ekspozycji, a nie sama liczbe szkod bez kontekstu przejechanych kilometrow. Jezeli w lokalnym srodowisku nie ma `statsmodels`, skrypt uzywa fallbacku `sklearn.linear_model.PoissonRegressor`, estymujac czestosc szkody i wazac obserwacje przez `exposure_km`.

Wyniki zapisywane sa do `data/model_outputs/`:

- `training_dataset.csv`
- `glm_coefficients.csv`
- `glm_test_predictions.csv`
- `glm_metrics.json`
- `glm_summary.txt`

Przy nadmiernej dyspersji, gdy wariancja liczby szkod jest istotnie wieksza niz srednia, Poisson moze byc zbyt prosty. Wtedy nalezy rozwazyc Negative Binomial albo Tweedie.

## Scoring i skladka

Scoring wylicza:

```text
risk_multiplier = predicted_frequency / average_predicted_frequency
technical_premium = base_premium * risk_multiplier
```

Domyslna skladka bazowa to 1000 PLN. Jedna aktualizacja skladki jest ograniczona limitem +/-10%, aby uniknac skokow po pojedynczej paczce danych. Historia trafia do:

```text
data/premium_history/premium_history.csv
```

## Wizualizacje

Notebook `notebooks/01_telematics_project.ipynb` zawiera komorki do wykresow:

1. liczba eventow ryzyka w czasie,
2. przekroczenia predkosci wedlug kierowcy,
3. risk score wedlug kierowcy,
4. predicted claim frequency,
5. techniczna skladka wedlug kierowcy,
6. zmiana skladki wybranych kierowcow w czasie,
7. porownanie profili safe / average / aggressive.

## Krytyczna ocena i ograniczenia

1. Dane sa syntetyczne, wiec nie wolno wyciagac realnych wnioskow taryfowych.
2. Prawdziwa skladka zalezy od wielu zmiennych poza telematyka.
3. Pojedyncze zdarzenia nie powinny mechanicznie zmieniac skladki.
4. GLM jest interpretowalny, ale moze byc zbyt prosty dla zlozonych relacji telematycznych.
5. Poisson moze byc niewystarczajacy przy nadmiernej dyspersji.
6. Telematyka rodzi problemy prywatnosci i wymaga silnej kontroli zgody, retencji i celu przetwarzania danych.
7. Spark/Kafka maja sens przy danych strumieniowych lub duzych wolumenach. W malej probce sa demonstracja architektury, a nie koniecznoscia obliczeniowa.
8. Rozszerzenie real-time decisioning uzywa prostych regul demonstracyjnych. W realnym systemie potrzebne bylyby deduplikacja komunikatow, cooldown, testy progow, audyt decyzji i integracja z aplikacja mobilna lub CRM.
