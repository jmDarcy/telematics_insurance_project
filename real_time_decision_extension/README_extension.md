# Real-Time Decision Extension

Ten folder dodaje do projektu warstwe, ktorej brakowalo w podstawowej wersji: realna decyzje biznesowa podejmowana w trakcie naplywu danych telematycznych.

Podstawowy projekt nadal robi poprawna rzecz aktuarialna: pojedynczy event nie zmienia skladki, a model GLM i skladka techniczna sa liczone okresowo na agregatach. To rozszerzenie dodaje osobny komponent operacyjny:

```text
surowe zdarzenia telematyczne
-> flagi ryzyka w czasie rzeczywistym
-> sliding window 5 minut
-> decyzja biznesowa tu i teraz
-> topic Kafka driver_interventions
```

## Co zostalo dodane

Pliki:

- `realtime_decision_engine.py` - Spark Structured Streaming, ktory publikuje decyzje do Kafka topic `driver_interventions`.
- `intervention_consumer.py` - prosty konsument Kafka do podgladu decyzji.
- `offline_intervention_demo.py` - demonstracja bez Kafka/Spark, przydatna na prezentacji awaryjnej.
- `create_extension_topics.sh` - tworzy topic `driver_interventions`.
- `README_extension.md` - ten opis.

Nowy topic Kafka:

```text
driver_interventions
```

## Rodzaje decyzji

Rozszerzenie generuje trzy glowne typy decyzji.

### 1. CHECK_DRIVER_STATUS

Decyzja natychmiastowa dla zdarzen wygladajacych jak potencjalna kolizja albo bardzo ryzykowny manewr, np. ekstremalne hamowanie.

Przyklad biznesowy:

```text
Wyslij push/SMS: "Czy wszystko w porzadku?"
Przygotuj wstepny case assistance.
```

To nie jest wycena skladki. To operacyjna reakcja ubezpieczyciela w trakcie jazdy.

### 2. SEND_SAFETY_NUDGE

Decyzja dla aktywnej podrozy, gdy w ostatnich minutach wystepuje zbyt duzo ryzykownych sygnalow: przekroczenia predkosci, telefon, ostre hamowanie, jazda nocna lub zla pogoda.

Przyklad biznesowy:

```text
Wyslij komunikat w aplikacji:
"Widzimy podwyzszone ryzyko tej podrozy. Jedz ostrozniej, zeby utrzymac bonus."
```

W projekcie to jest najwazniejszy element real-time analytics: system nie tylko rejestruje dane, ale probuje ograniczyc ryzyko zanim powstanie szkoda.

### 3. GRANT_SAFE_DRIVING_POINTS

Decyzja pozytywna dla spokojnej jazdy w aktywnym oknie czasowym.

Przyklad biznesowy:

```text
Dodaj punkty safe driving do portfela nagrod klienta.
```

To daje lepsza narracje niz karanie kierowcy po jednym zdarzeniu. Ubezpieczyciel moze budowac relacje, nagradzac dobre zachowanie i dawac klientowi poczucie, ze telematyka sluzy takze jemu.

## Przewaga konkurencyjna

Standardowy ubezpieczyciel czesto widzi klienta dopiero przy zakupie polisy, odnowieniu albo szkodzie. W tym rozszerzeniu ubezpieczyciel staje sie aktywnym partnerem podczas jazdy:

- ostrzega, gdy ryzyko w aktywnej podrozy rosnie,
- pyta, czy wszystko w porzadku po potencjalnie groznym zdarzeniu,
- nagradza spokojna jazde bez czekania do konca miesiaca,
- moze szybciej uruchomic assistance lub likwidacje szkody,
- buduje poczucie opieki, a nie tylko kontroli.

Najprostsza narracja do prezentacji:

```text
Konkurencyjna przewaga nie polega na tym, ze skladka zmienia sie po kazdym hamowaniu.
Polega na tym, ze ubezpieczyciel potrafi zareagowac w momencie, kiedy klient faktycznie jedzie:
ostrzec, pomoc albo nagrodzic. To zmienia telematyke z narzedzia taryfikacji po fakcie
w usluge aktywnej prewencji i opieki.
```

## Jak uruchomic prezentacje z Kafka i Spark

Uruchom komendy z katalogu glownego projektu `telematics_insurance_project`.

### 1. Utworz topiki Kafka

Mozesz uzyc podstawowego skryptu:

```bash
bash scripts/create_topics.sh
```

Albo tylko topic rozszerzenia:

```bash
bash real_time_decision_extension/create_extension_topics.sh
```

### 2. Uruchom producenta danych

```bash
python producer_telematics.py --drivers 50 --events-per-second 10 --duration-seconds 300 --seed 2026
```

### 3. Uruchom real-time decision engine

Dla Spark 3.5 / Scala 2.12:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 real_time_decision_extension/realtime_decision_engine.py --show-console
```

Dla Spark 4.0 preview / Scala 2.13:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 real_time_decision_extension/realtime_decision_engine.py --show-console
```

### 4. Obserwuj decyzje biznesowe

Opcja przez Kafka CLI:

```bash
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic driver_interventions --from-beginning
```

Opcja przez Python:

```bash
python real_time_decision_extension/intervention_consumer.py --from-beginning
```

W wiadomosciach powinny pojawiac sie pola:

```text
decision_type
priority
recommended_action
customer_message
risk_score_event albo risk_score_5m
```

To jest moment prezentacyjny: mozna pokazac, ze system nie czeka na batchowy scoring skladki, tylko podejmuje dzialanie w czasie rzeczywistym.

## Demo awaryjne bez Kafka/Spark

Jesli srodowisko Kafka/Spark nie jest dostepne, uruchom:

```bash
python real_time_decision_extension/offline_intervention_demo.py
```

Skrypt wygeneruje syntetyczne eventy, zastosuje te sama logike decyzyjna w pamieci i zapisze wynik do:

```text
data/realtime_decision_extension/offline_interventions.jsonl
```

To demo nie zastapi architektury streamingowej, ale pozwala pokazac logike biznesowa decyzji.

## Jak to pokazac w filmie

Proponowana narracja:

1. Najpierw pokazujemy podstawowy pipeline: Kafka, Spark, agregaty, GLM i skladka.
2. Nastepnie mowimy, ze sama skladka jest decyzja okresowa, a nie real-time.
3. Pokazujemy rozszerzenie `real_time_decision_extension`.
4. Uruchamiamy producenta i `realtime_decision_engine.py`.
5. W topicu `driver_interventions` pokazujemy decyzje:
   - `SEND_SAFETY_NUDGE`,
   - `CHECK_DRIVER_STATUS`,
   - `GRANT_SAFE_DRIVING_POINTS`.
6. Podkreslamy, ze to sa decyzje operacyjne: ostrzezenie, pomoc, nagroda.
7. Dopiero potem wracamy do GLM i skladki jako dlugoterminowej oceny ryzyka.

## Ograniczenia

Reguly w tym folderze sa demonstracyjne. W realnym systemie nalezaloby dodac:

- deduplikacje komunikatow i cooldown, zeby klient nie dostawal zbyt wielu powiadomien,
- walidacje progow decyzyjnych na danych historycznych,
- zgody marketingowe i regulacyjne na komunikaty push/SMS,
- audyt decyzji i wyjasnialnosc reguly,
- monitoring false positives,
- integracje z aplikacja mobilna, assistance i CRM.

Najwazniejsze: rozszerzenie nie zmienia zasady aktuarialnej projektu. Pojedynczy event nadal nie zmienia skladki. Pojedynczy event moze jednak uruchomic natychmiastowa reakcje operacyjna, bo to jest inny typ decyzji biznesowej.
