# Gaming Lakehouse Pipeline — Layihə İzahları

> Bu sənəd layihəni **biznes** və **texniki** tərəfdən tam izah edir.
> Dil: Azərbaycan · Yeniləndi: 2026-10-09 (Phase 2: ClickHouse anbarı + Superset dashboard)

---

## 1. BİZNES TƏRƏFİ

### 1.1 Hansı problemi həll edirik?

Bir oyun şirkətini təsəvvür edin: minlərlə oyunçu hər gün mobil cihazlardan oyuna girir,
bəziləri oyun içindən alış-veriş edir (mikro-tranzaksiya), bəziləri bir neçə dəqiqədən
sonra oyunu bağlayır və bir daha qayıtmır. Şirkətin qarşısında üç sual durur:

1. **Pul haradan gəlir?** — Hansı oyunlar və hansı oyunçu seqmentləri gəlir gətirir?
2. **Oyunçuları itiririk?** — Hansı mərhələdə, hansı əlamətlərlə çıxırlar (churn)?
3. **Kampaniya pulunu haraya qədirik?** — Retensiya (geri qaytarma) və marketinq
   büdcəsini hansı seqmentə yönəltmək ən çox gəlir gətirər?

Bu suallara cavab **məlumatların özündədir**, amma biznes üçün əlçatmazdır: hadisələr
oyun serverlərində (source OLTP bazasında) yaşayır, analitiklər isə daha yavaş, tarixi
sorğular üçün ayrı bir sistemdə işləməlidir. Əgər bu iki dünya arasında **etibarlı,
avtomatik axın** yoxdursa, komanda həftəlik hesabatları əl ilə SQL sorğuları və Excel
eksportları ilə yığır — həm yavaşı, həm də səhvlidir (müxtəlif insanlar müxtəlif "DAU"
rəqəmləri görür).

### 1.2 Layihə biznesə nə verir? (dəyər təklifi)

| Dəyər | İzah |
|---|---|
| **Gəlir perspektivi ilə davranış** | Sessiya (pulsuz davranış) və alış-veriş (pul) hadisələri **bir** kurasiya modelində birləşir — "hansı seqment ödəyir" sualı üç analitikin bir həftəlik əl işi yox, hazır cədvəldir. |
| **Erkən churn siqnalı** | `agg_player_daily_engagement` (Spark, Phase 3) hər oyunçunun gündəlik aktivliyini verir. Sessiya başlayıb təkrar oynamayan seqment **hələ itkin çatmayana qədər** retensiya kampaniyasına düşür — geri qaytarmaq itkinlərdən 5–25 dəfə baha başa gəlir. |
| **Büdcə sübutla bölünür** | Haftalıq Growth iclası kampaniya pulunu eyni kurasiya cədvəllərindən bölür — finance rəqəmləri ilə uzlaşan, test olunmuş, **bir** mənbədən. |
| **Bir etibarlı "player truth"** | Raw lay qeydiyyatdan keçmiş hər hadisəni itirmir (append-only), kurasiya layı dbt testləri ilə yoxlanılır. Product, finance və marketinq eyni `player_id` açarlı rəqəmləri görür. |
| **Yeni sual ucuzlaşır** | CDC axını qurulduqdan sonra yeni analitik sual ("A/B cohort-u olanlar daha çox alış edirmi?") mövcud tarixin üstündə yeni bir dbt model deməkdir — yeni bir backfill layihəsi deyil. |

**Kim istifadə edir:** Product/Growth analitik komandası (əsas), finance (uzlaşma),
özü də gələcəkdə ML komandası (churn proqnozu üçün hazırlanmış xüsusiyyətlər).

### 1.3 Biznes kimi necə istifadə olunur?

Growth analitiki səhər Superset-i açır (`http://localhost:8088`) və dashboard-da görür:
bu gün neçə event axıb, neçə alış-veriş olub, hansı ölkələrdə aktivlik artır, hansı
oyunlarda təkrar oynama düşüb. Yeni kampaniya başlayanda kampaniya ID-si ilə filtr
qoyub təsiri ölçür. Heç bir mühəndisə "mənə bu cədvəli eksport edərsən" demir.

---

## 2. ARXİTEKTURA (böyük şəkil)

```
Oyun (sintetik generator)                  ──►  Source PostgreSQL (OLTP)
                                                │  wal_level=logical
                                                ▼
                                           Debezium (Kafka Connect)  ── CDC ──►  Apache Kafka (topic)
                                                                                        │
                                             ┌──────────────────────────────────────────┘
                                             │ Airflow task: land_raw_kafka_to_rustfs
                                             ▼
                                        RustFS (S3-uyumlu raw zona, NDJSON fayllar)
                                             │
                                             │ Airflow task: load_raw_to_clickhouse
                                             ▼
                                        ClickHouse — RAW bazası (ReplacingMergeTree)
                                             │
                                             │ Airflow task: dbt_run_staging
                                             ▼
                                        ClickHouse — STAGING (tipləşdirilmiş 1:1 view)
                                             │
                                             │ Airflow task: dbt_test_staging  ◄── DQ darvazası
                                             ▼
                                        ClickHouse — CURATED (dim_player, fct_in_game_transaction)
                                             │
                                             │ Airflow task: publish_serving
                                             ▼
                                        ClickHouse — SERVING (SQL view) + RustFS-də CSV hesabat
                                             │
                                             ▼
                                  Apache Superset — analitik dashboard (8088)
```

**Əsas ideya:** hadisələr **axınla** (CDC) Kafka-ya çatıır; Airflow isə hər gün
(əl ilə də trigger oluna bilər) **batch** kəsimini işə salır: Kafka → RustFS →
ClickHouse → dbt → serving. Generator və CDC orkestr edilmir — onlar müstəqil,
dayanmadan işləyir (real oyundakı kimi).

### 2.1 Komponent rolu cədvəli (pipeline ↔ komponent)

| Pipeline rolu | Komponent | Status |
|---|---|---|
| Mənbə sistem | Python generator → source Postgres (`source.game_events`) | ✅ |
| CDC / axın | Debezium PostgreSQL connector (Kafka Connect) | ✅ |
| Yenidən oxunaq bufer | Kafka topic `gaming.source.game_events` | ✅ |
| Raw zona (data lake) | RustFS bucket (`raw/game_events/dt=<gün>/`) | ✅ |
| Anbar | **ClickHouse** (raw / staging / curated / serving bazaları) | ✅ |
| Tipləşdirmə / kurasiya | dbt (dbt-clickhouse adapter) + DQ testləri | ✅ |
| Batch hesablama | Spark (Phase 1-də connectivity proof; real S3A işi Phase 3) | 📋 |
| Orkestrasiya | Airflow `gaming_pipeline` DAG (logical-date parametrli) | ✅ |
| Serving | `serving.v_player_engagement_daily` view + CSV hesabat | ✅ |
| Dashboard | Apache Superset (ClickHouse-a qoşulur) | ✅ |

---

## 3. TEXNİKİ DETALLAR

### 3.1 Niyə anbar olaraq ClickHouse? (PostgreSQL-dən keçid qərarı)

Phase 1-də anbar PostgreSQL 16.4 idi. Phase 2-də **ClickHouse 24.8**-ə keçid edildi.
Səbəblər:

| Meyar | PostgreSQL | ClickHouse |
|---|---|---|
| Analitik sorğu sürəti | Sətir-store: böyük `GROUP BY` / zamanda kəsiliş yığımı yavaşıdır | Sütun-store + vektor icra: milyonlarla sətirdə sayıntı/sum millisanıyələrdə |
| Aqreqasiya funksiyaları | Standart SQL | `countIf`, `uniq`, `quantile`, `argMax` — analitika üçün doğma |
| Gündəlik yükləmə nümunəsi | `INSERT ... ON CONFLICT` sətir-sətir yavaş | `client.insert` batch (row-binary) — on minlərlə saniyələrdə |
| Idempotency | `ON CONFLICT DO NOTHING` | `ReplacingMergeTree` + eyni ORDER BY açarı ilə dəyişməz-ləşdirmə |
| OLTP yükü | Yaxşı (amma bu anbar deyil, source-da qaldı) | OLTP üçün yaramaz — ona görə source PostgreSQL-də QALDI |
| dbt dəstəyi | Əla | `dbt-clickhouse` rəsmi adapteri |

**Vacib nüans:** source (mənbə) bazası PostgreSQL olaraq qalır — o, oyunun canlı OLTP
bazasını təmsil edir. ClickHouse yalnız analitik anbardır. Bu, real world şəbilərinin
düzgün ayrılmasıdır: OLTP sistemə analitik yüklə qoşulmaz.

**Keçidin təsiri kodda:** `load_raw_to_postgres` → `load_raw_to_clickhouse`
(clickhouse-connect batch insert), dbt profili `dbt-postgres` → `dbt-clickhouse`,
serving view ClickHouse dialektində. Köhnə `POSTGRES_*` dəyişənləri geriyə uyğunluq
üçün saxlanılır (deprecated).

### 3.2 ClickHouse-da cədvəl mühərriki: niyə ReplacingMergeTree?

Raw cədvəl belə yaradılır (DAG `RAW_TABLE_DDL`):

```sql
CREATE TABLE raw.game_events (...) 
ENGINE = ReplacingMergeTree
ORDER BY (source_partition_date, event_id)
```

- **ReplacingMergeTree**: eyni ORDER BY açarı ilə (`source_partition_date`, `event_id`)
  təkrar insert olunsa, merge zamanı təkrarlar bir sətrə yığılır → **yenidən işə
  salma idempotentdir** (eyni günü 2 dəfə yükləsən, dublikat yaranmır).
- Raw lay "dumb" (elə-gəldiyi-kimi) saxlanılır: `player_signup_date` Debezium-un
  göndərdiyi epoch-days int kimi **string** olaraq qalır, `source_partition_date`
  string dt açarıdır. Tipləşdirmə staging-də olur.

### 3.3 Idempotency (yenidən işə salma təhlükəsizliyi) — hər qatda

| Qat | Mexanizm |
|---|---|
| RustFS raw obyektlər | Fayl adında Kafka **offset aralığı**: `raw/game_events/dt=2026-10-09/0-199.jsonl`. Yenidən oxuyanda eyni açar üstə yazır. |
| ClickHouse raw | `ReplacingMergeTree` — eyni `event_id` təkrarı merge-də yığılır. |
| Staging | Materalized **view** — həmişə raw-dan hesablanır, dublikat ola bilməz. |
| Curated | dbt **table** materializasiyası — hər run-da tam yenidən qurulur (`CREATE OR REPLACE`). |
| Serving | `CREATE OR REPLACE VIEW` + deterministik CSV açarı (`dt=<gün>/report.csv`). |

### 3.4 Dirty record siyasəti (çirkli məlumat)

Generator təsadüfi olaraq (5–10%) pis qeydlər yaradır: null `player_id`, pozulmuş
timestamp, eksik sahələr. Siyasət:

- **Heç bir sətir atılmır.** Raw-dan staging-ə hamısı keçir.
- Pozulmuş timestamp → staging-də `event_time = NULL` (bayraq edilir, sayılır).
- `player_id` UUID-ə çevrilməyəndə → NULL (cədvəl `Nullable(UUID)`).
- **Uzlaşma hesabatı** (smoke test) fərqi izah edir: məsələn `staging 5000, fct 1681`
  fərqi = sessiya eventləri + NULL-timestamplu transaction-lar. Gizlədilmir, izahlanır.

### 3.5 DQ (Data Quality) darvazası

DAG-da `dbt_test_staging` taskı **kurated və serving-dən əvvəl** gəlir — testlər
keçməsə, downstream tasklar skip olunur və run `failed` işarələnir. Mövcud yoxlamalar:

1. `not_null(stg_game_events.event_id)` — PK boş ola bilməz
2. `unique(stg_game_events.event_id)` — dedup doğruluğu
3. `accepted_values(event_type ∈ {session_start, session_end, in_game_transaction})`
4. `assert_staging_has_rows` (singular test) — boş raw üzərində run yayımlamaqsız dayanır
5. `not_null/unique` curated modellərdə (dim_player.player_id, fct.event_id)

### 3.6 Logical-date parametrasiyası

DAG bütün tasklarda Airflow templating dəyişəni `{{ ds }}` (run-un logical tarixi)
işlədir: consumer `dt=<ds>` partisiyasına yazır, raw loader həmin günün obyektlərini
yükləyir, serving CSV `dt=<ds>` açarına gedir. Nəticə: **hansı gün üçün işlədiyini
bilmək üçün komanda tarixçəsinə baxmaq kifayətdir** və köhnə günü yenidən
(reprocess) etmək üçün həmin tarixə trigger etmək bəs edir.

### 3.7 Airflow task qrafı

```
land_raw_kafka_to_rustfs ─► load_raw_to_clickhouse ─► dbt_run_staging ─► dbt_test_staging ─► dbt_run_curated ─► publish_serving
spark_connectivity_proof (paralel)
```

- **land_raw_kafka_to_rustfs**: Kafka istehlakçısı bounded işləyir — `max_batches=25`
  (≤5000 mesaj) + `idle_exit_seconds=20` (yeni mesaj gəlməsə dayan). Bu, Airflow
  taskının sonsuz qalmaması üçündür.
- **PIPELINE_FAIL_SWITCH=1** (.env-də): qəsdən xəta demo — ingestion taskı
  `AirflowFailException` atır, hamı downstream skip. Reviewer üçün sənədləşdirilib.
- Retry siyasəti: bütün tasklar 2 dəfə, 1 dəqiqə fəslə ilə.

### 3.8 Spark: niyə hələlik yalnız connectivity proof?

Phase 1 tələbi: hər komponent ya **pipeline-da istifadə olunur**, ya da
**sənədləşdirilmiş connectivity proof ilə örtülür**. Spark üçün real S3A batch işi
Phase 3-dür (RustFS-dən oxuyub `agg_player_daily_engagement` yazır); Phase 1-də
`spark_connectivity_proof` taskı Spark konteynerində real `spark-submit` işlədir
(HOME/ivy problemi həll edilmiş). Tam əsaslandırma: `docs/decisions/0001-spark-batch-s3a-phase3.md`.

### 3.9 Təhlükəsizlik və gizli məlumatlar

- Heç bir secret repoda yoxdur: `.env` gitignore-dadır, `.env.example` yer tutucularla.
- dbt profili `env_var()` ilə işləyir — repoya kommit oluna bilər.
- Airflow container-ların docker socket-i **read-only** mount-dur (yalnız Spark proof).
- Parollar `.env`-də generasiya təlimatları ilə (`openssl rand`, `secrets` modulu).

### 3.10 CI (GitHub Actions)

`.github/workflows/ci.yml`: hər PR/main push-da:
1. `ruff check .` — lint
2. `python -m compileall` — bütün modullar kompilyasiya olunur
3. `docker compose config --quiet` — infrastruktur tərifi etibarlıdır

---

## 4. SUPERSET DASHBOARD (analitik panel)

### 4.1 Niyə Superset?

Müqayisə (anbar analitika dashboard-ları üçün):

| Alət | Artıqlıq | Çatışmazlıq (bizim kontekstdə) |
|---|---|---|
| **Apache Superset** ✅ | Açıq kod, ClickHouse üçün rəsmi driver, "anlıq" chart avto-yenilənməsi (auto-refresh), SQL Lab (sorğu editing), pulsuz | İlk quraşdırma bir az çətin (indi compose ilə bir əmrdədir) |
| Metabase | Ən asan UI | ClickHouse rəsmi driver deyil (plugin), anlıq avto-refresh zəifdir |
| Grafana | Ən yaxşı "live" panellər | Daha çox metrik/alert_tool; analitik cədvəllər üçün Superset-dən əskidir |
| Tableau/PowerBI | Enterprise | Pul, lisenziya, dərs layihəsi üçün ağır |

"Anlıq event görmək" tələbi üçün Superset-in chart-level **auto-refresh** (10s/30s)
yete r: son N dəqiqənin event sayı, ölkə bazasında aktivlik, transaksiya həcmi
canlı yenilənir (mənbə: ClickHouse-dan birbaşa sorğu — sürətli, saniyələşdirilmiş gecikmə).

### 4.2 Quraşdırma (bir dəfə)

1. Stack qalxanda Superset avtomatik başlayır: `http://localhost:8088`
   (login: `.env`-də `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD`).
2. Sol menyu → **Database Connections** → **+ Database** → **ClickHouse (connect)**:
   - Host: `clickhouse`, Port: `8123`, Username: `CLICKHOUSE_USER` (.env-dən),
     Password: `CLICKHOUSE_PASSWORD`, Database: `CLICKHOUSE_DB` (gaming_lakehouse)
3. **Datasets** → `curated.dim_player`, `curated.fct_in_game_transaction`,
   `serving.v_player_engagement_daily`, `staging.stg_game_events` əlavə edin.
4. Dashboard yarat: "Gaming Overview" → chartlar:

| Chart | Növ | Sorğu (qısa) |
|---|---|---|
| Saatlıq event axını (canlı, 10s refresh) | Line / Bar | `SELECT toStartOfHour(event_time) h, count(*) FROM staging.stg_game_events WHERE event_time > now() - INTERVAL 6 HOUR GROUP BY h` |
| Ölkə üzrə aktiv oyunçu | Bar | `staging` üzərindən `uniq(player_id)` by `player_country` |
| Gəlir (USD) oyun üzrə | Bar | `fct_in_game_transaction` by `game_id` (staging-dən join) |
| Top xərcləyən oyunçular | Table | `serving.v_player_engagement_daily` ORDER BY `total_amount_usd DESC` |
| Churn siqnalı (sessiya başa vurmayanlar) | Big Number | `session_end` sayı / `session_start` sayı |

### 4.3 "Anlıq" necə işləyir (dürüst gecikmə hesabatı)

Axın gecikməsi = generator→Debezium (saniyələr) + consumer batch (200 mesaj və ya
20s idle) + Airflow run intervalı. Dashboard **canlıya yaxın**dır: son dəqiqələrin
event-ləri bir neçə dəqiqəyə dashboard-da görünür (batch arxitekturasının təbiəti).
Tam saniyə-saniyə canlılıq Phase 2+ streaming kəsimi (Kafka → ClickHouse push)
ilə gələcək.

---

## 5. NECƏ İŞƏ SALINIR (əmrlər xülasəsi)

```bash
cp .env.example .env            # dəyərləri doldur (Fernet, secret key, parollar)
make up                         # stack qalxır (ilk dəfə Airflow image build olunur ~3-5 dəq)
make register-connector         # Debezium CDC (idempotent)
make generate COUNT=5000 RATE=1000   # source-a 5000 event yaz
make rerun                      # DAG-ı bugün üçün trigger et (və ya Airflow UI-dən)
make smoke                      # uc-u-cərvə smoke test + qat yoxlaması
```

Brauzerlər: Airflow `8080` · Superset `8088` · Kafka UI `8085` · Spark UI `8081` ·
RustFS konsol `9001` · ClickHouse HTTP `8123`.

Yoxlama (anbar qatları):

```bash
docker exec -it gaming-clickhouse clickhouse-client \
  --query "SELECT 'raw', count(*) FROM gaming_lakehouse.raw.game_events
           UNION ALL SELECT 'staging', count(*) FROM gaming_lakehouse.staging.stg_game_events
           UNION ALL SELECT 'fct', count(*) FROM gaming_lakehouse.curated.fct_in_game_transaction"
```

---

## 6. FAZALAR VƏ STATUS

| Faza | Əhatə | Status |
|---|---|---|
| Phase 0 | İnfrastruktur + skeleton | ✅ |
| Phase 1 | Walking skeleton: Kafka→RustFS→raw→dbt→serving, DQ, smoke, CI | ✅ |
| Phase 2 | **ClickHouse anbarı + Superset dashboard** (bu dəyişiklik) | ✅ |
| Phase 3 | Real Spark S3A batch (`agg_player_daily_engagement`) | 📋 |
| Phase 4 | Streaming kəsimi + BI/ML genişlənmə | 📋 |

## 7. Risklər və qeydlər

- `bitnamilegacy/spark:3.5.3` — Bitnami 2025 registry dəyişikliyi səbəbilə legacy
  repo; Phase 3-də rəsmi `apache/spark` image-ə keçid tövsiyə olunur.
- Superset image-ə `clickhouse-connect` boot-da pip ilə quraşdırılır (ilk start
  internet tələb edir). Tam offline üçün custom image tövsiyə olunur.
- ClickHouse host port 9004 ← konteyner 9000 (RustFS 9000-i tutur).
- LocalExecutor birdən çox taskı paralel işə sala bilir; tasklar müstəqil
  idempotent olduğundan təhlükəsizdir.
