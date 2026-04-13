# ElasticRelay - Multi-Source CDC Gateway zu Elasticsearch

![ElasticRelay Screenshot](/releases/download/asset/screenshot_02.png)

<p align="center">
  <a href="https://github.com/yogoosoft/ElasticRelay/releases"><img src="https://img.shields.io/badge/version-v1.4.4-blue.svg" alt="Version"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-1.25.2+-00ADD8.svg" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="Lizenz"></a>
</p>
<p align="center">
  <a href="/README.md">English</a> |
  <a href="README.de.md">Deutsch</a> |
  <a href="README.fr.md">Français</a> |
  <a href="README.ja.md">日本語</a> |
  <a href="README.ru.md">Русский</a> |
  <a href="README.zh-CN.md">中文</a>
</p>

## Vision

ElasticRelay ist ein nahtloser, heterogener Daten-Synchronisierer, der entwickelt wurde, um Echtzeit-Change Data Capture (CDC) von wichtigen OLTP-Datenbanken (MySQL, PostgreSQL, MongoDB) zu Elasticsearch bereitzustellen. Es zielt darauf ab, benutzerfreundlicher und zuverlässiger als bestehende Lösungen wie Logstash oder Flink zu sein.

## 🎉 v1.4.4 Highlights - Produktionsreife CDC-Plattform mit Transform Engine

**Drei Hauptdatenbankquellen und Enterprise-Datentransformation:**

| Quelle | Status | Funktionen |
|--------|--------|----------|
| **MySQL** | ✅ Vollständig | Binlog CDC + Initial Sync + Parallele Snapshots |
| **PostgreSQL** | ✅ Produktionsgehärtet | Logische Replikation + WAL-Parsing + Stabiler Snapshot-zu-CDC-Übergang |
| **MongoDB** | ✅ Vollständig | Change Streams + Sharded Clusters + Resume Tokens |
| **Transform Engine** | ✅ Vollständig | Feld-Mapping + Datenmaskierung + Typkonvertierung + Ausdrucks-Engine |

## Hauptfunktionen

- **Multi-Source CDC**: Vollständige Unterstützung für MySQL, PostgreSQL und MongoDB mit Echtzeit-Änderungserfassung
- **Transform Engine**: Enterprise-Datentransformation mit Feld-Mapping, Datenmaskierung (Telefon, Ausweis, E-Mail, Bankkarte), Typkonvertierung, Ausdrucksauswertung und bedingter Filterung — Verarbeitung mit 800.000+ Ops/Sek
- **Zero-Code Konfiguration**: JSON-basierte Konfiguration mit Assistenten-GUI (in Entwicklung)
- **Multi-Table Dynamische Indexierung**: Erstellt automatisch separate Elasticsearch-Indizes für jede Quelltabelle mit konfigurierbaren Namensmustern (z.B. `elasticrelay-users`, `elasticrelay-orders`)
- **Eingebaute Governance**: Handhabt Datenstrukturierung, Anonymisierung, Typkonvertierung, Normalisierung und Anreicherung
- **Zuverlässigkeit von Anfang an**: Nutzt CDC auf Transaktionslog-Ebene, präzises Checkpointing für Wiederaufnahme und idempotente Schreibvorgänge zur Sicherstellung der Datenintegrität
- **Dead Letter Queue (DLQ)**: Umfassende Fehlerbehandlung mit exponentiellem Backoff-Retry und persistentem Speicher
- **Parallele Verarbeitung**: Erweiterte parallele Snapshot-Verarbeitung mit Chunking-Strategien für große Tabellen
- **Zentralisiertes Logging**: Zur Laufzeit konfigurierbare Log-Level (debug/info/warn/error) mit thread-sicherer globaler Steuerung

## Technologie-Stack

- **Data Plane (Go)**: Die Kern-Datensynchronisierungslogik ist in Go (1.25.2+) gebaut für hohe Nebenläufigkeit, geringen Speicherbedarf und einfache Bereitstellung.
- **Control Plane & GUI (TypeScript/Next.js)**: Eine reichhaltige, interaktive Benutzeroberfläche für Konfiguration und Überwachung (in Entwicklung).
- **APIs (gRPC)**: Interne Kommunikation zwischen Komponenten wird über gRPC für hohe Leistung mit vollständigen Service-Implementierungen abgewickelt.
- **Datenbankunterstützung**: 
  - **MySQL CDC**: Erweitertes Binlog-Parsing mit Echtzeit-Synchronisierung (go-mysql Bibliothek)
  - **PostgreSQL CDC**: Logische Replikation mit WAL-Parsing, Replikationsslots, Publications, und produktionsgehärteter Snapshot-zu-CDC-Übergang
  - **MongoDB CDC**: Change Streams mit Replica Set und Sharded Cluster Unterstützung (mongo-driver)
- **Transform Engine**: Vollständige Datentransformations-Pipeline mit Feld-Mapping, Typkonvertierung, Datenmaskierung (4 Strategien, 5 Vorlagen), Ausdrucks-Engine (16 eingebaute Funktionen) und bedingter Filterung (10 Operatoren)
- **Elasticsearch Integration**: Offizieller Elasticsearch Go-Client (v8) mit Bulk-Indexierungsunterstützung
- **Konfiguration**: JSON-basierte Konfiguration mit automatischer Formaterkennung und Migration
- **Zuverlässigkeit**: Umfassende Fehlerbehandlung, DLQ-System und Checkpoint-Verwaltung
- **Logging**: Zentralisiertes Log-Level-Kontrollsystem mit Laufzeitkonfiguration

## Architektur

Das System besteht aus mehreren Schlüsselkomponenten:

- **Source Connectors**: Erfassen Änderungen aus MySQL (Binlog), PostgreSQL (logische Replikation) und MongoDB (Change Streams).
- **Durable Buffer**: Asynchrone CDC-Ereigniswarteschlange zur Entkopplung von Quellen-Lesevorgängen und nachgelagerter Verarbeitung.
- **Transform Engine**: Enterprise-Datentransformations-Pipeline mit Feld-Mapping, Typkonvertierung, Datenmaskierung, Ausdrucksauswertung und bedingter Filterung.
- **ES Sink Writer**: Schreibt Daten effizient in Batches nach Elasticsearch mit automatischer Indexverwaltung.
- **Orchestrator**: Verwaltet den Lebenszyklus von Synchronisierungsaufgaben und unterstützt sowohl Legacy-Einzelquellen- als auch Multi-Source-Konfigurationen.
- **Dead Letter Queue**: Behandelt fehlgeschlagene Events mit exponentiellem Backoff-Retry und persistentem Speicher.
- **Checkpoint Manager**: Persistente Positionsverfolgung (Binlog-Positionen, PostgreSQL LSN, MongoDB Resume Tokens) für fehlertolerante Wiederaufnahme.
- **Control Plane**: Die Benutzeroberfläche und das Konfigurationsmanagement-Backend (in Entwicklung).

## Schnellstart

Um ElasticRelay schnell zum Laufen zu bringen, folgen Sie diesen drei einfachen Schritten:

### Schritt 1: Bauen
```sh
./scripts/build.sh
```

### Schritt 2: Konfigurieren

#### MongoDB Setup (Erforderlich für MongoDB CDC)
MongoDB erfordert den Replica Set Modus für Change Streams. Führen Sie das Setup-Skript aus:
```sh
./scripts/reset-mongodb.sh
```

Oder manuell:
```sh
docker-compose down
rm -rf ./data/mongodb/*
docker-compose up -d mongodb
docker-compose up mongodb-init
```

Überprüfen Sie, ob MongoDB bereit ist:
```sh
./scripts/verify-mongodb.sh
```

📚 **Siehe**: `QUICKSTART.md` für detaillierte MongoDB-Setup-Anweisungen.

#### PostgreSQL Setup
Für PostgreSQL stellen Sie sicher, dass die logische Replikation aktiviert ist:
```sql
-- Logische Replikation in postgresql.conf aktivieren
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Benutzer mit Replikationsrechten erstellen
CREATE USER elasticrelay_user WITH LOGIN PASSWORD 'password' REPLICATION;
GRANT CONNECT ON DATABASE your_database TO elasticrelay_user;
GRANT USAGE ON SCHEMA public TO elasticrelay_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO elasticrelay_user;
```

#### Konfigurationsdateien
Bearbeiten Sie die Konfigurationsdatei `./config/parallel_config.json` und stellen Sie sicher, dass die Datenbank- und Elasticsearch-Verbindungsinformationen korrekt sind.

### Schritt 3: Ausführen
```sh
./start.sh
```

Nach Abschluss dieser Schritte wird ElasticRelay beginnen, Datenbankänderungen zu überwachen und sie mit Elasticsearch zu synchronisieren.

---

## Ausführung

### Voraussetzungen

- Go (1.25.2+)
- Protobuf Compiler (`protoc`)
- Elasticsearch (7.x oder 8.x)
- **MySQL** (5.7+ oder 8.x) mit aktiviertem Binlog
- **PostgreSQL** (10+ empfohlen, 9.4+ Minimum) mit aktivierter logischer Replikation
- **MongoDB** (4.0+) mit Replica Set oder Sharded Cluster Konfiguration

### Installation

1.  **Go-Abhängigkeiten und Tools installieren**:
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    ```

2.  **`protoc` installieren**:
    Auf macOS mit Homebrew:
    ```sh
    brew install protobuf
    ```

3.  **Abhängigkeiten aufräumen**:
    ```sh
    go mod tidy
    ```

### Server bauen und ausführen

#### Schnell-Build (Entwicklung)
```sh
# Einfacher Build ohne Versionsinformationen
go build -o elasticrelay ./cmd/elasticrelay

# Server ausführen
./elasticrelay -config multi_config.json
```

#### Produktions-Build (Empfohlen)
```sh
# Build mit Versionsinformationen über Makefile
make build

# Versionierte Binary ausführen
./bin/elasticrelay -config multi_config.json
```

#### Versionsverwaltung
ElasticRelay verfügt über umfassende Versionsverwaltung mit Build-Zeit-Injektion:

```sh
# Aktuelle Versionsinformationen mit detaillierten Build-Informationen anzeigen
./bin/elasticrelay -version

# Versionsinformationen vom Makefile prüfen
make version

# Entwicklungs-Build (schnell, ohne Versionsinjektion)
make dev

# Produktions-Build (optimiert mit Versionsinformationen)
make release

# Plattformübergreifende Builds für mehrere Architekturen
make build-all

# Build mit benutzerdefinierter Version
VERSION="v1.3.0" make build

# Alle Tools einschließlich Migrations-Utilities bauen
make build-tools
```

Das Versionssystem umfasst:
- **Git Integration**: Automatische Versionserkennung aus Git-Tags
- **Build-Metadaten**: Commit-Hash, Build-Zeit, Go-Version und Plattforminformationen
- **Farbige Ausgabe**: Reichhaltige Konsolenausgabe mit Versionsdetails und ASCII-Art-Logo
- **Plattformübergreifend**: Unterstützung für Linux, macOS (Intel/ARM) und Windows

Der Server wird standardmäßig auf Port `50051` starten und lauschen.

**Alternative**: Sie können auch direkt ohne Bauen ausführen:
```sh
go run ./cmd/elasticrelay -config multi_config.json
```

### Multi-Table Konfiguration

ElasticRelay unterstützt sowohl Legacy-Einzelkonfiguration als auch moderne Multi-Config-Formate mit automatischer Erkennung und Migration.

#### Modernes Multi-Config Format (`multi_config.json`):

```json
{
  "version": "3.0",
  "data_sources": [
    {
      "id": "mysql-main",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "user": "elastic_user",
      "password": "password",
      "database": "elasticrelay",
      "server_id": 100,
      "table_filters": ["users", "orders", "products"]
    },
    {
      "id": "postgresql-main",
      "type": "postgresql",
      "host": "localhost",
      "port": 5432,
      "user": "elastic_user",
      "password": "password",
      "database": "elasticrelay",
      "table_filters": ["users", "orders", "products"],
      "options": {
        "ssl_mode": "disable",
        "slot_name": "elasticrelay_slot",
        "publication_name": "elasticrelay_publication",
        "batch_size": 1000,
        "max_connections": 10,
        "parallel_snapshots": true
      }
    },
    {
      "id": "mongodb-main",
      "type": "mongodb",
      "host": "localhost",
      "port": 27017,
      "user": "elasticrelay_user",
      "password": "password",
      "database": "elasticrelay",
      "table_filters": ["users", "orders", "products"],
      "options": {
        "auth_source": "admin",
        "replica_set": "rs0"
      }
    }
  ],
  "sinks": [
    {
      "id": "es-main",
      "type": "elasticsearch",
      "addresses": ["http://localhost:9200"],
      "options": {
        "index_prefix": "elasticrelay"
      }
    }
  ],
  "jobs": [],
  "global": {
    "log_level": "info",
    "grpc_port": 50051,
    "dlq_config": {
      "enabled": true,
      "storage_path": "dlq",
      "max_retries": 3,
      "retry_delay": "30s"
    }
  }
}
```

#### Legacy-Konfigurationsformat (`config.json`):

```json
{
  "db_host": "localhost",
  "db_port": 3306,
  "db_user": "elastic_user",
  "db_password": "password",
  "db_name": "elasticrelay",
  "server_id": 100,
  "table_filters": ["users", "orders", "products"],
  "es_addresses": ["http://localhost:9200"]
}
```

Das System erkennt automatisch das Konfigurationsformat und unterstützt die Migration zwischen Formaten. Dies erstellt separate Indizes:
- `elasticrelay-users` für die `users`-Tabelle
- `elasticrelay-orders` für die `orders`-Tabelle  
- `elasticrelay-products` für die `products`-Tabelle

### Dead Letter Queue (DLQ) Unterstützung

ElasticRelay enthält ein umfassendes DLQ-System zur Behandlung fehlgeschlagener Events:

- **Automatischer Retry**: Fehlgeschlagene Events werden automatisch mit exponentiellem Backoff wiederholt
- **Persistenter Speicher**: DLQ-Elemente werden mit vollständiger Zustandsverwaltung auf die Festplatte gespeichert
- **Deduplizierung**: Verhindert, dass doppelte Events in die Warteschlange aufgenommen werden
- **Status-Tracking**: Vollständige Lebenszyklus-Verfolgung (ausstehend, wiederholt, erschöpft, gelöst, verworfen)
- **Manuelle Verwaltung**: Unterstützung für manuelle Element-Inspektion und -Verwaltung
- **Automatische Bereinigung**: Gelöste Elemente werden nach konfigurierbarer Dauer automatisch bereinigt

### PostgreSQL Unterstützung

ElasticRelay bietet umfassende PostgreSQL CDC-Funktionen mit erweiterten Features:

#### Kern PostgreSQL Features
- **Logische Replikation**: Nutzt PostgreSQL's native logische Replikation mit `pgoutput` Plugin
- **WAL-Parsing**: Erweitertes Write-Ahead Log Parsing für Echtzeit-Änderungserfassung
- **Replikationsslots**: Automatische Erstellung und Verwaltung von logischen Replikationsslots
- **Publications**: Dynamische Publication-Verwaltung für Tabellenfilterung
- **LSN-Verwaltung**: Präzise Log Sequence Number Verfolgung für Checkpoint/Resume-Funktionalität

#### Erweiterte PostgreSQL Funktionen
- **Connection Pooling**: Intelligente Verbindungspool-Verwaltung mit konfigurierbaren Limits
- **Parallele Snapshots**: Multi-Thread initiale Datensynchronisierung mit Chunking-Strategien
- **Typ-Mapping**: Umfassende PostgreSQL zu Elasticsearch Typ-Konvertierung einschließlich:
  - Alle numerischen Typen (bigint, integer, real, double, numeric)
  - Text- und Zeichentypen (text, varchar, char)
  - Datum/Zeit-Typen mit Zeitzonenunterstützung (timestamp, timestamptz, date, time)
  - JSON/JSONB mit nativem Objekt-Mapping
  - Array-Typen (integer arrays, text arrays)
  - Erweiterte Typen (UUID, bytea, inet, geometrische Typen)
- **Leistungsoptimierungen**: 
  - Adaptive Planung für große Tabellen
  - Streaming-Modus für Speichereffizienz
  - Konfigurierbare Batch-Größen und Worker-Pools
  - Verbindungslebenszyklus-Verwaltung

#### PostgreSQL Konfigurationsoptionen
```json
{
  "type": "postgresql",
  "options": {
    "ssl_mode": "disable|require|verify-ca|verify-full",
    "slot_name": "custom_replication_slot_name",
    "publication_name": "custom_publication_name",
    "batch_size": 1000,
    "max_connections": 10,
    "min_connections": 2,
    "parallel_snapshots": true,
    "enable_performance_monitoring": true
  }
}
```

### MongoDB Unterstützung

ElasticRelay bietet vollständige MongoDB CDC-Funktionen mit Change Streams:

#### Kern MongoDB Features
- **Change Streams**: Echtzeit-CDC mit MongoDB's nativer Change Streams API
- **Cluster-Unterstützung**: Automatische Erkennung und Unterstützung für Replica Sets und Sharded Clusters
- **Resume Tokens**: Persistentes Resume Token Management für Checkpoint/Resume-Funktionalität
- **Operations-Mapping**: Vollständige Unterstützung für INSERT, UPDATE, REPLACE und DELETE Operationen

#### Erweiterte MongoDB Funktionen
- **Sharded Cluster Unterstützung**: 
  - Multi-Shard Überwachung via mongos
  - Migrations-Bewusstsein für Konsistenz während Chunk-Migrationen
  - Chunk-Verteilungsüberwachung
- **Typ-Konvertierung**: Vollständige BSON zu JSON-freundliche Typ-Konvertierung:
  - ObjectID → string (Hex-Format)
  - DateTime → RFC3339 Zeitstempel
  - Decimal128 → string (Präzision erhalten)
  - Binary → base64 kodiert
  - Verschachtelte Dokumente mit konfigurierbarer Abflachungstiefe
- **Parallele Snapshots**: 
  - ObjectID-basiertes Chunking für Standard-Collections
  - Numerisches ID-basiertes Chunking für Integer-Primärschlüssel
  - Skip/Limit Fallback für komplexe ID-Typen

#### MongoDB Konfigurationsoptionen
```json
{
  "type": "mongodb",
  "host": "localhost",
  "port": 27017,
  "user": "elasticrelay_user",
  "password": "password",
  "database": "your_database",
  "options": {
    "auth_source": "admin",
    "replica_set": "rs0",
    "read_preference": "primaryPreferred",
    "batch_size": 1000,
    "flatten_depth": 3
  }
}
```

#### MongoDB Setup-Anforderungen
```sh
# MongoDB muss im Replica Set Modus für Change Streams laufen
# Verwenden Sie das bereitgestellte Setup-Skript:
./scripts/reset-mongodb.sh

# Oder mit Docker Compose:
docker-compose up -d mongodb
docker-compose up mongodb-init

# Überprüfen Sie, ob das Replica Set konfiguriert ist:
./scripts/verify-mongodb.sh
```

### Transform Engine

ElasticRelay enthält eine vollständige Datentransformations-Pipeline, konfigurierbar über eine separate JSON-Datei (`-transform-config`):

#### Feld-Mapping
- **Umbenennen**: Feldnamen ändern (z. B. `user_name` → `username`)
- **Kopieren**: Felder unter neuen Namen duplizieren und Originale beibehalten
- **Verschachtelte Pfade**: Zugriff und Änderung verschachtelter Felder per Punktnotation (`user.profile.name`)
- **Feldausschluss**: Sensible oder unnötige Felder vor der Indexierung entfernen

#### Typkonvertierung

| Quelltyp | Zieltypen |
|----------|-----------|
| string | int, int64, float64, bool, date, timestamp |
| int/int64 | string, float64, bool, timestamp |
| float64 | string, int, int64, bool |
| bool | string, int |
| time.Time | string (RFC3339), timestamp (Unix) |

#### Datenmaskierung

| Vorlage | Eingabe | Ausgabe |
|---------|---------|---------|
| `phone` | `13812345678` | `138****5678` |
| `id_card` | `110101199001011234` | `1101**********1234` |
| `email` | `john@example.com` | `jo***@example.com` |
| `bank_card` | `6222021234567890` | `6222********7890` |
| `name` | `张三` | `张*` |

Maskierungsstrategien: `mask` (Zeichenmaskierung), `hash` (SHA256/MD5), `token` (Tokenisierung), `regex` (Musteraustausch).

#### Ausdrucks-Engine

Eingebaute Funktionen für berechnete Felder:

| Kategorie | Funktionen |
|-----------|------------|
| String | `concat()`, `substr()`, `upper()`, `lower()`, `trim()`, `replace()`, `length()` |
| Mathematik | `round()`, `abs()`, `floor()`, `ceil()`, `min()`, `max()` |
| Datum | `now()`, `formatDate()`, `parseDate()` |
| Bedingt | `ifNull()`, `ifEmpty()`, `coalesce()` |

Beispielausdrücke:
```javascript
$.age < 18 ? 'minor' : 'adult'
concat($.first_name, ' ', $.last_name)
round($.price * $.quantity, 2)
```

#### Bedingte Filterung

| Operator | Beschreibung | Beispiel |
|----------|--------------|----------|
| `eq` | Gleich | `status == "active"` |
| `ne` | Ungleich | `status != "deleted"` |
| `gt` / `gte` | Größer (oder gleich) | `age > 18` |
| `lt` / `lte` | Kleiner (oder gleich) | `price < 100` |
| `in` / `nin` | In / nicht in Liste | `type in ["a", "b"]` |
| `regex` | Regex-Übereinstimmung | `email ~ ".*@example.com"` |
| `exists` | Feld existiert | `email exists` |

#### Transform-Konfiguration

```sh
# Mit Transform-Regeln ausführen
./bin/elasticrelay -config multi_config.json -transform-config ./config/mysql_transform.json

# Ohne Transform (Pass-Through-Modus, Standard)
./bin/elasticrelay -config multi_config.json
```

#### Leistung

| Operation | Durchsatz | Speicher |
|-----------|-----------|----------|
| Vollständige Transform-Pipeline | ~800.000 Ops/Sek | 1.601 B/Op |
| Feld-Mapping | ~4.500.000 Ops/Sek | 416 B/Op |
| Typkonvertierung | ~22.000.000 Ops/Sek | 16 B/Op |
| Filterauswertung | ~5.000.000 Ops/Sek | ~200 B/Op |
| Datenmaskierung (4 Felder) | ~1.000.000 Ops/Sek | ~500 B/Op |

### Parallele Verarbeitung

Erweiterte parallele Snapshot-Verarbeitungsfähigkeiten:

- **Chunking-Strategien**: Unterstützung für ID-basiertes, zeitbasiertes und hash-basiertes Chunking
- **Worker-Pools**: Konfigurierbare Worker-Pool-Größen mit adaptiver Planung
- **Fortschrittsverfolgung**: Echtzeit-Fortschrittsüberwachung und Statistiken
- **Große Tabellen Unterstützung**: Optimierte Handhabung großer Tabellen mit intelligentem Chunking
- **Streaming-Modus**: Speichereffiziente Streaming-Verarbeitung für große Datensätze
- **Primärschlüssel-Erkennung**: Automatische Erkennung von Primärschlüssel-Spalten für korrekte Dokument-IDs

## Aktueller Status

**Aktuelle Version**: v1.4.4 | **Phase**: Phase 2 Abgeschlossen ✅, Phase 3 in Arbeit (Transform Engine abgeschlossen)

Dieses Projekt hat seine Kern-Multi-Source-CDC-Plattform (Phase 2) abgeschlossen und die Transform Engine als ersten großen Meilenstein von Phase 3 geliefert. PostgreSQL-CDC wurde mit umfangreichen Stabilitätsfixes produktionsgehärtet.

### ✅ Abgeschlossene Features (v1.4.4)
- **Multi-Source CDC Pipeline**: 
  - **MySQL CDC**: Vollständige Implementierung mit binlog-basierter Echtzeit-Synchronisierung und konsistenter Datums-/Zeitbehandlung
  - **PostgreSQL CDC**: Produktionsgehärtete logische Replikation mit WAL-Parsing, Replikationsslots, Publications, stabilem Snapshot-zu-CDC-Übergang, asynchroner Batch-Entkopplung und jobbezogenem Replikationsslot-Management
  - **MongoDB CDC**: Vollständige Change-Streams-Implementierung mit Replica-Set- und Sharded-Cluster-Unterstützung
- **Transform Engine** (v1.4.0+):
  - Feld-Mapping (Umbenennen, Kopieren, Verschieben) mit Unterstützung verschachtelter Pfade
  - Typkonvertierung (string, int, float, bool, date, timestamp, object)
  - Datenmaskierung (Telefon, Ausweis, E-Mail, Bankkarte, Name) mit 4 Strategien
  - Ausdrucks-Engine mit 16 eingebauten Funktionen
  - Bedingte Filterung mit 10 Operatoren und Include-/Exclude-/Route-Aktionen
  - Prioritätsbasierte Mehrfachregel-Zuordnung mit Tabellenmuster-Wildcards
  - Leistung: 800.000+ Ops/Sek (80× über dem Designziel)
- **Multi-Table Dynamische Indexierung**: Automatische Elasticsearch-Index-Erstellung und -Verwaltung pro Tabelle mit konfigurierbarer Benennung
- **gRPC Architektur**: Vollständige Service-Definitionen und Implementierungen (Connector, Orchestrator, Sink, Transform, Health)
- **Erweitertes Konfigurationsmanagement**: 
  - Multi-Source Konfigurationssystem mit Legacy-Migrationsunterstützung
  - Konfigurationssynchronisierung und Hot-Reload-Fähigkeiten
  - Automatische Formaterkennung und Migrationstools
- **Elasticsearch Integration**: Hochleistungs-Bulk-Schreiben mit automatischem Index-Management und Datenbereinigung
- **Checkpoint/Resume**: Persistente Positionsverfolgung für Fehlertoleranz mit automatischer Wiederherstellung (binlog, LSN, resume tokens)
- **Dead Letter Queue (DLQ)**: 
  - Umfassendes DLQ-System mit exponentiellem Backoff-Retry (konfigurierbare max. Wiederholungen)
  - Persistenter Speicher mit Deduplizierung und Status-Tracking
  - Automatische Bereinigung gelöster Elemente
  - Unterstützung für manuelle Element-Verwaltung und -Inspektion
- **Parallele Verarbeitung**: 
  - Erweiterte parallele Snapshot-Verarbeitung mit Chunking-Strategien
  - Automatische Primärschlüssel-Erkennung für korrekte Dokument-ID-Generierung
  - Konfigurierbare Worker-Pools und adaptive Planung
  - Fortschrittsverfolgung und Statistiksammlung
  - Unterstützung für große Tabellenoptimierung (MySQL, PostgreSQL, MongoDB)
- **Versionsverwaltung**: Vollständiges Versionsinjektionssystem mit Build-Zeit-Metadaten
- **Robuste Fehlerbehandlung**: Umfassende Fehlerbehandlung mit Fallback-Mechanismen
- **Log-Level-Steuerung**: Zentralisiertes Logging-System (debug/info/warn/error) mit Laufzeitkonfiguration und thread-sicherer globaler Steuerung

### 🚧 In Arbeit (Phase 3 Verbleibend)
- **Prometheus Metrics**: Vollständige Observability mit Metrik-Export
- **HTTP REST API**: grpc-gateway Integration mit OpenAPI-Dokumentation
- **Health Check Erweiterung**: Kubernetes-ready Readiness/Liveness Probes

### 📋 Geplant (Phase 4+)
- **Frontend-Entwicklung**: Control Plane GUI (TypeScript/Next.js)
- **Hochverfügbarkeit**: Multi-Replica Deployment mit automatischem Failover
- **Sicherheitserweiterung**: mTLS, RBAC und Audit-Logging
- **Erweiterte Governance**: Umfangreiche Datentransformationsregeln und feldbasierte Governance

---

## 📄 Lizenz

ElasticRelay ist unter der [Apache License 2.0](LICENSE) lizenziert.

```
Copyright 2024 上海悦高软件股份有限公司 (Yogoo Software Co., Ltd.)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## 🤝 Mitwirken

Wir freuen uns über Beiträge! Bitte sehen Sie unsere [Beitragsrichtlinien](CONTRIBUTING.md) für Details.

## 📞 Support

- 🐦 X (Twitter): [@ElasticRelay](https://x.com/ElasticRelay)
- 🌐 Offizielle Website: [www.elasticrelay.com](http://www.elasticrelay.com)
- 📧 E-Mail: support@yogoo.net
- 💬 Community: [GitHub Discussions](https://github.com/yogoosoft/ElasticRelay/discussions)
- 🐛 Fehlerberichte: [GitHub Issues](https://github.com/yogoosoft/ElasticRelay/issues)
- 📖 Dokumentation: [docs.elasticrelay.com](https://docs.elasticrelay.com)
