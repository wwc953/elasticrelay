# ElasticRelay - Passerelle CDC Multi-Sources vers Elasticsearch

![ElasticRelay Screenshot](/releases/download/asset/screenshot_02.png)

<p align="center">
  <a href="https://github.com/yogoosoft/ElasticRelay/releases"><img src="https://img.shields.io/badge/version-v1.4.4-blue.svg" alt="Version"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-1.25.2+-00ADD8.svg" alt="Version Go"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="Licence"></a>
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

ElasticRelay est un synchroniseur de données hétérogènes transparent, conçu pour fournir une Capture de Changement de Données (CDC) en temps réel depuis les principales bases de données OLTP (MySQL, PostgreSQL, MongoDB) vers Elasticsearch. Il vise à être plus convivial et plus fiable que les solutions existantes comme Logstash ou Flink.

## 🎉 Points forts de la v1.4.4 - Plateforme CDC prête pour la production avec moteur de transformation

**Trois sources majeures + transformation de données de niveau entreprise :**

| Source | Statut | Fonctionnalités |
|--------|--------|----------|
| **MySQL** | ✅ Complet | CDC Binlog + Sync Initial + Snapshots Parallèles |
| **PostgreSQL** | ✅ Durci pour la production | Réplication Logique + Parsing WAL + Transition Snapshot-CDC stable |
| **MongoDB** | ✅ Complet | Change Streams + Clusters Shardés + Resume Tokens |
| **Moteur de Transformation** | ✅ Complet | Mapping de Champs + Masquage de Données + Conversion de Types + Moteur d'Expressions |

## Fonctionnalités Principales

- **CDC Multi-Sources** : Support complet pour MySQL, PostgreSQL et MongoDB avec capture de changements en temps réel
- **Moteur de Transformation** : Transformation de données de niveau entreprise avec mapping de champs, masquage de données (téléphone, carte d'identité, email, carte bancaire), conversion de types, évaluation d'expressions et filtrage conditionnel — traitement à 800 000+ ops/sec
- **Configuration Sans Code** : Configuration basée sur JSON avec GUI de type assistant (en développement)
- **Indexation Dynamique Multi-Tables** : Crée automatiquement des index Elasticsearch séparés pour chaque table source avec des modèles de nommage configurables (ex: `elasticrelay-users`, `elasticrelay-orders`)
- **Gouvernance Intégrée** : Gère la structuration des données, l'anonymisation, la conversion de types, la normalisation et l'enrichissement
- **Fiabilité par Défaut** : Utilise le CDC au niveau du journal de transactions, des points de contrôle précis pour la reprise, et des écritures idempotentes pour garantir l'intégrité des données
- **Dead Letter Queue (DLQ)** : Gestion complète des échecs avec retry à backoff exponentiel et stockage persistant
- **Traitement Parallèle** : Traitement avancé de snapshots parallèles avec stratégies de chunking pour les grandes tables
- **Logging Centralisé** : Niveaux de log configurables à l'exécution (debug/info/warn/error) avec contrôle global thread-safe

## Stack Technologique

- **Plan de Données (Go)** : La logique de synchronisation de données principale est construite en Go (1.25.2+) pour une haute concurrence, une faible empreinte mémoire et un déploiement simple.
- **Plan de Contrôle & GUI (TypeScript/Next.js)** : Une interface utilisateur riche et interactive pour la configuration et le monitoring (en développement).
- **APIs (gRPC)** : La communication interne entre les composants est gérée via gRPC pour une haute performance avec des implémentations de services complètes.
- **Support de Bases de Données** : 
  - **MySQL CDC** : Parsing binlog avancé avec synchronisation en temps réel (bibliothèque go-mysql)
  - **PostgreSQL CDC** : Réplication logique avec parsing WAL, slots de réplication et publications, et transition snapshot-CDC durcie pour la production
  - **MongoDB CDC** : Change Streams avec support replica set et cluster shardé (mongo-driver)
- **Moteur de Transformation** : Pipeline de transformation de données complet avec mapping de champs, conversion de types, masquage de données (4 stratégies, 5 modèles prédéfinis), moteur d'expressions (16 fonctions intégrées) et filtrage conditionnel (10 opérateurs)
- **Intégration Elasticsearch** : Client Go Elasticsearch officiel (v8) avec support d'indexation en masse
- **Configuration** : Configuration basée sur JSON avec détection automatique du format et migration
- **Fiabilité** : Gestion complète des erreurs, système DLQ et gestion des points de contrôle
- **Logging** : Système de contrôle centralisé du niveau de log avec configuration à l'exécution

## Architecture

Le système est composé de plusieurs composants clés :

- **Connecteurs Sources** : Capturent les changements depuis MySQL (binlog), PostgreSQL (réplication logique) et MongoDB (change streams).
- **Tampon Durable** : File d'attente d'événements CDC asynchrone découplant la lecture source du traitement aval.
- **Moteur de Transformation** : Pipeline de transformation de données de niveau entreprise avec mapping de champs, conversion de types, masquage de données, évaluation d'expressions et filtrage conditionnel.
- **ES Sink Writer** : Écrit les données vers Elasticsearch en lots efficaces avec gestion automatique des index.
- **Orchestrateur** : Gère le cycle de vie des tâches de synchronisation, avec prise en charge des configurations legacy mono-source et multi-sources.
- **Dead Letter Queue** : Gère les événements échoués avec nouvelle tentative à backoff exponentiel et stockage persistant.
- **Gestionnaire de Points de Contrôle** : Suivi persistant des positions (positions binlog, LSN PostgreSQL, jetons de reprise MongoDB) pour une reprise tolérante aux pannes.
- **Plan de Contrôle** : Interface utilisateur et backend de gestion de configuration (en développement).

## Démarrage Rapide

Pour démarrer rapidement ElasticRelay, suivez ces trois étapes simples :

### Étape 1 : Construire
```sh
./scripts/build.sh
```

### Étape 2 : Configurer

#### Configuration MongoDB (Requis pour MongoDB CDC)
MongoDB nécessite le mode replica set pour les Change Streams. Exécutez le script de configuration :
```sh
./scripts/reset-mongodb.sh
```

Ou manuellement :
```sh
docker-compose down
rm -rf ./data/mongodb/*
docker-compose up -d mongodb
docker-compose up mongodb-init
```

Vérifiez que MongoDB est prêt :
```sh
./scripts/verify-mongodb.sh
```

📚 **Voir** : `QUICKSTART.md` pour des instructions détaillées de configuration MongoDB.

#### Configuration PostgreSQL
Pour PostgreSQL, assurez-vous que la réplication logique est activée :
```sql
-- Activer la réplication logique dans postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Créer un utilisateur avec des privilèges de réplication
CREATE USER elasticrelay_user WITH LOGIN PASSWORD 'password' REPLICATION;
GRANT CONNECT ON DATABASE your_database TO elasticrelay_user;
GRANT USAGE ON SCHEMA public TO elasticrelay_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO elasticrelay_user;
```

#### Fichiers de Configuration
Modifiez le fichier de configuration `./config/parallel_config.json` et assurez-vous que les informations de connexion à la base de données et à Elasticsearch sont correctes.

### Étape 3 : Exécuter
```sh
./start.sh
```

Après avoir complété ces étapes, ElasticRelay commencera à surveiller les changements de la base de données et à les synchroniser vers Elasticsearch.

---

## Comment Exécuter

### Prérequis

- Go (1.25.2+)
- Compilateur Protobuf (`protoc`)
- Elasticsearch (7.x ou 8.x)
- **MySQL** (5.7+ ou 8.x) avec binlog activé
- **PostgreSQL** (10+ recommandé, 9.4+ minimum) avec réplication logique activée
- **MongoDB** (4.0+) avec configuration replica set ou cluster shardé

### Installation

1.  **Installer les dépendances et outils Go** :
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    ```

2.  **Installer `protoc`** :
    Sur macOS avec Homebrew :
    ```sh
    brew install protobuf
    ```

3.  **Ranger les dépendances** :
    ```sh
    go mod tidy
    ```

### Construire et Exécuter le Serveur

#### Build Rapide (Développement)
```sh
# Build simple sans informations de version
go build -o elasticrelay ./cmd/elasticrelay

# Exécuter le serveur
./elasticrelay -config multi_config.json
```

#### Build Production (Recommandé)
```sh
# Build avec informations de version via Makefile
make build

# Exécuter le binaire versionné
./bin/elasticrelay -config multi_config.json
```

#### Gestion des Versions
ElasticRelay dispose d'une gestion complète des versions avec injection au moment du build :

```sh
# Afficher les informations de version actuelles avec détails du build
./bin/elasticrelay -version

# Vérifier les informations de version depuis le Makefile
make version

# Build développement (rapide, sans injection de version)
make dev

# Build production (optimisé avec informations de version)
make release

# Builds multi-plateformes pour plusieurs architectures
make build-all

# Build avec version personnalisée
VERSION="v1.3.0" make build

# Construire tous les outils incluant les utilitaires de migration
make build-tools
```

Le système de version inclut :
- **Intégration Git** : Détection automatique de version depuis les tags git
- **Métadonnées de Build** : Hash de commit, heure de build, version Go et informations de plateforme
- **Sortie Colorisée** : Sortie console riche avec détails de version et logo ASCII art
- **Multi-Plateforme** : Support pour Linux, macOS (Intel/ARM) et Windows

Le serveur démarrera et écoutera sur le port `50051` par défaut.

**Alternative** : Vous pouvez aussi exécuter directement sans construire :
```sh
go run ./cmd/elasticrelay -config multi_config.json
```

### Configuration Multi-Tables

ElasticRelay supporte à la fois les formats de configuration legacy simple et les formats multi-config modernes avec détection automatique et migration.

#### Format Multi-Config Moderne (`multi_config.json`) :

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

#### Format de Configuration Legacy (`config.json`) :

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

Le système détecte automatiquement le format de configuration et supporte la migration entre formats. Cela crée des index séparés :
- `elasticrelay-users` pour la table `users`
- `elasticrelay-orders` pour la table `orders`  
- `elasticrelay-products` pour la table `products`

### Support Dead Letter Queue (DLQ)

ElasticRelay inclut un système DLQ complet pour gérer les événements échoués :

- **Retry Automatique** : Les événements échoués sont automatiquement réessayés avec backoff exponentiel
- **Stockage Persistant** : Les éléments DLQ sont persistés sur disque avec gestion complète de l'état
- **Déduplication** : Empêche les événements dupliqués d'être ajoutés à la file d'attente
- **Suivi de Statut** : Suivi complet du cycle de vie (en attente, en cours de retry, épuisé, résolu, rejeté)
- **Gestion Manuelle** : Support pour l'inspection et la gestion manuelle des éléments
- **Nettoyage Automatique** : Les éléments résolus sont automatiquement nettoyés après une durée configurable

### Support PostgreSQL

ElasticRelay fournit des capacités CDC PostgreSQL complètes avec des fonctionnalités avancées :

#### Fonctionnalités PostgreSQL de Base
- **Réplication Logique** : Utilise la réplication logique native de PostgreSQL avec le plugin `pgoutput`
- **Parsing WAL** : Parsing avancé du Write-Ahead Log pour la capture de changements en temps réel
- **Slots de Réplication** : Création et gestion automatiques des slots de réplication logique
- **Publications** : Gestion dynamique des publications pour le filtrage de tables
- **Gestion LSN** : Suivi précis du Log Sequence Number pour la fonctionnalité checkpoint/reprise

#### Capacités PostgreSQL Avancées
- **Pool de Connexions** : Gestion intelligente du pool de connexions avec limites configurables
- **Snapshots Parallèles** : Synchronisation initiale multi-thread avec stratégies de chunking
- **Mapping de Types** : Conversion complète des types PostgreSQL vers Elasticsearch incluant :
  - Tous les types numériques (bigint, integer, real, double, numeric)
  - Types texte et caractère (text, varchar, char)
  - Types date/heure avec support fuseau horaire (timestamp, timestamptz, date, time)
  - JSON/JSONB avec mapping d'objets natif
  - Types tableau (integer arrays, text arrays)
  - Types avancés (UUID, bytea, inet, types géométriques)
- **Optimisations de Performance** : 
  - Planification adaptative pour les grandes tables
  - Mode streaming pour l'efficacité mémoire
  - Tailles de lot et pools de workers configurables
  - Gestion du cycle de vie des connexions

#### Options de Configuration PostgreSQL
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

### Support MongoDB

ElasticRelay fournit des capacités CDC MongoDB complètes utilisant les Change Streams :

#### Fonctionnalités MongoDB de Base
- **Change Streams** : CDC en temps réel utilisant l'API Change Streams native de MongoDB
- **Support Cluster** : Détection et support automatiques des replica sets et clusters shardés
- **Resume Tokens** : Gestion persistante des resume tokens pour la fonctionnalité checkpoint/reprise
- **Mapping d'Opérations** : Support complet pour les opérations INSERT, UPDATE, REPLACE et DELETE

#### Capacités MongoDB Avancées
- **Support Cluster Shardé** : 
  - Surveillance multi-shard via mongos
  - Conscience des migrations pour la cohérence pendant les migrations de chunks
  - Surveillance de la distribution des chunks
- **Conversion de Types** : Conversion complète BSON vers types compatibles JSON :
  - ObjectID → string (format hex)
  - DateTime → timestamp RFC3339
  - Decimal128 → string (précision préservée)
  - Binary → encodé base64
  - Documents imbriqués avec profondeur d'aplatissement configurable
- **Snapshots Parallèles** : 
  - Chunking basé sur ObjectID pour les collections standard
  - Chunking basé sur ID numérique pour les clés primaires entières
  - Fallback Skip/Limit pour les types d'ID complexes

#### Options de Configuration MongoDB
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

#### Prérequis de Configuration MongoDB
```sh
# MongoDB doit fonctionner en mode replica set pour les Change Streams
# Utilisez le script de configuration fourni :
./scripts/reset-mongodb.sh

# Ou avec Docker Compose :
docker-compose up -d mongodb
docker-compose up mongodb-init

# Vérifiez que le replica set est configuré :
./scripts/verify-mongodb.sh
```

### Moteur de Transformation

ElasticRelay inclut un pipeline de transformation de données complet, configurable via un fichier JSON séparé (`-transform-config`) :

#### Mapping de Champs
- **Renommer** : Changer les noms de champs (ex. `user_name` → `username`)
- **Copier** : Dupliquer les champs sous de nouveaux noms tout en conservant les originaux
- **Support de chemins imbriqués** : Accéder et modifier des champs imbriqués avec la notation point (`user.profile.name`)
- **Exclusion de champs** : Supprimer des champs sensibles ou inutiles avant indexation

#### Conversion de Types

| Type source | Types cibles |
|-------------|--------------|
| string | int, int64, float64, bool, date, timestamp |
| int/int64 | string, float64, bool, timestamp |
| float64 | string, int, int64, bool |
| bool | string, int |
| time.Time | string (RFC3339), timestamp (Unix) |

#### Masquage de Données

| Modèle | Entrée | Sortie |
|--------|--------|--------|
| `phone` | `13812345678` | `138****5678` |
| `id_card` | `110101199001011234` | `1101**********1234` |
| `email` | `john@example.com` | `jo***@example.com` |
| `bank_card` | `6222021234567890` | `6222********7890` |
| `name` | `张三` | `张*` |

Stratégies de masquage : `mask` (masquage de caractères), `hash` (SHA256/MD5), `token` (tokenisation), `regex` (remplacement par motif).

#### Moteur d'Expressions

Fonctions intégrées pour les champs calculés :

| Catégorie | Fonctions |
|-----------|-----------|
| Chaîne | `concat()`, `substr()`, `upper()`, `lower()`, `trim()`, `replace()`, `length()` |
| Math | `round()`, `abs()`, `floor()`, `ceil()`, `min()`, `max()` |
| Date | `now()`, `formatDate()`, `parseDate()` |
| Conditionnel | `ifNull()`, `ifEmpty()`, `coalesce()` |

Exemples d'expressions :
```javascript
$.age < 18 ? 'minor' : 'adult'
concat($.first_name, ' ', $.last_name)
round($.price * $.quantity, 2)
```

#### Filtrage Conditionnel

| Opérateur | Description | Exemple |
|-----------|-------------|---------|
| `eq` | Égal | `status == "active"` |
| `ne` | Différent | `status != "deleted"` |
| `gt` / `gte` | Supérieur (ou égal) | `age > 18` |
| `lt` / `lte` | Inférieur (ou égal) | `price < 100` |
| `in` / `nin` | Dans / pas dans la liste | `type in ["a", "b"]` |
| `regex` | Correspondance regex | `email ~ ".*@example.com"` |
| `exists` | Champ existant | `email exists` |

#### Configuration de Transformation

```sh
# Exécuter avec règles de transformation
./bin/elasticrelay -config multi_config.json -transform-config ./config/mysql_transform.json

# Exécuter sans transformation (mode pass-through, par défaut)
./bin/elasticrelay -config multi_config.json
```

#### Performance

| Opération | Débit | Mémoire |
|-----------|-------|---------|
| Pipeline de transformation complète | ~800 000 ops/sec | 1 601 o/op |
| Mapping de champs | ~4 500 000 ops/sec | 416 o/op |
| Conversion de types | ~22 000 000 ops/sec | 16 o/op |
| Évaluation de filtre | ~5 000 000 ops/sec | ~200 o/op |
| Masquage de données (4 champs) | ~1 000 000 ops/sec | ~500 o/op |

### Traitement Parallèle

Capacités avancées de traitement de snapshots parallèles :

- **Stratégies de Chunking** : Support pour le chunking basé sur ID, temps et hash
- **Pools de Workers** : Tailles de pool de workers configurables avec planification adaptative
- **Suivi de Progression** : Surveillance de progression en temps réel et statistiques
- **Support Grandes Tables** : Gestion optimisée des grandes tables avec chunking intelligent
- **Mode Streaming** : Traitement streaming économe en mémoire pour les grands ensembles de données
- **Découverte de Clé Primaire** : Détection automatique des colonnes de clé primaire pour des IDs de documents corrects

## Statut Actuel

**Version Actuelle** : v1.4.4 | **Phase** : Phase 2 Terminée ✅, Phase 3 En Cours (Moteur de Transformation terminé)

Ce projet a achevé sa plateforme CDC multi-sources principale (Phase 2) et a livré le Moteur de Transformation comme première étape majeure de la Phase 3. Le CDC PostgreSQL a été durci pour la production grâce à de nombreuses corrections de stabilité.

### ✅ Fonctionnalités Terminées (v1.4.4)
- **Pipeline CDC Multi-Sources** : 
  - **MySQL CDC** : Implémentation complète avec synchronisation en temps réel basée sur binlog et gestion cohérente des datetime
  - **PostgreSQL CDC** : Réplication logique durcie pour la production avec parsing WAL, slots de réplication, publications, transition snapshot-CDC stable, découplage asynchrone par lots, et gestion des slots de réplication par scope de job
  - **MongoDB CDC** : Implémentation complète des Change Streams avec support replica set et cluster shardé
- **Moteur de Transformation** (v1.4.0+) :
  - Mapping de champs (renommer, copier, déplacer) avec support de chemins imbriqués
  - Conversion de types (chaîne, int, float, bool, date, timestamp, objet)
  - Masquage de données (téléphone, carte d'identité, email, carte bancaire, nom) avec 4 stratégies
  - Moteur d'expressions avec 16 fonctions intégrées
  - Filtrage conditionnel avec 10 opérateurs et actions include/exclude/route
  - Correspondance multi-règles par priorité avec jokers de motifs de table
  - Performance : 800 000+ ops/sec (80× au-dessus de la cible de conception)
- **Indexation Dynamique Multi-Tables** : Création et gestion automatiques d'index Elasticsearch par table avec nommage configurable
- **Architecture gRPC** : Définitions et implémentations de services complètes (Connector, Orchestrator, Sink, Transform, Health)
- **Gestion de Configuration Avancée** : 
  - Système de configuration multi-sources avec support de migration legacy
  - Synchronisation de configuration et capacités de hot-reload
  - Détection automatique de format et outils de migration
- **Intégration Elasticsearch** : Écriture en masse haute performance avec gestion automatique d'index et nettoyage de données
- **Checkpoint/Reprise** : Suivi de position persistant pour la tolérance aux pannes avec récupération automatique (binlog, LSN, resume tokens)
- **Dead Letter Queue (DLQ)** : 
  - Système DLQ complet avec retry à backoff exponentiel (max retries configurable)
  - Stockage persistant avec déduplication et suivi de statut
  - Nettoyage automatique des éléments résolus
  - Support pour la gestion et l'inspection manuelles des éléments
- **Traitement Parallèle** : 
  - Traitement de snapshots parallèles avancé avec stratégies de chunking
  - Découverte automatique de clé primaire pour la génération correcte des IDs de documents
  - Pools de workers configurables et planification adaptative
  - Suivi de progression et collecte de statistiques
  - Support pour l'optimisation des grandes tables (MySQL, PostgreSQL, MongoDB)
- **Gestion des Versions** : Système complet d'injection de version avec métadonnées de build
- **Gestion d'Erreurs Robuste** : Gestion complète des erreurs avec mécanismes de fallback
- **Contrôle du Niveau de Log** : Système de logging centralisé (debug/info/warn/error) avec configuration à l'exécution et contrôle global thread-safe

### 🚧 En Cours (Phase 3 Restante)
- **Métriques Prometheus** : Observabilité complète avec export de métriques
- **API REST HTTP** : Intégration grpc-gateway avec documentation OpenAPI
- **Amélioration Health Check** : Probes readiness/liveness prêts pour Kubernetes

### 📋 À Venir (Phase 4+)
- **Développement Frontend** : GUI Plan de Contrôle (TypeScript/Next.js)
- **Haute Disponibilité** : Déploiement multi-réplica avec failover automatique
- **Amélioration Sécurité** : mTLS, RBAC et audit logging
- **Gouvernance Avancée** : Règles de transformation de données riches et gouvernance au niveau des champs

---

## 📄 Licence

ElasticRelay est sous licence [Apache License 2.0](LICENSE).

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

## 🤝 Contribuer

Nous accueillons les contributions ! Veuillez consulter nos [Directives de Contribution](CONTRIBUTING.md) pour plus de détails.

## 📞 Support

- 🐦 X (Twitter) : [@ElasticRelay](https://x.com/ElasticRelay)
- 🌐 Site Web Officiel : [www.elasticrelay.com](http://www.elasticrelay.com)
- 📧 Email : support@yogoo.net
- 💬 Communauté : [GitHub Discussions](https://github.com/yogoosoft/ElasticRelay/discussions)
- 🐛 Rapports de Bugs : [GitHub Issues](https://github.com/yogoosoft/ElasticRelay/issues)
- 📖 Documentation : [docs.elasticrelay.com](https://docs.elasticrelay.com)
