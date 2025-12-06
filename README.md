
# Rapport de Projet Big Data  
Architecture Spark Streaming avec Docker et HDFS

**Auteur** : Zakaria EL GUAZZAR  
**Date** : 2024-12-07

---

## Table des matières

1. [Introduction](#introduction)
   - [Contexte du Projet](#contexte-du-projet)
   - [Objectifs](#objectifs)
2. [Architecture Technique](#architecture-technique)
   - [Composants de l'Architecture](#composants-de-larchitecture)
   - [Configuration Docker Compose](#configuration-docker-compose)
   - [Configuration Hadoop](#configuration-hadoop)
3. [Implémentation de l'Application Spark](#implémentation-de-lapplication-spark)
   - [Application de Streaming](#application-de-streaming)
   - [Structure des Données](#structure-des-données)
4. [Déploiement et Exécution](#déploiement-et-exécution)
   - [Étapes de Déploiement](#étapes-de-déploiement)
   - [Script d'Automatisation](#script-dautomatisation)
5. [Résultats et Observations](#résultats-et-observations)
   - [Sortie de l'Application](#sortie-de-lapplication)
   - [Interfaces de Surveillance](#interfaces-de-surveillance)
   - [Performances Observées](#performances-observées)
6. [Problèmes Rencontrés et Solutions](#problèmes-rencontrés-et-solutions)
7. [Améliorations Possibles](#améliorations-possibles)
   - [Améliorations Techniques](#améliorations-techniques)
   - [Améliorations Fonctionnelles](#améliorations-fonctionnelles)
   - [Scripts d'Amélioration](#scripts-damélioration)
8. [Conclusion](#conclusion)
   - [Bilan du Projet](#bilan-du-projet)
   - [Acquis Techniques](#acquis-techniques)
   - [Perspectives](#perspectives)
9. [Annexes](#annexes)
   - [Commandes Utiles](#commandes-utiles)
   - [Structure des Fichiers du Projet](#structure-des-fichiers-du-projet)
   - [Questions pour Amélioration](#questions-pour-amélioration)

---

## Introduction

### Contexte du Projet
Ce projet a pour objectif de mettre en place une architecture complète de traitement de données en temps réel utilisant Apache Spark Structured Streaming. L'infrastructure est conteneurisée avec Docker et utilise Hadoop HDFS pour le stockage distribué des données.

### Objectifs
- Déployer une architecture Big Data complète avec Docker Compose
- Implémenter une application Spark Structured Streaming en Java
- Configurer HDFS pour le stockage distribué
- Traiter des flux de données en temps réel
- Automatiser le déploiement et l'exécution des jobs Spark

---

## Architecture Technique

### Composants de l'Architecture

**Diagramme d'architecture globale**

![Architecture globale du système](architecture-diagram)

**Hadoop HDFS**
- **Namenode** : Gestionnaire des métadonnées HDFS
- **Datanode** : Stockage distribué des données
- **Ports** : 9870 (Web UI), 8020 (RPC)

**YARN Resource Manager**
- Gestion des ressources pour MapReduce
- Port : 8088 (Web UI)

**Spark Cluster**
- **Spark Master** : Coordinateur du cluster Spark
- **Spark Worker** : Exécution des tâches Spark
- **Ports** : 7077 (Master), 8080 (Web UI)

### Configuration Docker Compose

Extrait du `docker-compose.yml` :

```yaml
services:
  namenode:
    image: apache/hadoop:3.3.6
    hostname: namenode
    command: ["hdfs", "namenode"]
    ports:
      - "9870:9870"
      - "8020:8020"

  spark-master:
    image: spark:latest
    hostname: spark-master
    command:
      - "/opt/spark/bin/spark-class"
      - "org.apache.spark.deploy.master.Master"
      - "--host"
      - "spark-master"
      - "--port"
      - "7077"
      - "--webui-port"
      - "8080"
    ports:
      - "7077:7077"
      - "8080:8080"
    depends_on:
      - namenode
````

### Configuration Hadoop

Extrait des fichiers de configuration :

```text
CORE-SITE.XML_fs.defaultFS=hdfs://namenode
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=3
MAPRED-SITE.XML_mapreduce.framework.name=yarn
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager
```

---

## Implémentation de l'Application Spark

### Application de Streaming

Classe `Main.java` :

```java
public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession ss = SparkSession.builder()
                .appName("Structured streaming App")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("order_id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("client_id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("client_name", DataTypes.StringType, false, Metadata.empty()),
                // ... autres champs
        });

        Dataset<Row> inputDF = ss.readStream()
                .schema(schema)
                .option("header", true)
                .csv("hdfs://namenode:8020/data");

        StreamingQuery query = inputDF.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();
    }
}
```

### Structure des Données

| Champ       | Type    | Description                    |
| ----------- | ------- | ------------------------------ |
| order_id    | Long    | Identifiant unique de commande |
| client_id   | Long    | Identifiant du client          |
| client_name | String  | Nom du client                  |
| product     | String  | Produit commandé               |
| quantity    | Integer | Quantité                       |
| price       | Double  | Prix unitaire                  |
| order_date  | String  | Date de commande               |
| status      | String  | Statut de la commande          |
| total       | Double  | Total de la commande           |

---

## Déploiement et Exécution

### Étapes de Déploiement

```bash
# Construction du projet
mvn clean package

# Lancement de l'infrastructure
docker compose up -d

# Copie des données
docker cp orders1.csv namenode:/tmp/
docker cp orders2.csv namenode:/tmp/
docker cp orders3.csv namenode:/tmp/

# Copie de l'application
docker cp SparkLab5-1.0-SNAPSHOT.jar spark-master:/opt/spark/work-dir/

# Transfert vers HDFS
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put /tmp/orders1.csv /data/
docker exec namenode hdfs dfs -put /tmp/orders2.csv /data/
docker exec namenode hdfs dfs -put /tmp/orders3.csv /data/

# Exécution de l'application Spark
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class org.example.Main \
    /opt/spark/work-dir/SparkLab5-1.0-SNAPSHOT.jar \
    hdfs://namenode:8020/data
```

### Script d'Automatisation

```bash
#!/bin/bash
echo "🔨 Construction du projet..."
mvn clean package -DskipTests

echo "🐳 Lancement des conteneurs..."
docker compose up -d

echo "📁 Copie des fichiers de données..."
for file in orders{1..3}.csv; do
    docker cp $file namenode:/tmp/
done

echo "📤 Copie vers HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put /tmp/orders*.csv /data/

echo "⚡ Lancement de l'application Spark..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class org.example.Main \
    /opt/spark/work-dir/SparkLab5-1.0-SNAPSHOT.jar
```

---

## Résultats et Observations

### Sortie de l'Application

```text
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+---------+-----------+---------+--------+------+----------+------+------+
|order_id|client_id|client_name|product  |quantity|price |order_date|status|total |
+--------+---------+-----------+---------+--------+------+----------+------+------+
|1001    |101      |Client A   |Produit X|2       |25.99 |2024-01-15|PAID  |51.98 |
|1002    |102      |Client B   |Produit Y|1       |99.99 |2024-01-15|PENDING|99.99|
+--------+---------+-----------+---------+--------+------+----------+------+------+
```

### Interfaces de Surveillance

* **HDFS NameNode UI** : [http://localhost:9870](http://localhost:9870)
* **Spark Master UI** : [http://localhost:8080](http://localhost:8080)
* **YARN ResourceManager UI** : [http://localhost:8088](http://localhost:8088)

### Performances Observées

| Métrique                          | Valeur      |
| --------------------------------- | ----------- |
| Temps de démarrage du cluster     | 2-3 minutes |
| Latence de traitement des données | < 1 seconde |
| Taux de réplication HDFS          | 3           |
| Mémoire allouée par exécuteur     | 512MB       |
| Cœurs alloués                     | 2           |

---

## Problèmes Rencontrés et Solutions

### Problème 1 : Accès à HDFS

* **Symptôme** : "Input path does not exist"
* **Solution** : Vérification et correction du chemin HDFS, upload des fichiers

### Problème 2 : Compatibilité Java

* **Symptôme** : Erreurs avec Java 17
* **Solution** : Utilisation de Java 11 ou ajout des flags `--add-opens`

### Problème 3 : Ressources Insuffisantes

* **Symptôme** : Containers qui redémarrent
* **Solution** : Augmentation des ressources Docker à 4GB+

---

## Améliorations Possibles

### Améliorations Techniques

1. Ajout de Kafka pour ingestion en temps réel
2. Métriques Prometheus pour monitoring avancé
3. Autoscaling des Workers
4. Snapshots réguliers HDFS

### Améliorations Fonctionnelles

1. Traitement en temps réel des ventes
2. Alertes sur anomalies
3. Dashboard de visualisation
4. Intégration ML pour prédiction

### Scripts d'Amélioration

```bash
#!/bin/bash
echo "📊 Monitoring du Cluster Big Data"
echo "=================================="

echo "🗂️ HDFS Health:"
docker exec namenode hdfs dfsadmin -report | grep -A5 "Configured Capacity"

echo "⚡ Spark Status:"
curl -s http://localhost:8080 | grep -E "(ALIVE|WORKERS|APPLICATIONS)" | head -5

echo "💾 Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

---

## Conclusion

### Bilan du Projet

* Déploiement d'une architecture Big Data complète avec Docker Compose
* Utilisation de Hadoop HDFS pour le stockage distribué
* Traitement des données en temps réel avec Spark Structured Streaming

### Acquis Techniques

* Maîtrise du déploiement Docker
* Développement Spark Streaming en Java
* Gestion et transfert des données dans HDFS
* Monitoring et debugging de clusters distribués

### Perspectives

* Traiter des volumes plus importants
* Intégrer diverses sources de données
* Algorithmes ML sur les flux
* Déploiement en production avec HA

---

## Annexes

### Commandes Utiles

```bash
docker compose logs -f spark-master
docker exec -it namenode bash
docker compose down
docker compose restart spark-worker-1
docker system prune -af
```

### Structure des Fichiers du Projet

```
project/
├── docker-compose.yml
├── config
├── src/
│   └── main/
│       └── java/
│           └── org/
│               └── example/
│                   └── Main.java
├── target/
│   └── SparkLab5-1.0-SNAPSHOT.jar
├── data/
│   ├── orders1.csv
│   ├── orders2.csv
│   └── orders3.csv
└── scripts/
    ├── deploy.sh
    └── monitor.sh
```
