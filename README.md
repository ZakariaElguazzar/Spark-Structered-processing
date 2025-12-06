```markdown
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
    command: ["/opt/spark/bin/spark-class", 
              "org.apache.spark.deploy.master.Master",
              "--host", "spark-master", 
              "--port", "7077", 
              "--webui-port", "8080"]
    ports: ["7077:7077", "8080:8080"]
    depends_on: [namenode]
```

### Configuration Hadoop

Extrait des fichiers de configuration :

```
CORE-SITE.XML_fs.defaultFS=hdfs://namenode
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=3
MAPRED-SITE.XML_mapreduce.framework.name=yarn
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager
```

---

## Implémentation de l'Application Spark

### Application de Streaming

Classe Main.java :

```java
public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession ss = SparkSession.builder()
                .appName("Structured streaming App")
                .getOrCreate();
        
        StructType schema = new StructType(new StructField[]{
                new StructField("order_id", DataTypes.LongType, false, 
                Metadata.empty()),
                new StructField("client_id", DataTypes.LongType, false, 
                Metadata.empty()),
                new StructField("client_name", DataTypes.StringType, false, 
                Metadata.empty()),
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

| Champ | Type | Description |
|-------|------|-------------|
| order_id | Long | Identifiant unique de commande |
| client_id | Long | Identifiant du client |
| client_name | String | Nom du client |
| product | String | Produit commandé |
| quantity | Integer | Quantité |
| price | Double | Prix unitaire |
| order_date | String | Date de commande |
| status | String | Statut de la commande |
| total | Double | Total de la commande |

---

## Déploiement et Exécution

### Étapes de Déploiement

1. **Construction du Projet**
   ```bash
   mvn clean package
   ```

2. **Lancement de l'Infrastructure**
   ```bash
   docker compose up -d
   ```

3. **Copie des Données**
   ```bash
   docker cp orders1.csv namenode:/tmp/
   docker cp orders2.csv namenode:/tmp/
   docker cp orders3.csv namenode:/tmp/
   ```

4. **Copie de l'Application**
   ```bash
   docker cp SparkLab5-1.0-SNAPSHOT.jar spark-master:/opt/spark/work-dir/
   ```

5. **Transfert vers HDFS**
   ```bash
   # Dans le conteneur namenode
   hdfs dfs -mkdir -p /data
   hdfs dfs -put /tmp/orders1.csv /data/
   hdfs dfs -put /tmp/orders2.csv /data/
   hdfs dfs -put /tmp/orders3.csv /data/
   ```

6. **Exécution de l'Application Spark**
   ```bash
   /opt/spark/bin/spark-submit \
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

```
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

**HDFS NameNode UI** (http://localhost:9870)
- Vue d'ensemble du cluster HDFS
- Exploration des fichiers
- Surveillance des Datanodes

**Spark Master UI** (http://localhost:8080)
- Liste des applications en cours
- Statut des Workers
- Logs d'exécution

**YARN ResourceManager UI** (http://localhost:8088)
- Gestion des ressources
- Historique des jobs
- Métriques de performance

### Performances Observées

| Métrique | Valeur |
|----------|--------|
| Temps de démarrage du cluster | 2-3 minutes |
| Latence de traitement des données | < 1 seconde |
| Taux de réplication HDFS | 3 |
| Mémoire allouée par exécuteur | 512MB |
| Cœurs alloués | 2 |

---

## Problèmes Rencontrés et Solutions

### Problème 1 : Accès à HDFS
**Symptôme** : Erreur "Input path does not exist"  
**Cause** : Chemin HDFS incorrect ou fichiers non présents  
**Solution** : Vérification et correction du chemin, upload des fichiers

### Problème 2 : Compatibilité Java
**Symptôme** : Erreurs de compatibilité avec Java 17  
**Cause** : Spark 3.5.0 nécessite des flags spécifiques  
**Solution** : Utilisation de Java 11 ou ajout des flags `--add-opens`

### Problème 3 : Ressources Insuffisantes
**Symptôme** : Containers qui redémarrent  
**Cause** : Mémoire Docker insuffisante  
**Solution** : Augmentation des ressources Docker à 4GB+

---

## Améliorations Possibles

### Améliorations Techniques
1. **Ajout de Kafka** : Pour une ingestion de données en temps réel
2. **Métriques Prometheus** : Surveillance avancée du cluster
3. **Autoscaling** : Ajout automatique de workers selon la charge
4. **Backup HDFS** : Configuration de snapshots réguliers

### Améliorations Fonctionnelles
1. **Traitement en temps réel** : Agrégations continues des ventes
2. **Alertes** : Notification sur anomalies détectées
3. **Dashboard** : Interface de visualisation des données
4. **ML Integration** : Prédiction de tendances de vente

### Scripts d'Amélioration

```bash
#!/bin/bash
# Monitoring avancé du cluster
echo "📊 Monitoring du Cluster Big Data"
echo "=================================="

# Santé HDFS
echo "🗂️ HDFS Health:"
docker exec namenode hdfs dfsadmin -report | grep -A5 "Configured Capacity"

# Statut Spark
echo "⚡ Spark Status:"
curl -s http://localhost:8080 | grep -E "(ALIVE|WORKERS|APPLICATIONS)" | head -5

# Utilisation des ressources
echo "💾 Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

---

## Conclusion

### Bilan du Projet
Le projet a permis de déployer avec succès une architecture Big Data complète utilisant :
- Docker Compose pour l'orchestration des conteneurs
- Hadoop HDFS pour le stockage distribué
- Apache Spark pour le traitement des données
- Structured Streaming pour le traitement en temps réel

### Acquis Techniques
- Maîtrise du déploiement Docker d'infrastructure Big Data
- Développement d'applications Spark Streaming en Java
- Gestion de HDFS et transfert de données
- Surveillance et debugging d'applications distributées

### Perspectives
Cette architecture constitue une base solide pour des projets de traitement de données à grande échelle. Elle pourrait être étendue pour :
- Traiter des volumes de données plus importants
- Intégrer des sources de données variées
- Implémenter des algorithmes de machine learning
- Déployer en production avec haute disponibilité

---

## Annexes

### Commandes Utiles

```bash
# Voir les logs d'un service
docker compose logs -f spark-master

# Accéder à un conteneur
docker exec -it namenode bash

# Arrêter le cluster
docker compose down

# Redémarrer un service
docker compose restart spark-worker-1

# Nettoyer les conteneurs
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
