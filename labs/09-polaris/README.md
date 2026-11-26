# Lab 09 : gouverner les tables Iceberg avec Polaris

## Fichiers

| Fichier | Rôle |
| --- | --- |
| `files.sh` | Crée le chart Helm de l'énoncé dans `polaris/deployment/` (Chart, values, job de bootstrap, secret PostgreSQL) |
| `deploy.sh` | Lab 1 : ConfigMap, secrets, `helm upgrade --install`, contrôle de santé, extraction des identifiants, partie « Advanced » |
| `run.sh` | Lab 2 : enchaîne les étapes Spark, l'exercice 2 et la synchronisation des métadonnées |
| `lab_polaris.py` | Une étape par session Spark (`dev`, `analyst`, `dev-check`, `reader`, `cleanup`), comme un redémarrage du kernel |
| `rbac_silver_reader.sh` | Exercice 2 : rôle `silver_reader` limité au namespace `silver`, via l'API de management |
| `deployment_output.txt`, `output_*.txt` | Sorties des scripts |
| `orders_before.txt`, `orders_after.txt` | Objets sous `warehouse/silver/orders/` avant et après l'enregistrement |

Les identifiants des principals sont écrits dans `/home/onyxia/work/.polaris-credentials.env`, hors du dépôt
(droits 600). Les journaux du bootstrap qui contiennent les secrets sont filtrés de `deployment_output.txt`.

## Écarts avec l'énoncé

- Le service tourne déjà avec les limites 2000m / 2500Mi : le patch du StatefulSet n'est pas rejoué.
- Le premier `helm dependency update` a échoué sur une erreur DNS passagère du cluster (`auth.docker.io`). Il a
  réussi au second essai.
- La table enregistrée pointe vers `v2.metadata.json` (lu dans `version-hint.text`), pas `v1`, et compte 2830
  commandes, pas 5000 : le jeu de données du cours en contient 2829 après nettoyage, plus une commande ajoutée par
  l'exercice de time travel du lab 07.
- Le notebook est remplacé par des scripts `spark-submit`. Chaque étape ouvre une nouvelle session, ce qui
  équivaut au redémarrage du kernel demandé pour changer de principal.

## Déploiement (lab 1)

- Pods : `polaris`, `polaris-postgresql-0`, job `polaris-bootstrap` terminé. Santé : `UP`.
- Catalogue `user-l-lebeldepenguilly-ece`, type `INTERNAL`, emplacement `s3://user-l-lebeldepenguilly-ece/warehouse/`.
- Principals : `root`, `dev`, `analyst`. Rôles de principal : `service_admin`, `data_engineer`, `data_analyst`.
- Rôles de catalogue : `catalog_admin`, `catalog_contributor` (CATALOG_MANAGE_ACCESS, CATALOG_MANAGE_METADATA,
  CATALOG_MANAGE_CONTENT), `catalog_reader` (TABLE_READ_DATA, TABLE_READ_PROPERTIES, CATALOG_READ_PROPERTIES).

## Résultats (lab 2)

| Étape | Résultat |
| --- | --- |
| `SHOW NAMESPACES IN lab` / `IN polaris` | `explore`, `silver` / vide |
| `register_table` sur `v2.metadata.json` | snapshot 4330986847496337037, 2830 lignes, 2 fichiers |
| Objets sous `warehouse/silver/orders/` | 9 avant, 9 après, aucun nouvel objet |
| `polaris.silver.users` créée par `dev` | 50 lignes, `00000-<uuid>.metadata.json` |
| `ADD COLUMN country` par `dev` | nouveau fichier `00001-<uuid>.metadata.json` |
| Lecture par `analyst` | `SHOW TABLES` et `GROUP BY product` autorisés |
| `INSERT` par `analyst` | `ForbiddenException ... not authorized for op ADD_TABLE_SNAPSHOT` |
| `ADD COLUMN phone` par `analyst` | `ForbiddenException ... not authorized for op ADD_TABLE_SCHEMA` |
| Snapshots de `silver.orders` ensuite | toujours 2 (overwrite, append), 2830 lignes |

## Réponses aux questions

**SparkSession avec Polaris**

- `SHOW NAMESPACES IN lab` est traité par Spark lui-même : le catalogue Hadoop liste les dossiers de
  `s3a://<bucket>/warehouse/` (`explore`, `silver`). `SHOW NAMESPACES IN polaris` part en requête HTTP vers le
  serveur Polaris (API REST Iceberg), qui répond à partir de sa base PostgreSQL. La réponse vide le montre : les
  dossiers existent dans le bucket, mais Polaris ne connaît encore aucun namespace.
- La credential (`clientId:clientSecret`) est utilisée par Spark dans une requête à Polaris, sur
  `/api/catalog/v1/oauth/tokens`, pour obtenir un jeton OAuth. Ce jeton accompagne ensuite chaque appel au
  catalogue. Elle n'est jamais envoyée au bucket : l'accès aux fichiers passe par les clés S3A de la session.

**Enregistrement de la table silver**

- `register_table` a écrit uniquement dans Polaris : une entrée `silver.orders` qui pointe vers
  `v2.metadata.json`. Rien n'a été écrit dans le bucket : 9 objets avant, 9 après.
- Après l'enregistrement, deux pointeurs désignent la même table : `version-hint.text` pour le catalogue `lab`,
  l'entrée PostgreSQL pour `polaris`. Un commit via `lab` crée un `v3.metadata.json` et avance seulement le
  pointeur Hadoop. Polaris continue de servir `v2`, et un commit via Polaris partirait de `v2` : deux historiques
  divergents, des écritures invisibles pour l'un ou l'autre catalogue, et des fichiers de données que les
  opérations de maintenance d'un catalogue jugeraient orphelins.

**Création de tables par `dev`**

- Polaris a rangé `silver.users` sous `s3://<bucket>/warehouse/silver/users/`, c'est-à-dire
  `default-base-location` du catalogue, puis le namespace, puis le nom de la table. Le dossier est le même que
  celui de la table Hadoop `lab.silver.users` : les deux tables y cohabitent, avec des noms de fichiers
  différents (`v1.metadata.json` côté Hadoop, `0000N-<uuid>.metadata.json` côté REST).
- Une table enregistrée garde ses fichiers à leur emplacement d'origine, écrits par un autre catalogue : Polaris
  ne fait que référencer son fichier de métadonnées. Une table créée via Polaris a ses fichiers placés et nommés
  par Polaris, sous son emplacement par défaut, et tout son historique passe par lui dès le départ.

**Évolution de schéma par `dev`**

- Le changement est enregistré dans un nouveau fichier, `00001-<uuid>.metadata.json`. Ce n'est pas le fichier
  pointé par `register_table` : celui-ci concernait `orders` et chaque commit Iceberg écrit un nouveau fichier de
  métadonnées au lieu de modifier l'ancien.
- Polaris vérifie le privilège `TABLE_WRITE_PROPERTIES` (opération `ADD_TABLE_SCHEMA`). `dev` le possède via
  `CATALOG_MANAGE_CONTENT`, qui inclut tous les privilèges sur les tables et les namespaces.

**Lecture par l'analyste**

- Il manque `TABLE_WRITE_DATA` pour l'`INSERT` (opération `ADD_TABLE_SNAPSHOT`). `catalog_reader` donne
  `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` et `CATALOG_READ_PROPERTIES`.
- L'`INSERT` refusé n'a créé aucun snapshot : `silver.orders` a toujours 2 snapshots et 2830 lignes. Spark écrit
  les fichiers de données avant le commit. Quand Polaris refuse le commit, Spark abandonne l'écriture (`aborting`,
  `aborted` dans le journal). Le refus porte sur le commit des métadonnées.

**Ce que Polaris ne peut pas imposer ici**

Polaris contrôle les opérations sur le catalogue, pas l'accès au stockage. Le stockage n'offre pas de credential
vending (`stsUnavailable: true`) : chaque session Spark lit et écrit les fichiers avec ses propres clés S3. Un
analyste peut donc lire les Parquet, ou les modifier, directement dans le bucket, sans passer par Polaris.

## Exercices

1. Suppression de la colonne `country` : `DESCRIBE` ne la montre plus, mais la table a toujours un seul snapshot
   (overwrite). `metadata_log_entries` passe de 2 à 3 fichiers (`00000`, `00001`, `00002`). La suppression est une
   opération de métadonnées : Iceberg retire la colonne du schéma courant et les fichiers Parquet restent
   intacts. Les lecteurs ignorent la colonne, identifiée par son id. `latest_schema_id` reste à 0 dans
   `metadata_log_entries` car cette colonne donne le schéma du dernier snapshot, et aucun nouveau snapshot n'a
   été créé.
2. Rôle `silver_reader`, créé via l'API de management avec le jeton `root` (`rbac_silver_reader.sh`) :
   - privilèges sur le namespace `silver` uniquement : `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA`,
     `NAMESPACE_READ_PROPERTIES` ;
   - chaîne : principal `silver_analyst` → rôle de principal `silver_analyst_role` → rôle de catalogue
     `silver_reader` ;
   - `dev` crée le namespace `gold` et la table `gold.orders_by_product` (6 produits).

   | Opération par `silver_analyst` | Résultat |
   | --- | --- |
   | `SHOW TABLES IN silver` | autorisé (`orders`, `users`) |
   | `SELECT` sur `silver.orders` / `silver.users` | autorisé (2830 / 50) |
   | `SHOW TABLES IN gold` | refusé, `LIST_TABLES` |
   | `SELECT` sur `gold.orders_by_product` | refusé, `LOAD_TABLE` |
   | `INSERT INTO silver.orders` | refusé, aucun privilège d'écriture |

   Un privilège accordé sur un namespace s'applique à ce namespace et à ses tables. Les autres namespaces restent
   inaccessibles.

## Nettoyage

`polaris.silver.users`, `polaris.gold.orders_by_product` et le namespace `gold` sont supprimés en tant que `dev`
(`output_cleanup.txt`). `polaris.silver.orders` reste enregistrée (2830 lignes) pour le lab Trino. Polaris reste
déployé. Les métadonnées des tables sont commitées dans `warehouse/silver/*/metadata` (commit
`feat(polaris-lab): add Polaris table metadata`), avant le nettoyage.
