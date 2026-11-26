# Lab 08 : introduction à Apache Iceberg

Script : `run.sh` lance `lab_iceberg.py` avec `spark-submit` (Spark 4.1.1, Iceberg 1.11.0, `local[2]`).
Il réutilise `../07-spark-silver/session.py` (session Spark, S3A, catalogue Hadoop `lab` sur
`s3a://<bucket>/warehouse`). Sorties : `output.txt` (résultats Spark) et `layout.txt` (fichiers S3 des tables).

## Écarts avec l'énoncé

- Le lab tourne sur des copies des tables silver, dans l'espace de noms `lab.explore`
  (`lab.explore.orders` et `lab.explore.users`). Les tables `lab.silver.*` restent intactes pour les labs
  Polaris et Trino, qui en ont besoin.
- Pour la même raison, l'étape de nettoyage (suppression de `warehouse/`) n'est pas exécutée.
- La table de départ contient 2830 commandes et non 2829 : l'exercice de time travel du lab 07 a ajouté une
  commande à `lab.silver.orders`.

## Résultats

| Étape | Snapshots | Fichiers de données | Lignes |
| --- | --- | --- | --- |
| Copie de `lab.silver.orders` | 1 (overwrite) | 1 (84 Ko) | 2830 |
| Ajout de 3 commandes | 2 (+ append) | 2 | 2833 |
| `ADD COLUMN source` | 2 | 2 | 2833 |
| `INSERT` avec `source = 'spark'` | 3 (+ append) | 3 | 2834 |

Time travel : `VERSION AS OF <premier snapshot>` renvoie 2830 lignes, la version courante 2833 (avant l'insert
de la colonne `source`). Après l'évolution de schéma, les anciennes lignes lisent `source = NULL` (2833) et la
nouvelle `source = 'spark'` (1).

## Réponses aux questions

**Structure d'une table**

- Les lignes sont dans les fichiers Parquet de `data/`.
- La table est décrite par `metadata/` : les fichiers `vN.metadata.json` (schéma, partitionnement, liste des
  snapshots, snapshot courant), les manifest lists `snap-*.avro` et les manifests `*-m0.avro` (liste des fichiers
  de données avec leurs statistiques). `version-hint.text` pointe vers la dernière version pour le catalogue Hadoop.
- Un dossier de Parquet ne dit pas quels fichiers font partie de la table, ni quel est le schéma de référence, ni
  quelles versions ont existé. Iceberg ajoute cette couche : un état de table défini, versionné et cohérent.

**Tables de métadonnées**

- Après la copie : 1 snapshot et 1 fichier de données.
- La table `snapshots` donne l'identifiant, le parent, l'opération (overwrite, append), la date de commit et un
  résumé (lignes ajoutées, fichiers ajoutés). Un fichier Parquet ne contient que des lignes et son propre schéma.

**Second snapshot**

- 2 snapshots après l'ajout.
- Le fichier Parquet d'origine reste en place. L'append écrit un nouveau fichier (3 lignes) et un nouveau
  snapshot qui référence les deux fichiers.
- Un nouveau snapshot garde l'ancien état lisible : les requêtes en cours lisent une version stable, on peut
  revenir en arrière et l'écriture ne bloque pas les lectures.

**Time travel**

- Le time travel lit l'état de la table décrit par un snapshot : sa manifest list, donc sa liste de fichiers.
- Les fichiers sont immuables. Une ancienne version réutilise les fichiers de données déjà présents, seules les
  métadonnées changent. Aucune copie complète n'est nécessaire.
- Si les métadonnées ou les fichiers d'un ancien snapshot sont supprimés physiquement, ce snapshot devient
  illisible : la requête échoue sur un fichier manquant. C'est le rôle de `expire_snapshots`, qui retire le
  snapshot des métadonnées avant de supprimer les fichiers qui ne servent plus.

**Évolution de schéma**

- Aucun fichier Parquet existant n'a été réécrit : `ADD COLUMN` crée une nouvelle version de métadonnées
  (`v3.metadata.json`) sans nouveau snapshot de données.
- Le nouveau schéma est enregistré dans le fichier `metadata.json` (liste `schemas` et `current-schema-id`).
  Pour `users`, `metadata_log_entries` montre le passage de `latest_schema_id` 0 à 1.
- Iceberg identifie les colonnes par un identifiant, pas par leur nom ni leur position. Le schéma appartient à
  la table : les anciens fichiers sans la colonne sont lus avec `NULL`, sans réécriture. Modifier un fichier
  Parquet isolé laisserait des fichiers aux schémas incohérents.

**Parquet contre Iceberg**

- `silver/spark/orders.parquet/` est une simple collection de fichiers Parquet (plus un marqueur `_SUCCESS`).
- `warehouse/silver/orders/` a des métadonnées et des snapshots.
- Remplacer un dossier Parquet pendant une lecture est risqué : le lecteur voit un mélange d'anciens et de
  nouveaux fichiers, ou des fichiers absents. La méthode sûre consiste à écrire dans un nouveau dossier puis à
  basculer les lecteurs dessus, ce qui revient à refaire à la main ce que fait Iceberg.
- Iceberg rend l'état visible de façon atomique : les fichiers sont écrits d'abord, puis un seul échange de
  pointeur vers le nouveau `metadata.json` publie le commit. Un lecteur voit l'ancien état ou le nouveau, jamais
  un état intermédiaire.
- Avec plusieurs moteurs (Spark, Trino, DuckDB), il faut une source unique qui indique le `metadata.json`
  courant de chaque table et arbitre les commits concurrents. C'est le catalogue. Sans lui, chaque moteur devrait
  deviner la dernière version depuis le stockage, sans garantie d'atomicité.

## Exercices

1. Table Iceberg des users : `lab.explore.users`, 50 lignes, 1 snapshot overwrite.
2. Ajout de 5 users : 2 snapshots (overwrite 50, append 5).
3. Time travel : 50 lignes au premier snapshot, 55 après l'ajout.
4. Colonne `country` ajoutée, puis 2 lignes insérées avec `country = 'US'` : 55 lignes à `NULL`, 2 à `US`.
   `metadata_log_entries` liste 4 versions, le schéma passe de l'id 0 à l'id 1 à la quatrième.
5. Comparaison des layouts S3 (`layout.txt`) : les deux tables ont la même structure, `data/` et `metadata/`.
   Chacune a 3 fichiers Parquet (un par écriture), 3 manifests, 3 manifest lists (un par snapshot) et 4 fichiers
   `vN.metadata.json` (un par changement, y compris l'évolution de schéma qui ne crée pas de snapshot). Seuls la
   taille des fichiers et le contenu changent.
6. Parquet, Iceberg et catalogue :
   - Parquet est un format de fichier colonne. Il stocke des lignes et leur schéma, rien de plus.
   - Iceberg est un format de table. Il organise un ensemble de fichiers Parquet avec des métadonnées : schéma
     versionné, snapshots, liste exacte des fichiers, commits atomiques.
   - Un catalogue Iceberg associe un nom de table (`lab.silver.orders`) à son `metadata.json` courant et garantit
     l'atomicité des commits. Ici c'est un catalogue Hadoop basé sur le stockage. Au lab 09, Polaris joue ce rôle
     sous forme de service REST partagé entre moteurs.
