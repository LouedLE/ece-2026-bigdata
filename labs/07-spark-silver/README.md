# Lab 07 : de bronze à silver avec Spark DataFrames, Parquet et Iceberg

Scripts : `run.sh` (notebook principal `lab_silver.py`, comparaison avec dbt), `commit.sh` (métadonnées Iceberg),
`run_ex.sh` (exercices `exercises.py`). Module commun : `session.py` (session Spark, S3A, catalogue Iceberg `lab`).
Sorties : `output.txt` et `exercises_output.txt`. Spark 4.1.1, Iceberg 1.11.0, exécution `local[2]`.

## Écarts avec l'énoncé

- Les identifiants S3 sont lus dans le profil `default` (`aws configure export-credentials`) : les variables
  `AWS_*` du service expirent.
- Le disque du nœud était presque plein : les fichiers temporaires de Spark (`SPARK_LOCAL_DIRS`), le cache Ivy et
  le cache uv sont placés sur le volume persistant `/home/onyxia/work`.
- Mémoire du driver limitée à 1 Go pour rester sous la limite du conteneur.

## Résultats

| Mesure | Valeur |
| --- | --- |
| Commandes après typage | 2829 |
| Avec 3 doublons injectés / après dédoublonnage | 2832 / 2829 |
| Dédoublonnage DataFrame et SQL | identiques |
| Chaînes vides restantes dans `state` et `zip_code` | 0 |
| Comparaison dbt / Spark (commandes, unités, users) | 2829, 8615, 50 des deux côtés |
| Tables Iceberg `lab.silver.users` / `lab.silver.orders` | 50 / 2829 lignes, 1 fichier chacune |

Types : dbt écrit `order_id` en `UUID` et `ordered_at` en `TIMESTAMP WITH TIME ZONE`, Spark écrit `VARCHAR` et
`TIMESTAMP`, car Spark n'a pas de type UUID et son `timestamp` est implicitement en UTC.

## Exercices

1. `birthdate_is_valid` : 5 users invalides sur 50, même résultat en DataFrame et en SQL (0 ligne d'écart).
2. Extraction du code postal : `regexp_extract` 212 ms, UDF Python 208 ms, `pandas_udf` 215 ms sur 50 lignes.
   À ce volume, le coût fixe du job domine. Sur de gros volumes, `regexp_extract` reste dans la JVM, alors que les
   UDF sérialisent chaque ligne vers Python, par lots Arrow pour `pandas_udf`.
3. Cache des DataFrames bronze : 1596 ms pour la première action (lecture S3), 492 ms pour la seconde (mémoire).
4. Ajout d'une commande : la table Iceberg passe de 2829 à 2830 lignes et garde l'ancien snapshot, lisible avec
   `VERSION AS OF`. Le répertoire Parquet passe aussi à 2830 lignes, mais l'état précédent n'est plus lisible.
