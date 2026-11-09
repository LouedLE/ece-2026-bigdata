# Lab 06 : API RDD de Spark

Script : `lab_rdd.py`, exécuté avec `spark-submit --master 'local[*]' labs/06-rdd/lab_rdd.py`
depuis la racine du projet, après téléchargement de `users.csv` et `orders.csv` depuis `bronze/`.
Sortie complète : `output.txt`. Spark 4.1.1, Scala 2.13, 2 cœurs.

## Résultats clés

| Mesure | Valeur |
| --- | --- |
| Partitions par défaut de `textFile()` | 2 (`defaultMinPartitions` = 2) |
| Lignes physiques de `users.csv` | 101 |
| Enregistrements users après filtre UUID | 50 |
| Commandes après suppression de l'en-tête | 2829 |
| Shuffle `reduceByKey` (écriture / lecture) | 2 513 o / 1 065 o |
| Shuffle `groupByKey` (écriture / lecture) | 6 230 o / 2 643 o |
| Dédoublonnage avec 3 doublons injectés | 2832 → 2829 |
| Commandes sans user (`leftOuterJoin`) | 0, puis 1 avec l'ID factice |

## Réponses aux questions

**Partitions.** `textFile()` prend `max(defaultMinPartitions, nombre de splits)`. Le fichier fait moins d'un bloc
(32 Mo en local), donc un seul split, et `defaultMinPartitions = min(defaultParallelism, 2)` impose 2 partitions.
`repartition()` déclenche un shuffle complet. `coalesce()` fusionne des partitions voisines sans shuffle, et sert
seulement à réduire leur nombre.

**Évaluation paresseuse.** Le second `count()` crée un nouveau job et un nouveau stage (mesuré : 1 stage à chaque
appel). Rien n'est mis en cache : Spark relit et refiltre le fichier. Un `.cache()` sur `orders_data` placé avant le
premier `count()` ferait lire le second depuis la mémoire.

**Adresse multiligne.** `spark.read.csv(multiLine=True)` interprète le format CSV et sait qu'un saut de ligne entre
guillemets fait partie du champ. `textFile()` découpe sur chaque `\n` sans connaître le format. Un lecteur ligne à
ligne n'est sûr que si chaque enregistrement tient sur une ligne : aucun séparateur de ligne dans les valeurs
(JSON Lines, CSV sans champ multiligne).

**Parsing manuel.** Une virgule de trop dans `product` donne 6 champs : le `split` lève un `ValueError`, et le job
échoue à la première action qui touche cette ligne (testé : `Py4JJavaError`). L'API DataFrame applique le schéma à la
lecture, avec un mode `PERMISSIVE` (NULL + `_corrupt_record`), `DROPMALFORMED` ou `FAILFAST`.

**`reduceByKey` contre `groupByKey`.** `reduceByKey` additionne d'abord dans chaque partition et n'envoie qu'une paire
par clé et par partition. `groupByKey` envoie chaque valeur : 2,5 fois plus d'octets écrits ici, et beaucoup plus sur
de vraies volumétries, avec un risque de saturer la mémoire sur une clé chaude. `groupByKey` reste nécessaire quand
le calcul exige toutes les valeurs à la fois : médiane, tri des valeurs, liste complète, fenêtre glissante.

**Dédoublonnage.** Le shuffle de `reduceByKey` regroupe toutes les lignes d'un même `order_id` sur une partition, le
rôle du `partitionBy` de la fenêtre. La fonction de fenêtre exprime l'intention plus directement : « une ligne par
clé, la plus récente ». Le combineur maison cache cette règle dans une fonction Python.

**Jointure.** Le shuffle disparaît si les deux RDD partagent le même partitionneur et le même nombre de partitions
(`partitionBy(4)` des deux côtés, voir la lignée dans `output.txt`). Avec `leftOuterJoin()`, une commande sans user
reste présente avec `None` côté user, au lieu de disparaître. Un `collect()` sur `enriched` rapatrie tout dans le
driver : sur un gros volume, dépassement mémoire du driver.

## Exercices

1. `aggregateByKey((0, 0), ...)` produit `(nombre_commandes, quantité_totale)` en une passe, puis la moyenne. Les
   totaux sont identiques à `user_quantity` (vérifié par `assert`).
2. Une commande avec un `user_id` factice ressort seule du filtre `leftOuterJoin` : 1 orpheline.
3. Version DataFrame en 1 ligne contre 2 pour le RDD, avec les mêmes totaux. Le plan montre une agrégation partielle
   (`partial_sum`) avant l'`Exchange`, l'équivalent automatique du combineur de `reduceByKey`, et ne lit que les
   colonnes `user_uuid` et `quantity`.
4. Avec cache, `user_quantity.count()` prend 0,16 s et l'onglet Storage liste 3 RDD en mémoire. Sans cache, 0,45 s et
   l'onglet Storage est vide. Sur 330 Ko l'écart reste faible, il grandit avec le volume relu.
