from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 1. Initialiser la session Spark
spark = SparkSession.builder.appName("JointureEtudiantsNotes").getOrCreate()

# 2. Charger les fichiers CSV en DataFrames
etudiants = spark.read.csv("/tmp/etudiant.csv", header=True, inferSchema=True)
notes = spark.read.csv("/tmp/notes.csv", header=True, inferSchema=True)

# 3. Afficher les colonnes pour vérifier
print("Colonnes étudiants:", etudiants.columns)
print("Colonnes notes:", notes.columns)

# 4. Réaliser la jointure sur "id"
df_joint = etudiants.join(notes, "id")

# 5. Calculer la moyenne de la classe
moyenne_classe = df_joint.select(avg(col("mark"))).collect()[0][0]
print(f"\n✅ Moyenne de la classe : {moyenne_classe:.2f}/20\n")

# 6. Filtrer les étudiants ayant une note supérieure à 15
top_etudiants = df_joint.filter(col("mark") > 15)

# 7. Afficher et sauvegarder le résultat
top_etudiants.show()
top_etudiants.write.csv("/tmp/etudiants_top.csv", header=True, mode="overwrite")

# 8. Arrêter Spark
spark.stop()