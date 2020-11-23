package samples

import org.apache.hadoop.fs.DF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit, mean, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TP1 {
  def majuscule(s: String, filtre: String): String = {
    if (s.contains(filtre)) s
    else s.toUpperCase
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    /*EXERCICE 1 */

    //Question 1 : Lire le fichier "films.csv" sous forme de RDD[String]
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //Question 2 : Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val Dicaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    val nb_films = Dicaprio.count()
    println("Nombre de films de Di caprio : "+nb_films)

    //Question 3 : Quelle est la moyenne des notes des films de Di Caprio ?

    val notes = Dicaprio.map(item => (item.split(";")(2).toDouble))
    val mean_notes = notes.sum() / notes.count()
    println( "Moyenne des notes de Di Caprio : " + mean_notes)

    //Question 4 : Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?

    val vues = Dicaprio.map(item => (item.split(";")(1).toDouble))
    val total_vues = rdd.map(item => (item.split(";")(1).toDouble))
    val pourcentage_vues_diCaprio =(vues.sum() / total_vues.sum())*100
    println("Pourcentage des vues de Di Caprio : "+ pourcentage_vues_diCaprio)

    //Question 5 : Quelle est la moyenne des notes par acteur dans cet échantillon ?





    /* EXERCICE 2*/

    //Question 1 : Lire le fichier "films.csv" avec la commande suivante : spark.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("films.csv")
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")


    //Question 2 : Nommez les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal

    val df_renamed = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")
    df_renamed.show

    //Question 3 : Refaire les questions 2, 3, 4 et 5 en utilisant les DataFrames.

    //2
    val di_caprio = df_renamed.filter(col("acteur_principal") === "Di Caprio").count()
    println("Nombre de films de Di Caprio : " + di_caprio)

    //3
    val mean_di_caprio = df_renamed.groupBy("acteur_principal").mean("note_film").filter(col("acteur_principal") === "Di Caprio").select("avg(note_film)").first.get(0).toString.toDouble
    println("Moyenne des notes de Di Caprio : "+mean_di_caprio)

    //4
    val vues_di_caprio = df_renamed.groupBy("acteur_principal").sum("nombre_vues").filter(col("acteur_principal") === "Di Caprio").select("sum(nombre_vues)").first.get(0).toString.toDouble

    val tot_vues = df_renamed.agg(sum("nombre_vues")).first.get(0).toString.toDouble
    val pourcentage_di_caprio = vues_di_caprio / tot_vues

    println("Pourcentage de vues Di Caprio : " + pourcentage_di_caprio *100)

    //Question 4 : Créer une nouvelle colonne dans ce DataFrame, "pourcentage de vues", contenant le pourcentage de vues pour chaque film (combien de fois les films de cet acteur ont-ils été vus par rapport aux vues globales ?)

    val df_vues_par_film = df_renamed.withColumn("pourcentages_vues",(col("nombre_vues")/tot_vues) *100)
    println("DataFrame avec le pourcentage de vues par film")
    df_vues_par_film.show

  }

}
