package samples

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source



object ExerciseDf {

  def majuscule(s: String, filtre: String): String = {
    if(s.contains(filtre)) s
    else s.toUpperCase
  }

  val transformField = udf((date: String) => {
    val formatDate = new SimpleDateFormat("yyyy-MM-dd")
    formatDate.format(new Date(date))
  })
  val divideCost = udf((cost: Double) => {
    if(cost > 200) cost / 2;
    else cost
  })

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire le fichier en inférant les types
    val df:DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("data/data.csv")

    //On modifie le format des dates
    val transformDateDf = df.withColumn("Order Date", transformField(col("Order Date"))).withColumn("Ship Date", transformField(col("Ship Date")))
    //transformDateDf.printSchema()
    //transformDateDf.show

    //On divise les couts par deux si cout supérieur à 200
    val reduceCostDf = df.withColumn("Unit Cost", divideCost(col("Unit Cost")))
    //reduceCostDf.show

    //Question 2 : Combien avons-nous de produits ayant un prix unitaire supérieur à 500 et plus de 3000 unités vendues ?
    val products = df.filter(col("Unit Price") >= 500).filter(col("Units Sold") >= 3000).count()
    println(products)

    //Question 3 : Faites la somme de tous les produits vendus valant plus de $500 en prix unitaire
    val sum = df.filter(col("Unit Price") >= 100).groupBy("Unit Price").sum("Units Sold")
    sum.show

    //Question 4 : Quel est le prix moyen de tous les produits vendus ? (En groupant par item_type)
    val mean = df.groupBy("Item Type").mean("Unit Price")
    mean.show

    //Question 5 : Créer une nouvelle colonne, "total_revenue" contenant le revenu total des ventes par produit vendu.

    val revenue = df.withColumn("total_revenue2", col("Units Sold") * col("Unit Price"))
    //revenue.show

    //Question 6 : Créer une nouvelle colonne, "total_cost", contenant le coût total des ventes par produit vendu.

    val cost = revenue.withColumn("total_cost2", col("Units Sold") * col("Unit Cost"))
    //cost.show

    //Question 7 : Créer une nouvelle colonne, "total_profit", contenant le bénéfice réalisé par produit.

    val profit = cost.withColumn("Total Profit", col("total_revenue2") - col("total_cost2"))
    profit.show

    //Question 8 : Créer une nouvelle colonne, "unit_price_discount", qui aura comme valeur "unit_price" si le nombre d'unités vendues est plus de 3000.

    val discount = profit.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7)
      .otherwise(col("Unit Price")))

    //Question 9 : Faire répercuter ça sur les 3 colonnes créées précédemment : total_revenue, total_cost et total_profit.

    val revenue_discount = discount.withColumn("total_revenue2", col("Units Sold") * col("unit_price_discount"))
    val dataDF = revenue_discount.withColumn("total_profit2", col("total_revenue2") - col("total_cost2"))
    dataDF.show

    //Question 10 :Écrire le résultat sous format parquet, partitionné par formatted_ship_date

    //On doit d'abord supprimer les espaces dans les colonnes
    val columnsList = dataDF.columns
    val columnsWithoutSpace: Array[String] = columnsList.map(elem => elem.replaceAll(" ", "_"))
    //columnsWithoutSpace.foreach(println)

    //On affecte nos colonnes au dataframe et on le write en parquet
    val DfWithRightColumnNames = dataDF.toDF(columnsWithoutSpace:_*)
    DfWithRightColumnNames.show()

    //On écrit dans le fichier parquet, en partitionnant selon la colonne de notre choix
    DfWithRightColumnNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    parquetDF.printSchema()
    parquetDF.show

  }
}
