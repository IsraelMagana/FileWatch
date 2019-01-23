import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.mongodb.spark._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config._
import org.apache.spark.sql.SQLContext
import org.bson.Document
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.col


object FileWatch {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    var contador=0;
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    
    val result = sqlContext.sql("use ci_trans_upl_prueba")
    var query = sqlContext.sql("SELECT * FROM tabla_final_bloqueos")
    var cuenta = query.filter(query("fec_ult_ope")==="2017-09-24").count()
    println("======================!!!!!!!!!!!!!!!!!!============================")
    println("======================!!!!!!!!!!!!!!!!!!============================")
    println("======================!!!!!!!!!!!!!!!!!!============================")
    println("Cuenta:" + cuenta)
    println("======================!!!!!!!!!!!!!!!!!!============================")
    println("======================!!!!!!!!!!!!!!!!!!============================")
    while(cuenta <= 0)
    {
        println("Entrando a dormir")
        Thread.sleep(10000)
        println("Saliendo de dormir")
        println("Bandera no activa")
        query = sqlContext.sql("SELECT * FROM tabla_final_bloqueos")
        cuenta = query.filter(query("fec_ult_ope")==="2017-09-24").count()
        contador = contador + 1
        
        if(contador==1000)
        {
          System.exit(1)
        }
    } 
    
    println("Ejecución exitosa")
    System.exit(0)
    
  }
}
