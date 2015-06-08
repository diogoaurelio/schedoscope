package schedoscope.example.osm.datahub

import org.schedoscope.dsl.View
import org.schedoscope.dsl.views.Id
import org.schedoscope.dsl.views.JobMetadata
import schedoscope.example.osm.processed.Nodes
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.views.DateParameterizationUtils.allMonths
import org.schedoscope.dsl.transformations.HiveTransformation
import org.schedoscope.dsl.transformations.HiveTransformation.insertInto
import org.schedoscope.dsl.transformations.HiveTransformation.queryFromResource
import schedoscope.example.osm.Globals._
import org.schedoscope.dsl.Parquet
import org.schedoscope.dsl.transformations.PigTransformation
import org.schedoscope.Settings
import org.schedoscope.dsl.transformations.PigTransformation.scriptFromResource
import org.schedoscope.dsl.TextFile

case class Trainstations() extends View
    with Id
    with JobMetadata {

  val station_name = fieldOf[String]
  val area = fieldOf[String]

  // transform via Pig
  val nodes = dependsOn(() =>
    for ((year, month) <- allMonths())
      yield Nodes(p(year), p(month)))

  transformVia(() =>
    PigTransformation(
      scriptFromResource("pig_scripts/datahub/insert_trainstations.pig")).configureWith(
        Map(
          //"input_table" -> .tableName, FIXME: extract input table from nodes()
          "output_table" -> this.tableName,
          "env" -> this.env,
          "exec.type" -> "MAPREDUCE",
          "mapred.job.tracker" -> Settings().jobTrackerOrResourceManager,
          "fs.default.name" -> Settings().nameNode)))

  comment("View of trainstations generated by Pig transformation")

  storedAs(Parquet())
}