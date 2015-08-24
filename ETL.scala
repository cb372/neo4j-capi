import com.gu.contentapi.client._
import com.gu.contentapi.client.model._
import com.squareup.okhttp._

import java.util.Base64
import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext.Implicits.global

object ETL extends App {

  val capiKey = args(0)
  val neo4jPassword = args(1)

  val capiClient = new GuardianContentClient(capiKey)
  
  val Json = MediaType.parse("application/json; charset=utf-8")
  val http = new OkHttpClient()

  loadSections()
  loadTags()
  //loadContents(1000)

  def loadSections() = {
    capiClient.getResponse(SectionsQuery()) map { response =>
      val statements = response.results map { section =>
        val escapedTitle = section.webTitle.replaceAllLiterally("'", "\\\\'")
        s"MERGE (s:Section { id:'${section.id}', webTitle:'$escapedTitle' })"
      }
      sendToNeo4j(statements)
    }
  }

  def loadTags() = {
    // TODO could add references to the graph as well

    val query = TagsQuery().pageSize(100)
    var currentPage = 1
    var pages = -1
    while (currentPage != pages) {
      capiClient.getResponse(query.page(currentPage)) map { response =>
        val statements = response.results map { tag =>
          val escapedTitle = tag.webTitle.replaceAllLiterally("'", "\\\\'")
          tag.sectionId match {
            case Some(sId) =>
              s"MATCH (s:Section {id:'${sId}'}) MERGE (t:Tag { id:'${tag.id}', webTitle:'$escapedTitle' })-[:IN_SECTION]->(s)"
            case None =>
              s"MERGE (t:Tag { id:'${tag.id}', webTitle:'$escapedTitle' })"
          }
        }
        sendToNeo4j(statements)
        println(s"Sent 100 tags to Neo4j (page = $currentPage)")
        Thread.sleep(1000)

        currentPage = currentPage + 1
        pages = response.pages
      }
    }
  }


  def sendToNeo4j(statements: Seq[String]) = {
    val stmtObjects = statements map { stmt =>
      s"""{ "statement": "$stmt" }"""
    }
    val json = s"""
    {
      "statements": [
        ${ stmtObjects mkString ",\n" }
      ]
    }
    """

    println(json)
    val requestBody = RequestBody.create(Json, json)
    val request = new Request.Builder()
      .url("http://localhost:7474/db/data/transaction/commit")
      .addHeader("Authorization", Credentials.basic("neo4j", neo4jPassword))
      .post(requestBody)
      .build
    val response = http.newCall(request).execute()
    println(s"Neo4j responded with ${response.code}. ${response.body.string}")
    println(s"${response.headers}")
  }

}
