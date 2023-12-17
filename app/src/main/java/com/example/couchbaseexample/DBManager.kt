package com.example.couchbaseexample

import android.content.Context
import android.util.Log
import com.couchbase.lite.BasicAuthenticator
import com.couchbase.lite.Collection
import com.couchbase.lite.CollectionConfiguration
import com.couchbase.lite.CouchbaseLite
import com.couchbase.lite.DataSource
import com.couchbase.lite.Database
import com.couchbase.lite.DatabaseConfiguration
import com.couchbase.lite.Expression
import com.couchbase.lite.From
import com.couchbase.lite.Meta
import com.couchbase.lite.MutableArray
import com.couchbase.lite.MutableDictionary
import com.couchbase.lite.MutableDocument
import com.couchbase.lite.Parameters
import com.couchbase.lite.Query
import com.couchbase.lite.QueryBuilder
import com.couchbase.lite.Replicator
import com.couchbase.lite.ReplicatorChange
import com.couchbase.lite.ReplicatorConfigurationFactory
import com.couchbase.lite.ReplicatorType
import com.couchbase.lite.SelectResult
import com.couchbase.lite.URLEndpoint
import com.couchbase.lite.newConfig
import com.couchbase.lite.replicatorChangesFlow
import com.google.gson.Gson
import kotlinx.coroutines.flow.Flow
import java.net.URI

object DBManager {

    lateinit var database: Database
    lateinit var collection: com.couchbase.lite.Collection
    lateinit var replicator: Replicator

    // One-off initialization
    fun init(context: Context) {
        CouchbaseLite.init(context)
        Log.d(TAG, "CBL Initialized")

    }


    // Create a database
    fun createDb(dbName: String = "hotel-suphan", context: Context): Database {
//        val config = DatabaseConfiguration()
//        config.directory = context.filesDir.absolutePath
        database = Database(dbName)
        Log.d(TAG, "Database created: $dbName")
        return database
    }

    // List all of the collections in each of the scopes in the database
    fun orintAllScope() {
        database.scopes.forEach { scope ->
            Log.d(TAG, "Scope :: " + scope.name)
            scope.collections.forEach {
                Log.d(TAG, "    Collection :: " + it.name)
            }
        }
    }

    // Close a database
    fun closeDb() {
        database.close()
        Log.d(TAG, "Database close:")
    }

    // Create a new named collection (like a SQL table)
// in the database's default scope.
    fun createCollection(scopeName: String, collName: String): Collection {
        collection = database.createCollection(collName, scopeName)
        Log.d(TAG, "Collection created: $collection")
        return collection
    }


    // Create a new document (i.e. a record)
// and save it in a collection in the database.
    fun createBangliDoc(branch: Long): MutableDocument {
        val address = MutableDictionary()
        address.setString("street", "1 Main st.")
        address.setString("city", "San Francisco")
        address.setString("state", "CA")
        address.setString("country", "USA")
        address.setString("code", "90210")

        val phones = MutableArray()
        phones.addString("650-000-0000")
        phones.addString("650-000-0001")

        val mutableDocument = MutableDocument()
            .setString("type", "hotel")
            .setString("name", "Hotel Bangli")
//            .setFloat("room_rate", 121.75f)
            .setLong("branch", branch)
//            .setDictionary("address", address)
//            .setArray("phones", phones)
//        collection.save(mutableDocument)
        return mutableDocument
    }

    fun createAiyalaDoc(branch: Long): MutableDocument {
        val address = MutableDictionary()
        address.setString("street", "2 Main st.")
        address.setString("city", "San SO")
        address.setString("state", "AD")
        address.setString("country", "ITALY")
        address.setString("code", "333223")

        val phones = MutableArray()
        phones.addString("084-3425245")
        phones.addString("983-$branch")

        val mutableDocument = MutableDocument()
            .setString("type", "hotel")
            .setString("name", "Hotel Aiyala")
//            .setFloat("room_rate", 21.75f)
            .setLong("branch", branch)
//            .setDictionary("address", address)
//            .setArray("phones", phones)
//        collection.save(mutableDocument)
        return mutableDocument
    }

    fun createBangliAddressDoc(branch: Long): MutableDocument {
        val address = MutableDictionary()
        address.setString("street", "2 Main st.")
        address.setString("city", "San SO")
        address.setString("state", "AD")
        address.setString("country", "ITALY")
        address.setString("code", "333223")

//        val phones = MutableArray()
//        phones.addString("084-3425245")
//        phones.addString("983-$branch")

        val mutableDocument = MutableDocument()
            .setString("type", "Address")
            .setString("name", "Hotel Bangli")
//            .setFloat("room_rate", 21.75f)
            .setLong("branch", branch)
            .setDictionary("address", address)
//            .setArray("phones", phones)
//        collection.save(mutableDocument)
        return mutableDocument
    }

//    fun queryStatement(query: String) {
//        val thisQuery = thisDb.createQuery(
//            "SELECT META().id AS id FROM _ WHERE type = \"hotel\""
//        )
//
//        return thisQuery.execute().use { rs -> rs.allResults() }
//    }

    // Retrieve immutable document and log the database generated
// document ID and some document properties
    fun retrieveDoc(docId: String) {
        collection.getDocument(docId)
            ?.let {
                Log.d(TAG, "Document ID :: ${docId}")
                Log.d(TAG, "Learning :: ${it.getString("language")}")
            }
            ?: Log.d(TAG, "No such document :: $docId")
    }

    fun getAllDoc(collection: Collection): From {
        return QueryBuilder
//            .select(SelectResult.all())
            .select(
                SelectResult.expression(Meta.id),
                SelectResult.property("name"),
                SelectResult.property("type"),
                SelectResult.property("branch")
            )
            .from(DataSource.collection(collection))
    }


    // Retrieve immutable document and update `language` property
// document ID and some document properties
    fun updateDoc(docId: String, collection: Collection) {
        collection.getDocument(docId)?.let {
            collection.save(
                it.toMutable().setString("name", "Hotel Bangli Suphan")
            )
        }
    }

    fun updateAllDoc(db:Database, collection: Collection) {

        val hotels = mutableListOf<String>()
        db.let { database ->
            val query =
                database.createQuery("SELECT META().id AS id  FROM `test`.`bangli-address` ")


            query.execute().use {
                it.forEach {
                    hotels.add(it.getString("id").orEmpty())
                }
            }
            hotels.forEach {
                collection.getDocument(it)?.let {
                    collection.save(
                        it.toMutable().setString("name", "Hotel Bangli")
                    )
                }

            }
        }
    }


    // Create a query to fetch documents with language == Kotlin.
    fun queryDocs() {
        val coll = collection ?: return
        val query: Query = QueryBuilder.select(SelectResult.all())
            .from(DataSource.collection(coll))
            .where(Expression.property("language").equalTo(Expression.string("Kotlin")))
        query.execute().use { rs ->
            Log.d(TAG, "Number of rows :: ${rs.allResults().size}")
        }
    }


    // OPTIONAL -- if you have Sync Gateway Installed you can try replication too.
// Create a replicator to push and pull changes to and from the cloud.
// Be sure to hold a reference to the Replicator to prevent it from being GCed
    fun replicate(): Flow<ReplicatorChange>? {
        val coll = collection ?: return null

        val collConfig = CollectionConfiguration()
            .setPullFilter { doc, _ -> "Java" == doc.getString("language") }

        val repl = Replicator(
            ReplicatorConfigurationFactory.newConfig(
                target = URLEndpoint(URI("ws://localhost:4984/getting-started-db")),
                collections = mapOf(setOf(coll) to collConfig),
                type = ReplicatorType.PUSH_AND_PULL,
                authenticator = BasicAuthenticator("sync-gateway", "password".toCharArray())
            )
        )

        // Listen to replicator change events.
        val changes = repl.replicatorChangesFlow()

        // Start replication.
        repl.start()
        replicator = repl

        return changes
    }

}