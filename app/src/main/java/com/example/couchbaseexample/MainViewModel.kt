package com.example.couchbaseexample

import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.couchbase.lite.Collection
import com.couchbase.lite.DataSource
import com.couchbase.lite.Database
import com.couchbase.lite.Expression
import com.couchbase.lite.IndexBuilder
import com.couchbase.lite.Join
import com.couchbase.lite.Meta
import com.couchbase.lite.Parameters
import com.couchbase.lite.QueryBuilder
import com.couchbase.lite.QueryChange
import com.couchbase.lite.SelectResult
import com.couchbase.lite.ValueIndexItem
import com.couchbase.lite.queryChangeFlow
import com.google.gson.Gson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

class MainViewModel : ViewModel() {

    // Expose screen UI state
    private val _uiState = MutableStateFlow(DiceUiState())
    val uiState: StateFlow<DiceUiState> = _uiState.asStateFlow()
    lateinit var  database: Database

    // Handle business logic
    fun countDocument(collection: com.couchbase.lite.Collection): Int {
//        _uiState.update { currentState ->
//            currentState.copy(
//                firstDieValue = Random.nextInt(from = 1, until = 7),
//                secondDieValue = Random.nextInt(from = 1, until = 7),
//                numberOfRolls = currentState.numberOfRolls + 1,
//            )
//        }
        val countParam = "count"
        var counter = 0
        viewModelScope.launch {
            val result = QueryBuilder
                .select(
                    SelectResult.expression(
                        com.couchbase.lite.Function.count(
                            Expression.string(
                                "*"
                            )
                        )
                    ).`as`(countParam)
                )
                .from(DataSource.collection(collection))
                .where( //4
                    Expression.property("branch")
                        .equalTo(
                            Expression.longValue(2)
                        )
                )

            result.execute().use {
                counter = it.allResults()[0].getInt(countParam)
            }
        }
        _uiState.update { currentState ->
            currentState.copy(
                numberOfRolls = counter
            )
        }
        return counter
    }

    fun countDocumentFlow(): Flow<List<Hotel>> {
//        val collection = database.getScope(SCOPE_TEST)?.getCollection(BANGLI_COLLECTION)
        val collection = DBManager.database.createCollection(BANGLI_COLLECTION, SCOPE_TEST)
        val query = QueryBuilder
            .select(
                SelectResult.expression(Meta.id),
                SelectResult.property("name"),
                SelectResult.property("type"),
                SelectResult.property("branch")
            )
            .from(DataSource.collection(collection))


        val flow = query        // <1>
            .queryChangeFlow()  // <1>
            .map { qc -> mapQueryChangeToProject(qc) } // <2>
            .flowOn(Dispatchers.IO)  // <3>
        query.execute()  // <4>

        return flow
    }

    fun createTeamTypeIndex(database: Database){
        val teamIndexName = "test-index"
        try {
            Log.d(TAG, "createTeamTypeIndex: ${database.indexes.contains(teamIndexName)}")
            database.let {  // <1>
                if (!it.indexes.contains(teamIndexName)) {
                    // create index for ProjectListView to only return documents with
                    // the type attribute set to project and the team attribute set to the
                    // logged in users team
                    it.createIndex( // <2>
                        teamIndexName, // <3>
                        IndexBuilder.valueIndex(   // <4>
                            ValueIndexItem.property("name"), // <5>
                            ValueIndexItem.property("type")) // <5>
                    )
                }
            }
        } catch (e: Exception){
            android.util.Log.e(e.message, e.stackTraceToString())
        }
    }

    private fun mapQueryChangeToProject(qc: QueryChange): List<Hotel> {
        val hotels = arrayListOf<Hotel>()
        qc.results?.forEach {
            val json = it.toJSON()
            val thisHotel = Gson().fromJson(json, Hotel::class.java)
            hotels.add(thisHotel)
        }
        return hotels
    }

    fun testSearch(collection: Collection): MutableList<Hotel> {
        var whereQueryExpression = com.couchbase.lite.Function
            .lower(Expression.property("name"))
            .like(Expression.string("%" + "Sup".lowercase() + "%"))

        val stateQueryExpression = Expression.property("branch")
            .equalTo(Expression.longValue(2))  // <2>

        whereQueryExpression =
            whereQueryExpression.or(stateQueryExpression)  // <2>

        val hotels = mutableListOf<Hotel>()

         QueryBuilder
            .select(
                SelectResult.expression(Meta.id),
                SelectResult.property("name"),
                SelectResult.property("type"),
                SelectResult.property("branch")
            )
            .from(DataSource.collection(collection))
            .where(whereQueryExpression).execute().use { rs ->
                rs.forEach {
                    val json = it.toJSON()
                    val thisHotel = Gson().fromJson(json, Hotel::class.java)
                    hotels.add(thisHotel)
                }
            }

        return hotels
    }

    fun getDocumentSQL(db: Database): MutableList<Hotel> {
        var hotels = mutableListOf<Hotel>()


        viewModelScope.launch {

            db.let { database ->
                val query =
                    database.createQuery("SELECT META().id AS id, name, branch, type  FROM `test`.`bangli-hotel`  WHERE lower(name) = \"hotel bangli\" AND branch = \$branchParam")
                val parameters = Parameters() // 2
                parameters.setValue("branchParam", 2)
                query.parameters = parameters

                query.execute().use {
                    it.forEach {
                        val json = it.toJSON()
                        val thisHotel = Gson().fromJson(json, Hotel::class.java)
                        hotels.add(thisHotel)
                    }
                }
            }
        }
        return hotels
    }

    fun getDocumentSQLJoin(db: Database): MutableList<HotelJoin> {
        var hotels = mutableListOf<HotelJoin>()
        viewModelScope.launch {
            db.let { database ->
                val query =
                    database.createQuery("SELECT  doc2.name, doc2.branch, doc2.address  FROM `test`.`bangli-hotel` doc1 JOIN `test`.`bangli-address` doc2 ON doc1.name = doc2.name AND doc1.branch = doc2.branch")

                query.execute().use {
//                    Log.d(TAG, "getDocumentSQLJoin: ${it.allResults().size}")
                    it.forEach {
                        val json = it.toJSON()
                        val thisHotel = Gson().fromJson(json, HotelJoin::class.java)
                        hotels.add(thisHotel)
                    }
                }
            }
        }
        return hotels
    }


    fun getDocumentSQLAddress(db: Database): MutableList<HotelJoin> {
        var hotels = mutableListOf<HotelJoin>()
        viewModelScope.launch {
            db.let { database ->
                val query =
//                    database.createQuery("SELECT  ht.name, ht.branch, ad.address  FROM `test`.`bangli-hotel` AS ht JOIN `test`.`bangli-address` AS ad ON ht.name = ad.name AND ht.branch = ad.branch")
                    database.createQuery("SELECT  ad.name, ad.branch, ad.address  FROM `test`.`bangli-address` AS ad ")

                query.execute().use {
                    it.forEach {
                        val json = it.toJSON()
                        val thisHotel = Gson().fromJson(json, HotelJoin::class.java)
                        hotels.add(thisHotel)
                    }
                }
            }
        }
        return hotels
    }

    fun getDocumentQueryBuilderJoin(db: Database, bangli: Collection, address: Collection): MutableList<HotelJoin> {
        var hotels = mutableListOf<HotelJoin>()
        viewModelScope.launch {
            db.let { database ->
                val query =
                    QueryBuilder
                        .select(
                            SelectResult.expression(Expression.property("name").from("ht")),
                            SelectResult.expression(Expression.property("branch").from("ht")),
                            SelectResult.expression(Expression.property("address").from("ad")),
                        )
                        .from(DataSource.collection(bangli).`as`("ht"))
                        .join(
                            Join.join(DataSource.collection(address).`as`("ad"))
                                .on(
                                    Expression.property("branch").from("ht").equalTo(Expression.property("branch").from("ad"))
                                        .and(Expression.property("name").from("ht").equalTo(Expression.property("name").from("ad"))
                                        )
                                )
                        )

                query.execute().use {
//                    Log.d(TAG, "getDocumentQueryBuilderJoin: ${it.allResults().size}")
                    it.forEach {
                        val json = it.toJSON()
                        val thisHotel = Gson().fromJson(json, HotelJoin::class.java)
                        hotels.add(thisHotel)
                    }
                }
            }
        }
        return hotels
    }

    fun deleteDocument(db: Database, collection: Collection) {

        viewModelScope.launch {
            val id = mutableListOf<String>()
            db.let { database ->
//                val query =
//                    database.createQuery("SELECT META().id AS id  FROM `test`.`bangli-address` WHERE address IS NULL OR MISSING")
                val query = QueryBuilder
                    .select(SelectResult.expression(Meta.id))
                    .from(DataSource.collection(collection))
                    .where(Expression.property("address").isNotValued)


                query.execute().use {

                    it.forEach {
                        id.add(it.getString("id").orEmpty())
                    }
                }
                id.forEach {
                    collection.getDocument(it)?.let {
                        collection.delete(it)
                    }
                }
            }
        }
    }


}



data class DiceUiState(
    val numberOfRolls: Int = 0,
)