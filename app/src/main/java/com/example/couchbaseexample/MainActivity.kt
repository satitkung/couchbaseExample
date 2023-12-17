package com.example.couchbaseexample

import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.couchbase.lite.Database
import com.example.couchbaseexample.ui.theme.CouchBaseExampleTheme
import com.google.gson.Gson
import kotlinx.coroutines.launch

const val TAG = "tees"
const val SCOPE_TEST = "test"
const val SCOPE_PROD = "prod"
const val BANGLI_COLLECTION = "bangli-hotel"
const val BANGLI_ADDRESS_COLLECTION = "bangli-address"
const val AIYALA_COLLECTION = "aiyala-hotel"

class MainActivity : ComponentActivity() {

    lateinit var database: Database
    val viewModel: MainViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            CouchBaseExampleTheme {
                // A surface container using the 'background' color from the theme
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = MaterialTheme.colorScheme.background
                ) {
                    Greeting("Android")

                    val hotelList = viewModel.countDocumentFlow().collectAsState(initial = listOf())
                    hotelList.value.forEach {
                        Log.d(TAG, "name: ${it.name}, branh: ${it.branch}, id: ${it.id}")
                    }

                    Column {
                        FilledButtonExample(
                            onClick = {
//                                query(BANGLI_COLLECTION).forEach {
//                                    Log.d(TAG, "name: ${it.name}, branh: ${it.branch}, id: ${it.id}")
//                                }
                                database.getScope(SCOPE_TEST)?.getCollection(BANGLI_ADDRESS_COLLECTION)?.let {

//                                    Log.d(TAG, "countDocument: ${viewModel.countDocument(it)}")
//                                    viewModel.getDocumentSQL(database).forEach {
//                                        Log.d(TAG, "name: ${it.name}, branh: ${it.branch}, id: ${it.id}")
//                                    }
                                    viewModel.getDocumentSQLAddress(database).forEach {
                                        Log.d(TAG, "getDocumentSQLAddress name: ${it.name}, branh: ${it.branch}, address: ${it.address}")
                                    }
//                                    viewModel.getDocumentSQLJoin(database).forEach {
//                                        Log.d(TAG, "getDocumentSQLJoin name: ${it.name}, branh: ${it.branch}, address: ${it.address}")
//                                    }
//                                    viewModel.testSearch(it).forEach {
//                                        Log.d(TAG, " testSearch name: ${it.name}, branh: ${it.branch}, id: ${it.id}")
//                                    }

//                                    DBManager.updateDoc("047da48c-53f6-41e4-9ace-ecfdd12cc92f", it)
//                                    viewModel.createTeamTypeIndex(database)

//                                    database.getScope(SCOPE_TEST)?.getCollection(BANGLI_COLLECTION)?.let { bangli ->
//                                        database.getScope(SCOPE_TEST)?.getCollection(BANGLI_ADDRESS_COLLECTION)?.let { address ->
//                                            val result = viewModel.getDocumentQueryBuilderJoin(database, bangli, address)
//                                            result.forEach {
//                                                Log.d(TAG, " testSearch name: ${it.name}, branh: ${it.branch}, address: ${it.address}")
//                                            }
//
//                                        }
//                                    }
                                }

                            },
                            text = "Query Bangli hotel branch"
                        )

                        Spacer(modifier = Modifier.height(24.dp))

                        FilledButtonExample(
                            onClick = {
                                query(AIYALA_COLLECTION).forEach {
                                    Log.d(TAG, "name: ${it.name}, branh: ${it.branch}")
                                }
                            },
                            text = " Query Aiyala hotel branch"
                        )

                        Spacer(modifier = Modifier.height(200.dp))

                        FilledButtonExample(
                            onClick = {
                                database.getScope(SCOPE_TEST)?.getCollection(BANGLI_ADDRESS_COLLECTION)?.let {
//                                    insertBangli(it)
//                                    insertBangliAddress(it)
//                                    DBManager.updateAllDoc(database, it)
                                    viewModel.deleteDocument(database, it)
                                }
                            },
                            text = "insert Bangli hotel branch"
                        )

                        Spacer(modifier = Modifier.height(24.dp))

                        FilledButtonExample(
                            onClick = {
                                database.getScope(SCOPE_TEST)?.getCollection(AIYALA_COLLECTION)?.let {
                                    insertAiyala(it)
                                }
                            },
                            text = " insert Aiyala hotel branch"
                        )

                    }
                }
            }
        }

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                viewModel.uiState.collect {
                    Log.d(TAG,  " uiState countDocument: ${it.numberOfRolls}")

                }
            }
        }

        DBManager.init(this)
        database = DBManager.createDb(context = this)
        viewModel.database = database
        DBManager.createCollection(SCOPE_TEST, BANGLI_COLLECTION)
        DBManager.createCollection(SCOPE_TEST, AIYALA_COLLECTION)
        DBManager.createCollection(SCOPE_TEST, BANGLI_ADDRESS_COLLECTION)

        DBManager.orintAllScope()

//        val thisQuery = database.createQuery(
//            "SELECT META().id AS id FROM _ WHERE type = \"hotel\""
//        )
//
//        thisQuery.execute().use { rs ->
//            rs.allResults()
//            Log.d(TAG, "query all type hotel:  ${rs.allResults()}")
//        }

//        val query = QueryBuilder
//                .select(
//            SelectResult.expression(Meta.id),
//            SelectResult.property("name"),
//            SelectResult.property("type")
//        )
//            .from(DataSource.collection(aiyalaCollection))
//            .where(Expression.property("type").equalTo(Expression.string("hotel")))
//            .orderBy(Ordering.expression(Meta.id))
//
//        query.execute().use { rs ->
//            Log.d(TAG, "result size ${rs.count()}")
//
//            rs.forEach {
//                Log.d(TAG, "hotel name: ${it.getString("name")}, hotel branch: ${it.getLong("room_rate")}")
//            }
//        }
    }

    private fun query(collection: String): List<Hotel> {
        val hotels = mutableListOf<Hotel>()
        database.getScope(SCOPE_TEST)?.getCollection(collection)?.let {
            DBManager.getAllDoc(it).execute().use { rs ->
                rs.forEach {
                    val json = it.toJSON()
                    val thisHotel = Gson().fromJson(json, Hotel::class.java)
                    hotels.add(thisHotel)
                }
            }
        }
        return hotels
    }


    private fun insertBangli(collection: com.couchbase.lite.Collection) {
        for (i in 1..5) {
            collection.save(DBManager.createBangliDoc(i.toLong()))
        }
    }

    private fun insertBangliAddress(collection: com.couchbase.lite.Collection) {
        for (i in 1..5) {
            collection.save(DBManager.createBangliAddressDoc(i.toLong()))
        }
    }

    private fun insertAiyala(collection: com.couchbase.lite.Collection) {
        for (i in 1..3) {
            collection.save(DBManager.createAiyalaDoc(i.toLong()))
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        database.close()
    }
}


@Composable
fun Greeting(name: String, modifier: Modifier = Modifier) {
    Text(
        text = "Hello $name!",
        modifier = modifier
    )
}

@Composable
fun FilledButtonExample(onClick: () -> Unit, text: String) {
    Button(onClick = { onClick() }) {
        Text(text)
    }
}


@Preview(showBackground = true)
@Composable
fun GreetingPreview() {
    CouchBaseExampleTheme {
        Greeting("Android")
    }
}


data class Hotel(
    val type: String = "",
    val name: String = "",
    val id: String = "",
    val branch: Long = 0
)

data class HotelJoin(
    val name: String,
    val branch: Long,
    val address: Address
)

data class Address(
    val street: String,
    val city: String,
    val state: String,
    val country: String,
    val code: String
)