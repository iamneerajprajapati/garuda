package com.project.garuda.network

import android.content.Context
import android.os.Build
import android.util.Log
import com.project.garuda.hardware.DeviceHardwareManager
import com.project.garuda.mesh.protocol.GarudaPacket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL

data class FirebaseSyncState(
    val isConnected: Boolean = false,
    val projectId: String = "garuda-2aba2",
    val lastSyncTimestamp: Long = 0,
    val syncedPacketsCount: Int = 0,
    val isEmergencyActive: Boolean = false,
    val alertHeadline: String = "Standby Mode",
    val alertInstructions: String = "System ready for government alert.",
    val alertDistrict: String = "All Regions"
)

class FirebaseCloudGateway(
    private val scope: CoroutineScope,
    private val context: Context? = null
) {
    companion object {
        private const val TAG = "GarudaFirebase"
        private val API_KEY: String by lazy {
            try {
                String(android.util.Base64.decode("QUl6YVN5QmtSSkhEVE1KUU16MUFrZHhqeHNGcl9Vd3c3VndGTnNZ", android.util.Base64.DEFAULT))
            } catch (e: Exception) {
                ""
            }
        }
    }

    private var projectId = "garuda-2aba2"
    private val _syncState = MutableStateFlow(FirebaseSyncState(projectId = projectId))
    val syncState: StateFlow<FirebaseSyncState> = _syncState.asStateFlow()

    private var pollJob: Job? = null
    val hardwareManager = context?.let { DeviceHardwareManager(it) }

    val deviceId: String = {
        val model = (Build.MODEL ?: "GalaxyDevice").replace(" ", "_")
        val manufacturer = (Build.MANUFACTURER ?: "Samsung").replace(" ", "_")
        "${manufacturer}_${model}_NODE"
    }()

    init {
        startCloudListener()
    }

    fun updateProjectId(newProjectId: String) {
        this.projectId = newProjectId
        _syncState.value = _syncState.value.copy(projectId = newProjectId)
        startCloudListener()
    }

    fun startCloudListener() {
        pollJob?.cancel()
        pollJob = scope.launch(Dispatchers.IO) {
            while (isActive) {
                fetchGovernmentAlertStatus()
                sendDeviceHeartbeat()
                delay(4000) // Poll & Heartbeat every 4 seconds
            }
        }
    }

    private suspend fun sendDeviceHeartbeat() {
        try {
            val urlString = "https://firestore.googleapis.com/v1/projects/$projectId/databases/(default)/documents/active_nodes/$deviceId?key=$API_KEY"
            val url = URL(urlString)
            val connection = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = 3000
                readTimeout = 3000
                requestMethod = "PATCH"
                setRequestProperty("Content-Type", "application/json")
                doOutput = true
            }

            // Real Hardware Values
            val realBattery = hardwareManager?.getRealBatteryPercentage() ?: 80
            val locationData = hardwareManager?.locationFlow?.value
            val realLat = locationData?.latitude ?: 0.0
            val realLon = locationData?.longitude ?: 0.0
            val realLocationName = locationData?.locationName ?: "GPS Locating..."

            val deviceName = (Build.MANUFACTURER ?: "Samsung") + " " + (Build.MODEL ?: "Galaxy Device")

            val fields = JSONObject().apply {
                put("deviceId", JSONObject().put("stringValue", deviceId))
                put("deviceName", JSONObject().put("stringValue", deviceName))
                put("status", JSONObject().put("stringValue", "ONLINE"))
                put("batteryLevel", JSONObject().put("integerValue", "$realBattery"))
                put("meshRole", JSONObject().put("stringValue", "Relay Gateway Node"))
                put("lastSeen", JSONObject().put("integerValue", "${System.currentTimeMillis() / 1000}"))
                put("location", JSONObject().put("stringValue", realLocationName))
                put("latitude", JSONObject().put("doubleValue", realLat))
                put("longitude", JSONObject().put("doubleValue", realLon))
                put("connectionType", JSONObject().put("stringValue", "CLOUD_DIRECT"))
                put("isDirectCloud", JSONObject().put("booleanValue", true))
                put("hopCount", JSONObject().put("integerValue", "0"))
            }

            val body = JSONObject().put("fields", fields)
            val writer = OutputStreamWriter(connection.outputStream)
            writer.write(body.toString())
            writer.flush()
            writer.close()

            connection.responseCode // execute request
        } catch (e: Exception) {
            Log.v(TAG, "Heartbeat note: ${e.message}")
        }
    }

    private var lastNotifiedAlertTimestamp: Long = 0L

    private suspend fun fetchGovernmentAlertStatus() {
        try {
            val urlString = "https://firestore.googleapis.com/v1/projects/$projectId/databases/(default)/documents/alerts/current_status?key=$API_KEY"
            val url = URL(urlString)
            val connection = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = 3000
                readTimeout = 3000
                requestMethod = "GET"
            }

            if (connection.responseCode == 200) {
                val reader = BufferedReader(InputStreamReader(connection.inputStream))
                val response = reader.readText()
                reader.close()

                val json = JSONObject(response)
                val fields = json.optJSONObject("fields")

                if (fields != null) {
                    val title = fields.optJSONObject("title")?.optString("stringValue") ?: "Government Alert"
                    val severity = fields.optJSONObject("severity")?.optString("stringValue") ?: "Critical"
                    val district = fields.optJSONObject("targetDistrict")?.optString("stringValue") ?: "Disaster Zone"
                    val instructions = fields.optJSONObject("instructions")?.optString("stringValue") ?: "Evacuate"
                    val isEmergency = fields.optJSONObject("isEmergencyActive")?.optBoolean("booleanValue") ?: false
                    val alertTimestamp = fields.optJSONObject("timestamp")?.optString("integerValue")?.toLongOrNull() ?: 0L

                    val isNewEmergencyAlert = isEmergency && (alertTimestamp > lastNotifiedAlertTimestamp)

                    _syncState.value = _syncState.value.copy(
                        isConnected = true,
                        lastSyncTimestamp = System.currentTimeMillis(),
                        isEmergencyActive = isEmergency,
                        alertHeadline = if (isEmergency) title else "",
                        alertDistrict = if (isEmergency) district else "Standby",
                        alertInstructions = if (isEmergency) "$severity: $instructions" else ""
                    )

                    if (isNewEmergencyAlert) {
                        lastNotifiedAlertTimestamp = alertTimestamp
                        if (context != null) {
                            com.project.garuda.notification.GarudaNotificationManager.showHeadsUpNotification(
                                context = context,
                                title = title,
                                message = "$severity: $instructions",
                                targetArea = district,
                                isEmergency = true
                            )
                        }
                    } else if (!isEmergency && context != null) {
                        com.project.garuda.notification.GarudaNotificationManager.dismissEmergencyNotification(context)
                    }
                }
            } else if (connection.responseCode == 404) {
                _syncState.value = _syncState.value.copy(
                    isConnected = true,
                    lastSyncTimestamp = System.currentTimeMillis()
                )
            }
        } catch (e: Exception) {
            Log.v(TAG, "Cloud poll note: ${e.message}")
        }
    }

    suspend fun uploadSosToFirestore(
        packet: GarudaPacket,
        victimName: String = "Citizen",
        notes: String = "Relayed via Ground Mesh to Firebase Firestore"
    ): Boolean = withContext(Dispatchers.IO) {
        try {
            val docId = "SOS-${packet.packetId}"
            val urlString = "https://firestore.googleapis.com/v1/projects/$projectId/databases/(default)/documents/disaster_sos?documentId=$docId&key=$API_KEY"
            val url = URL(urlString)

            val connection = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = 4000
                readTimeout = 4000
                requestMethod = "POST"
                setRequestProperty("Content-Type", "application/json")
                doOutput = true
            }

            val realBattery = hardwareManager?.getRealBatteryPercentage() ?: 80

            val fields = JSONObject().apply {
                put("victimName", JSONObject().put("stringValue", victimName))
                put("bloodGroup", JSONObject().put("stringValue", "O+"))
                put("latitude", JSONObject().put("doubleValue", packet.latitude))
                put("longitude", JSONObject().put("doubleValue", packet.longitude))
                put("hopCount", JSONObject().put("integerValue", "${packet.hopCount}"))
                put("batteryLevel", JSONObject().put("integerValue", "$realBattery"))
                put("relayedByGatewayId", JSONObject().put("stringValue", deviceId))
                put("notes", JSONObject().put("stringValue", notes))
                put("priority", JSONObject().put("stringValue", "CRITICAL (Red)"))
                put("status", JSONObject().put("stringValue", "Pending Triage"))
                put("timestamp", JSONObject().put("integerValue", "${System.currentTimeMillis() / 1000}"))
            }

            val body = JSONObject().put("fields", fields)

            val writer = OutputStreamWriter(connection.outputStream)
            writer.write(body.toString())
            writer.flush()
            writer.close()

            val code = connection.responseCode
            val success = code in 200..299 || code == 409

            if (success) {
                _syncState.value = _syncState.value.copy(
                    syncedPacketsCount = _syncState.value.syncedPacketsCount + 1,
                    lastSyncTimestamp = System.currentTimeMillis()
                )
            }
            return@withContext success
        } catch (e: Exception) {
            Log.e(TAG, "Error uploading to Firestore: ${e.message}")
            return@withContext false
        }
    }
}
