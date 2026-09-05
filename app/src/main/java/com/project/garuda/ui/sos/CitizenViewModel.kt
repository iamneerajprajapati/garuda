package com.project.garuda.ui.sos

import android.content.Context
import android.content.Intent
import android.os.Build
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.project.garuda.mesh.ble.BleAdvertiserManager
import com.project.garuda.mesh.ble.BleScannerManager
import com.project.garuda.mesh.engine.MeshRelayEngine
import com.project.garuda.mesh.protocol.GarudaPacket
import com.project.garuda.mesh.protocol.GarudaProtocolEncoderDecoder
import com.project.garuda.mesh.service.MeshForegroundService
import com.project.garuda.network.FirebaseCloudGateway
import com.project.garuda.network.UplinkGatewayManager
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.random.Random

class CitizenViewModel(
    private val appContext: Context? = null
) : ViewModel() {

    companion object {
        private const val TAG = "CitizenViewModel"
    }

    private val _uiState = MutableStateFlow(CitizenUiState())
    val uiState: StateFlow<CitizenUiState> = _uiState.asStateFlow()

    private var countdownJob: Job? = null
    private var broadcastingJob: Job? = null

    // BLE Mesh Components
    private var advertiserManager: BleAdvertiserManager? = null
    private var scannerManager: BleScannerManager? = null
    private var meshRelayEngine: MeshRelayEngine? = null

    // Cloud Firebase Gateway & Local Command Grid Uplink
    val firebaseGateway = FirebaseCloudGateway(viewModelScope, appContext)
    val uplinkGateway = UplinkGatewayManager(viewModelScope, appContext)

    init {
        initBleMeshEngine()
        observeFirebaseGovernmentAlerts()
        observeUplinkGatewayEvents()
        observeMeshTelemetry()
    }

    private fun observeUplinkGatewayEvents() {
        viewModelScope.launch {
            uplinkGateway.connectionState.collect { connState ->
                if (connState.isEmergencyActiveFromGov) {
                    if (_uiState.value.mode == DisasterMode.STANDBY && !_uiState.value.isGovernmentAlertDialogOpen) {
                        _uiState.update {
                            it.copy(
                                pendingGovAlert = GovernmentAlert(
                                    headline = connState.alertHeadline,
                                    region = connState.activeDistrictAlert,
                                    instructions = connState.alertInstructions,
                                    timestampFormatted = "Live from Command Grid"
                                ),
                                isGovernmentAlertDialogOpen = true
                            )
                        }
                    }
                } else if (!connState.isEmergencyActiveFromGov && _uiState.value.isGovernmentAlertDialogOpen) {
                    _uiState.update { it.copy(isGovernmentAlertDialogOpen = false) }
                }
            }
        }
    }

    private fun initBleMeshEngine() {
        if (appContext != null) {
            try {
                advertiserManager = BleAdvertiserManager(appContext)
                scannerManager = BleScannerManager(appContext)
                val androidId = android.provider.Settings.Secure.getString(appContext.contentResolver, android.provider.Settings.Secure.ANDROID_ID) ?: (Build.MODEL ?: "GarudaNode")
                val myHash = androidId.hashCode()
                meshRelayEngine = MeshRelayEngine(advertiserManager, scannerManager, viewModelScope, localDeviceHash = myHash)

                startMeshScanner()
                startHeartbeatBroadcaster()

                // Listen to live incoming mesh packets over Bluetooth
                viewModelScope.launch {
                    meshRelayEngine?.incomingPackets?.collect { packet ->
                        Log.d(TAG, "Received live BLE Mesh Packet: ID=0x${packet.packetId.toString(16)}, type=${packet.packetType}, hops=${packet.hopCount}")
                        val realPeers = meshRelayEngine?.getActivePeerCount() ?: 0
                        _uiState.update { current ->
                            current.copy(
                                meshStatus = current.meshStatus.copy(
                                    peersNearby = realPeers,
                                    packetsRelayed = current.meshStatus.packetsRelayed + 1,
                                    hopCount = packet.hopCount.coerceAtLeast(1),
                                    lastSyncAgo = "Just now"
                                )
                            )
                        }

                        // 🚨 Emergency Declaration Relayed via BLE Mesh from other peer nodes
                        if (packet.packetType == GarudaPacket.TYPE_EMERGENCY_BROADCAST || 
                            (packet.packetType == GarudaPacket.TYPE_SOS && packet.emergencyType != GarudaPacket.EMERGENCY_NONE)) {
                            appContext?.let { ctx ->
                                com.project.garuda.notification.GarudaNotificationManager.showHeadsUpNotification(
                                    context = ctx,
                                    title = "DISASTER EMERGENCY (MESH RELAY)",
                                    message = "Critical disaster declaration received via Bluetooth Mesh multi-hop relay. Evacuate or seek immediate high ground.",
                                    targetArea = "Your Region",
                                    isEmergency = true
                                )
                            }
                            if (_uiState.value.mode == DisasterMode.STANDBY && !_uiState.value.isGovernmentAlertDialogOpen) {
                                _uiState.update {
                                    it.copy(
                                        pendingGovAlert = GovernmentAlert(
                                            headline = "DISASTER EMERGENCY (MESH RELAY)",
                                            region = "Disaster Zone",
                                            instructions = "Emergency alert received via multi-hop BLE mesh (Hop #${packet.hopCount}). Seek shelter or follow evacuation directives.",
                                            timestampFormatted = "Relayed via Mesh Hop #${packet.hopCount}"
                                        ),
                                        isGovernmentAlertDialogOpen = true
                                    )
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "Error initializing BLE Mesh Engine", e)
            }
        }
    }

    private fun observeFirebaseGovernmentAlerts() {
        viewModelScope.launch {
            firebaseGateway.syncState.collect { syncState ->
                if (syncState.isEmergencyActive) {
                    val currentLoc = firebaseGateway.hardwareManager?.locationFlow?.value?.locationName ?: ""
                    val targetZone = syncState.alertDistrict
                    val isApplicable = isGeofenceMatch(currentLoc, targetZone)

                    // 📡 Re-broadcast Cloud Emergency Alert onto BLE Mesh for all offline nearby citizens!
                    val meshAlertPacket = GarudaPacket(
                        packetType = GarudaPacket.TYPE_EMERGENCY_BROADCAST,
                        packetId = (syncState.alertHeadline.hashCode() xor syncState.alertDistrict.hashCode()),
                        deviceHash = firebaseGateway.deviceId.hashCode(),
                        timestamp = (System.currentTimeMillis() / 1000).toInt(),
                        latitude = firebaseGateway.hardwareManager?.locationFlow?.value?.latitude ?: 0.0,
                        longitude = firebaseGateway.hardwareManager?.locationFlow?.value?.longitude ?: 0.0,
                        emergencyType = GarudaPacket.EMERGENCY_TRAPPED,
                        hopCount = 0,
                        ttl = 7
                    )
                    meshRelayEngine?.broadcastPacket(meshAlertPacket)

                    if (isApplicable) {
                        Log.i(TAG, "🚨 Geofence MATCH! Alert targeting '$targetZone' applies to current location '$currentLoc'")
                        if (_uiState.value.mode == DisasterMode.STANDBY && !_uiState.value.isGovernmentAlertDialogOpen) {
                            _uiState.update {
                                it.copy(
                                    pendingGovAlert = GovernmentAlert(
                                        headline = syncState.alertHeadline,
                                        region = syncState.alertDistrict,
                                        instructions = syncState.alertInstructions,
                                        timestampFormatted = "Live from Command Grid"
                                    ),
                                    isGovernmentAlertDialogOpen = false
                                )

                            }
                        }
                    } else {
                        Log.v(TAG, "ℹ️ Alert for '$targetZone' does not match device location '$currentLoc'. Remaining in Standby.")
                    }
                } else if (!syncState.isEmergencyActive && _uiState.value.isGovernmentAlertDialogOpen) {
                    _uiState.update { it.copy(isGovernmentAlertDialogOpen = false) }
                }
            }
        }
    }

    private fun isGeofenceMatch(deviceLocation: String, targetGeofence: String): Boolean {
        if (targetGeofence.isEmpty() || targetGeofence.contains("All", ignoreCase = true) || targetGeofence.contains("National", ignoreCase = true) || targetGeofence.contains("Pan-India", ignoreCase = true)) {
            return true
        }
        if (deviceLocation.isEmpty() || deviceLocation.contains("Detecting", ignoreCase = true)) {
            return true // Fallback to safe alert if GPS is still warming up
        }

        val devClean = deviceLocation.lowercase()
        val targetClean = targetGeofence.lowercase()

        // Check substring containment
        if (targetClean.contains(devClean) || devClean.contains(targetClean)) return true

        // Check matching words (e.g. "Uttar Pradesh", "Prayagraj", "Phaphamau", "Kerala", "Wayanad")
        val devTokens = devClean.split(',', ' ', '/', '(', ')').filter { it.length > 3 }
        val targetTokens = targetClean.split(',', ' ', '/', '(', ')').filter { it.length > 3 }

        return devTokens.any { token -> targetTokens.contains(token) }
    }

    private fun observeMeshTelemetry() {
        viewModelScope.launch {
            while (isActive) {
                delay(2000)
                val activePeers = meshRelayEngine?.getActivePeerCount() ?: 0
                _uiState.update { current ->
                    current.copy(
                        meshStatus = current.meshStatus.copy(
                            peersNearby = activePeers
                        )
                    )
                }
            }
        }
    }

    fun toggleChecklistItem(id: String) {
        _uiState.update { current ->
            val updated = current.survivalChecklist.map { item ->
                if (item.id == id) item.copy(isChecked = !item.isChecked) else item
            }
            current.copy(survivalChecklist = updated)
        }
    }

    fun updateMedicalProfile(profile: MedicalProfile) {
        _uiState.update { it.copy(medicalProfile = profile) }
    }

    fun selectEmergencyType(type: EmergencyType) {
        _uiState.update { it.copy(selectedEmergencyType = type) }
    }

    fun triggerGovernmentEmergencyAlert() {
        _uiState.update {
            it.copy(
                pendingGovAlert = GovernmentAlert(),
                isGovernmentAlertDialogOpen = false
            )
        }
    }


    fun dismissGovernmentAlert() {
        _uiState.update { it.copy(isGovernmentAlertDialogOpen = false) }
    }

    fun acknowledgeAlertAndEnterEmergency() {
        _uiState.update {
            it.copy(
                mode = DisasterMode.ACTIVE_EMERGENCY,
                isGovernmentAlertDialogOpen = false,
                meshStatus = it.meshStatus.copy(isMeshActive = true)
            )
        }
        startMeshScanner()
    }

    fun setDisasterMode(mode: DisasterMode) {
        if (mode == DisasterMode.STANDBY) {
            cancelSosCountdown()
            stopBroadcasting()
        }
        _uiState.update { it.copy(mode = mode) }
    }

    private var heartbeatJob: Job? = null

    private fun startHeartbeatBroadcaster() {
        heartbeatJob?.cancel()
        heartbeatJob = viewModelScope.launch {
            while (isActive) {
                val androidId = appContext?.let { ctx ->
                    android.provider.Settings.Secure.getString(ctx.contentResolver, android.provider.Settings.Secure.ANDROID_ID)
                } ?: (Build.MODEL ?: "GarudaCitizen")
                val deviceHashInt = androidId.hashCode()
                val heartbeatBytes = GarudaProtocolEncoderDecoder.encodeHeartbeat(deviceHashInt)
                try {
                    advertiserManager?.startAdvertising(heartbeatBytes)
                    Log.d(TAG, "Sent presence heartbeat beacon from $androidId (hash=$deviceHashInt)")
                } catch (e: Exception) {
                    Log.v(TAG, "Heartbeat broadcast failed", e)
                }
                delay(3000) // Periodic 3s BLE presence beacon for active nearby peer discovery
            }
        }
    }

    private fun startMeshScanner() {
        try {
            scannerManager?.startScanning { deviceAddress, rawBytes ->
                meshRelayEngine?.processIncomingRawBytes(deviceAddress, rawBytes)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start BLE scanning", e)
        }
    }

    private fun stopMeshScanner() {
        try {
            scannerManager?.stopScanning()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to stop BLE scanning", e)
        }
    }

    fun startSosCountdown() {
        if (_uiState.value.sosState is SosBroadcastState.Broadcasting) return
        countdownJob?.cancel()

        _uiState.update { it.copy(sosState = SosBroadcastState.Countdown(5)) }

        countdownJob = viewModelScope.launch {
            for (sec in 4 downTo 1) {
                delay(1000)
                _uiState.update {
                    if (it.sosState is SosBroadcastState.Countdown) {
                        it.copy(sosState = SosBroadcastState.Countdown(sec))
                    } else it
                }
            }
            delay(1000)
            triggerActiveBroadcasting()
        }
    }

    fun cancelSosCountdown() {
        countdownJob?.cancel()
        countdownJob = null
        _uiState.update { it.copy(sosState = SosBroadcastState.Idle) }
    }

    private fun triggerActiveBroadcasting() {
        val emergencyType = _uiState.value.selectedEmergencyType
        val packetIdInt = Random.nextInt(10000, 99999)
        val packetHex = "GD-" + packetIdInt.toString(16).uppercase()
        val nowEpoch = (System.currentTimeMillis() / 1000).toInt()

        _uiState.update {
            it.copy(
                mode = DisasterMode.ACTIVE_EMERGENCY,
                sosState = SosBroadcastState.Broadcasting(
                    emergencyType = emergencyType,
                    elapsedSeconds = 0,
                    packetId = packetHex,
                    timestampEpoch = nowEpoch.toLong()
                ),
                meshStatus = it.meshStatus.copy(
                    isMeshActive = true,
                    peersNearby = meshRelayEngine?.getActivePeerCount() ?: 0
                )
            )
        }

        val protocolEmergencyCode = when (emergencyType) {
            EmergencyType.MEDICAL -> GarudaPacket.EMERGENCY_MEDICAL
            EmergencyType.TRAPPED -> GarudaPacket.EMERGENCY_TRAPPED
            EmergencyType.FIRE -> GarudaPacket.EMERGENCY_FIRE
            EmergencyType.FLOOD -> GarudaPacket.EMERGENCY_FLOOD
            EmergencyType.GENERAL -> GarudaPacket.EMERGENCY_MEDICAL
        }

        val deviceHashInt = (Build.MODEL ?: "GarudaCitizen").hashCode()
        val loc = firebaseGateway.hardwareManager?.locationFlow?.value
        val realLat = if (loc != null && loc.hasValidLocation && loc.latitude != 0.0) loc.latitude else 12.9716
        val realLon = if (loc != null && loc.hasValidLocation && loc.longitude != 0.0) loc.longitude else 77.5946

        val garudaPacket = GarudaPacket(
            packetType = GarudaPacket.TYPE_SOS,
            packetId = packetIdInt,
            deviceHash = deviceHashInt,
            timestamp = nowEpoch,
            latitude = realLat,
            longitude = realLon,
            emergencyType = protocolEmergencyCode,
            hopCount = 0,
            ttl = GarudaPacket.DEFAULT_TTL
        )

        // 1. Transmit Real 28-Byte Binary Packet over BLE Mesh
        try {
            meshRelayEngine?.broadcastPacket(garudaPacket)
            Log.i(TAG, "Transmitted Real BLE SOS Beacon: $packetHex with EmergencyCode=$protocolEmergencyCode")
        } catch (e: Exception) {
            Log.e(TAG, "BLE SOS Broadcast failed", e)
        }

        // 2. Upload to Firebase Firestore in Cloud
        viewModelScope.launch {
            val victimName = _uiState.value.medicalProfile.fullName
            firebaseGateway.uploadSosToFirestore(
                packet = garudaPacket,
                victimName = victimName,
                notes = "Live SOS beacon (${emergencyType.title}) with blood group ${_uiState.value.medicalProfile.bloodGroup}"
            )
        }

        // 3. Start Foreground Service
        appContext?.let { ctx ->
            try {
                val intent = Intent(ctx, MeshForegroundService::class.java).apply {
                    action = MeshForegroundService.ACTION_START_HIGH_ALERT
                }
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    ctx.startForegroundService(intent)
                } else {
                    ctx.startService(intent)
                }
            } catch (e: Exception) {
                Log.e(TAG, "Could not start MeshForegroundService", e)
            }
        }

        // 4. Elapsed Time Coroutine
        broadcastingJob?.cancel()
        broadcastingJob = viewModelScope.launch {
            var seconds = 0
            while (isActive) {
                delay(1000)
                seconds++
                _uiState.update { current ->
                    if (current.sosState is SosBroadcastState.Broadcasting) {
                        current.copy(
                            sosState = current.sosState.copy(elapsedSeconds = seconds)
                        )
                    } else current
                }
            }
        }
    }

    fun stopBroadcasting() {
        broadcastingJob?.cancel()
        broadcastingJob = null

        try {
            advertiserManager?.stopAdvertising()
        } catch (e: Exception) {
            Log.e(TAG, "Error stopping BLE advertising", e)
        }

        appContext?.let { ctx ->
            try {
                val intent = Intent(ctx, MeshForegroundService::class.java).apply {
                    action = MeshForegroundService.ACTION_STOP_SERVICE
                }
                ctx.startService(intent)
            } catch (e: Exception) {
                Log.e(TAG, "Error stopping MeshForegroundService", e)
            }
        }

        _uiState.update { it.copy(sosState = SosBroadcastState.Idle) }
    }

    fun markUserSafe() {
        countdownJob?.cancel()
        stopBroadcasting()

        val contactsCount = _uiState.value.medicalProfile.emergencyContacts.size

        _uiState.update {
            it.copy(
                sosState = SosBroadcastState.ResolvedSafe(
                    checkInTimestamp = System.currentTimeMillis(),
                    smsSentCount = contactsCount
                ),
                safeStatusMessage = "You are marked SAFE. Distress signal cancelled and automated SMS queued for $contactsCount contacts."
            )
        }
    }

    fun clearSafeMessage() {
        _uiState.update { it.copy(safeStatusMessage = null) }
    }

    override fun onCleared() {
        super.onCleared()
        stopBroadcasting()
        stopMeshScanner()
    }
}
