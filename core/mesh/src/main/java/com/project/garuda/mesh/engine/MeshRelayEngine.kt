package com.project.garuda.mesh.engine

import com.project.garuda.mesh.ble.BleAdvertiserManager
import com.project.garuda.mesh.ble.BleScannerManager
import com.project.garuda.mesh.protocol.GarudaPacket
import com.project.garuda.mesh.protocol.GarudaProtocolEncoderDecoder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import java.util.LinkedHashMap
import kotlin.random.Random

/**
 * Core BLE Multi-Hop Mesh Relay Engine.
 * Handles deduplication (LRU cache of size 500), TTL decrementing, HopCount incrementing,
 * randomized jitter re-broadcasting (100ms-600ms), and broadcasting to local subscribers.
 */
class MeshRelayEngine(
    private val advertiserManager: BleAdvertiserManager? = null,
    private val scannerManager: BleScannerManager? = null,
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    var localDeviceHash: Int = 0
) {

    companion object {
        const val LRU_CACHE_CAPACITY = 500
        const val JITTER_MIN_MS = 100L
        const val JITTER_MAX_MS = 600L
        const val ACTIVE_PEER_TIMEOUT_MS = 10000L // 10s sliding window for active nearby presence
    }

    // LRU Cache for packet deduplication
    private val seenPacketIds = object : LinkedHashMap<Int, Boolean>(LRU_CACHE_CAPACITY, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Int, Boolean>?): Boolean {
            return size > LRU_CACHE_CAPACITY
        }
    }

    // Active nearby peers map: uniqueDeviceHash -> lastSeenTimestampMs
    private val activePeersMap = java.util.concurrent.ConcurrentHashMap<Int, Long>()

    private val _incomingPackets = MutableSharedFlow<GarudaPacket>(extraBufferCapacity = 64)
    val incomingPackets: SharedFlow<GarudaPacket> = _incomingPackets.asSharedFlow()

    /**
     * Processes an incoming raw binary BLE packet with device hardware address.
     */
    fun processIncomingRawBytes(deviceAddress: String, rawBytes: ByteArray) {
        val packet = GarudaProtocolEncoderDecoder.decode(rawBytes) ?: return
        processIncomingPacket(packet)
    }

    /**
     * Processes an incoming raw binary BLE packet.
     */
    fun processIncomingRawBytes(rawBytes: ByteArray) {
        val packet = GarudaProtocolEncoderDecoder.decode(rawBytes) ?: return
        processIncomingPacket(packet)
    }

    /**
     * Processes a decoded [GarudaPacket].
     */
    @Synchronized
    fun processIncomingPacket(packet: GarudaPacket) {
        // Record active peer device hash timestamp (strictly ignore self-originating packets)
        if (packet.deviceHash != 0 && (localDeviceHash == 0 || packet.deviceHash != localDeviceHash)) {
            activePeersMap[packet.deviceHash] = System.currentTimeMillis()
        }

        // 1. Deduplication check using LRU Cache
        if (seenPacketIds.containsKey(packet.packetId)) {
            return // Ignore duplicate packet
        }

        // 2. Mark packet as seen
        seenPacketIds[packet.packetId] = true

        // 3. Emit packet to local application subscribers (UI, Data Sync, Chat, SOS)
        coroutineScope.launch {
            _incomingPackets.emit(packet)
        }

        // 4. Relay packet if TTL > 0 and not a simple 1-hop presence heartbeat
        if (packet.ttl > 0 && packet.packetType != GarudaPacket.TYPE_HEARTBEAT) {
            val relayedPacket = packet.copy(
                ttl = packet.ttl - 1,
                hopCount = packet.hopCount + 1
            )

            scheduleRelayBroadcast(relayedPacket)
        }
    }

    /**
     * Originates a new packet from this local node and broadcasts it into the mesh network.
     */
    fun broadcastPacket(packet: GarudaPacket) {
        synchronized(this) {
            seenPacketIds[packet.packetId] = true
        }

        val encodedBytes = GarudaProtocolEncoderDecoder.encode(packet)
        advertiserManager?.startAdvertising(encodedBytes)
    }

    /**
     * Schedules a multi-hop relay re-broadcast with randomized jitter (100ms - 600ms)
     * to avoid RF collision with nearby nodes.
     */
    private fun scheduleRelayBroadcast(packet: GarudaPacket) {
        coroutineScope.launch {
            val jitter = Random.nextLong(JITTER_MIN_MS, JITTER_MAX_MS)
            delay(jitter)

            val encodedBytes = GarudaProtocolEncoderDecoder.encode(packet)
            advertiserManager?.startAdvertising(encodedBytes)
        }
    }

    /**
     * Returns whether a packetId has already been processed by the LRU cache.
     */
    @Synchronized
    fun isPacketSeen(packetId: Int): Boolean {
        return seenPacketIds.containsKey(packetId)
    }

    /**
     * Clears the LRU deduplication cache.
     */
    @Synchronized
    fun clearCache() {
        seenPacketIds.clear()
        activePeersMap.clear()
    }

    /**
     * Returns the exact count of active unique mesh peer devices seen within [windowMs].
     */
    @Synchronized
    fun getActivePeerCount(windowMs: Long = ACTIVE_PEER_TIMEOUT_MS): Int {
        val now = System.currentTimeMillis()
        activePeersMap.entries.removeIf { now - it.value > windowMs }
        return activePeersMap.size
    }
}
