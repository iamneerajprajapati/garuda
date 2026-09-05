package com.project.garuda.mesh.protocol

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Encoder and Decoder for Garuda Compact Binary Protocol.
 * Converts [GarudaPacket] objects to/from byte arrays for BLE advertising & extended payload transmission.
 */
object GarudaProtocolEncoderDecoder {

    private const val FIXED_POINT_SCALE = 1e7

    /**
     * Encodes a [GarudaPacket] into a raw byte array.
     * Calculates and embeds CRC16 checksum at the end of the packet.
     */
    fun encode(packet: GarudaPacket): ByteArray {
        val payloadSize = packet.payload.size
        val buffer = ByteBuffer.allocate(GarudaPacket.LEGACY_FRAME_SIZE + payloadSize)
            .order(ByteOrder.BIG_ENDIAN)

        // 1. Header (2 bytes)
        buffer.put(GarudaPacket.MAGIC_BYTE_1)
        buffer.put(GarudaPacket.MAGIC_BYTE_2)

        // 2. PacketType (1 byte)
        buffer.put(packet.packetType)

        // 3. PacketId (4 bytes)
        buffer.putInt(packet.packetId)

        // 4. DeviceHash (4 bytes)
        buffer.putInt(packet.deviceHash)

        // 5. Timestamp (4 bytes)
        buffer.putInt(packet.timestamp)

        // 6. Latitude fixed point (4 bytes)
        buffer.putInt((packet.latitude * FIXED_POINT_SCALE).toInt())

        // 7. Longitude fixed point (4 bytes)
        buffer.putInt((packet.longitude * FIXED_POINT_SCALE).toInt())

        // 8. EmergencyType (1 byte)
        buffer.put(packet.emergencyType)

        // 9. HopCount & TTL combined into 1 byte (lower 4 bits: HopCount, upper 4 bits: TTL)
        val hopAndTtl = ((packet.hopCount and 0x0F) or ((packet.ttl and 0x0F) shl 4)).toByte()
        buffer.put(hopAndTtl)

        // 10. Extended Payload (if any)
        if (payloadSize > 0) {
            buffer.put(packet.payload)
        }

        // 11. Calculate CRC16 over all bytes packed so far
        val dataToChecksum = buffer.array().copyOf(buffer.position())
        val checksumShort = calculateCrc16(dataToChecksum)

        // Put CRC16 Checksum (2 bytes)
        buffer.putShort(checksumShort)

        return buffer.array()
    }

    /**
     * Encodes a lightweight [GarudaPacket.TYPE_HEARTBEAT] presence beacon (11 bytes)
     * fitting comfortably within legacy BLE 31-byte advertisement frames.
     */
    fun encodeHeartbeat(deviceHash: Int): ByteArray {
        val buffer = ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN)
        buffer.put(GarudaPacket.MAGIC_BYTE_1)
        buffer.put(GarudaPacket.MAGIC_BYTE_2)
        buffer.put(GarudaPacket.TYPE_HEARTBEAT)
        buffer.putInt(deviceHash)
        buffer.putInt((System.currentTimeMillis() / 1000).toInt())
        return buffer.array()
    }

    /**
     * Decodes a raw byte array into a [GarudaPacket].
     * Returns null if magic bytes don't match or CRC16 checksum validation fails.
     */
    fun decode(bytes: ByteArray): GarudaPacket? {
        if (bytes.size < 11) {
            return null
        }

        // Compact Heartbeat Beacon (11 bytes)
        if (bytes.size < GarudaPacket.LEGACY_FRAME_SIZE) {
            if (bytes[0] == GarudaPacket.MAGIC_BYTE_1 && bytes[1] == GarudaPacket.MAGIC_BYTE_2 && bytes[2] == GarudaPacket.TYPE_HEARTBEAT) {
                val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                buffer.get() // M1
                buffer.get() // M2
                buffer.get() // Type
                val devHash = buffer.int
                val ts = buffer.int
                return GarudaPacket(
                    packetType = GarudaPacket.TYPE_HEARTBEAT,
                    packetId = devHash xor ts,
                    deviceHash = devHash,
                    timestamp = ts,
                    latitude = 0.0,
                    longitude = 0.0
                )
            }
            return null
        }

        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)

        // 1. Verify Magic Header
        val magic1 = buffer.get()
        val magic2 = buffer.get()
        if (magic1 != GarudaPacket.MAGIC_BYTE_1 || magic2 != GarudaPacket.MAGIC_BYTE_2) {
            return null
        }

        // Validate CRC16 Checksum
        val dataLength = bytes.size - 2
        val expectedChecksum = calculateCrc16(bytes.copyOf(dataLength))
        val actualChecksum = ByteBuffer.wrap(bytes, dataLength, 2).order(ByteOrder.BIG_ENDIAN).short

        if (expectedChecksum != actualChecksum) {
            return null
        }

        // 2. PacketType (1 byte)
        val packetType = buffer.get()

        // 3. PacketId (4 bytes)
        val packetId = buffer.int

        // 4. DeviceHash (4 bytes)
        val deviceHash = buffer.int

        // 5. Timestamp (4 bytes)
        val timestamp = buffer.int

        // 6. Latitude (4 bytes)
        val latInt = buffer.int
        val latitude = latInt / FIXED_POINT_SCALE

        // 7. Longitude (4 bytes)
        val lonInt = buffer.int
        val longitude = lonInt / FIXED_POINT_SCALE

        // 8. EmergencyType (1 byte)
        val emergencyType = buffer.get()

        // 9. HopAndTtl (1 byte)
        val hopAndTtl = buffer.get().toInt()
        val hopCount = hopAndTtl and 0x0F
        val ttl = (hopAndTtl shl 28 ushr 28 and 0xF0 ushr 4) or ((hopAndTtl ushr 4) and 0x0F)

        // 10. Extended Payload (if any)
        val payloadSize = dataLength - (GarudaPacket.LEGACY_FRAME_SIZE - 2)
        val payload = if (payloadSize > 0) {
            ByteArray(payloadSize).also { buffer.get(it) }
        } else {
            byteArrayOf()
        }

        return GarudaPacket(
            header = byteArrayOf(magic1, magic2),
            packetType = packetType,
            packetId = packetId,
            deviceHash = deviceHash,
            timestamp = timestamp,
            latitude = latitude,
            longitude = longitude,
            emergencyType = emergencyType,
            hopCount = hopCount,
            ttl = ttl,
            payload = payload,
            checksum = actualChecksum
        )
    }

    /**
     * Calculates CRC16-CCITT checksum over a byte array.
     */
    fun calculateCrc16(bytes: ByteArray): Short {
        var crc = 0xFFFF
        for (b in bytes) {
            crc = (crc ushr 8) or (crc shl 8) and 0xFFFF
            crc = crc xor (b.toInt() and 0xFF)
            crc = crc xor ((crc and 0xFF) ushr 4)
            crc = crc xor ((crc shl 12) and 0xFFFF)
            crc = crc xor (((crc and 0xFF) shl 5) and 0xFFFF)
        }
        return (crc and 0xFFFF).toShort()
    }
}
