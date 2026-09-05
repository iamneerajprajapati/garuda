package com.project.garuda.mesh.ble

import android.bluetooth.BluetoothAdapter
import android.bluetooth.le.BluetoothLeScanner
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.util.Log

/**
 * Manages BLE Scanning for Project Garuda mesh network.
 * Filters incoming scan results for Garuda Manufacturer ID (0x4744).
 */
class BleScannerManager(private val context: Context) {

    companion object {
        private const val TAG = "BleScannerManager"
    }

    private var bluetoothLeScanner: BluetoothLeScanner? = null
    private var isScanning = false
    private var onPacketReceivedCallback: ((deviceAddress: String, rawBytes: ByteArray) -> Unit)? = null

    private val scanCallback = object : ScanCallback() {
        override fun onScanResult(callbackType: Int, result: ScanResult?) {
            super.onScanResult(callbackType, result)
            val record = result?.scanRecord ?: return
            val manufacturerData = record.getManufacturerSpecificData(BleAdvertiserManager.MANUFACTURER_ID)
                ?: return
            val deviceAddress = result.device?.address ?: ""

            onPacketReceivedCallback?.invoke(deviceAddress, manufacturerData)
        }

        override fun onBatchScanResults(results: MutableList<ScanResult>?) {
            super.onBatchScanResults(results)
            results?.forEach { result ->
                val record = result.scanRecord ?: return@forEach
                val manufacturerData = record.getManufacturerSpecificData(BleAdvertiserManager.MANUFACTURER_ID)
                    ?: return@forEach
                val deviceAddress = result.device?.address ?: ""
                onPacketReceivedCallback?.invoke(deviceAddress, manufacturerData)
            }
        }

        override fun onScanFailed(errorCode: Int) {
            super.onScanFailed(errorCode)
            isScanning = false
            Log.e(TAG, "BLE Mesh Scan failed with error code: $errorCode")
        }
    }

    init {
        val bluetoothAdapter = BluetoothAdapter.getDefaultAdapter()
        bluetoothLeScanner = bluetoothAdapter?.bluetoothLeScanner
    }

    /**
     * Starts listening for Garuda BLE mesh broadcast packets.
     */
    fun startScanning(onPacketReceived: (deviceAddress: String, rawBytes: ByteArray) -> Unit) {
        val scanner = bluetoothLeScanner ?: run {
            Log.e(TAG, "BluetoothLeScanner is unavailable on this device")
            return
        }

        if (isScanning) stopScanning()

        this.onPacketReceivedCallback = onPacketReceived

        val filters = listOf(
            ScanFilter.Builder()
                .setManufacturerData(BleAdvertiserManager.MANUFACTURER_ID, null)
                .build()
        )

        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        try {
            scanner.startScan(filters, settings, scanCallback)
            isScanning = true
            Log.d(TAG, "BLE Mesh Scanner started")
        } catch (e: SecurityException) {
            Log.e(TAG, "Missing Bluetooth permissions to start scanning", e)
        } catch (e: Exception) {
            Log.e(TAG, "Error starting BLE scanner", e)
        }
    }

    /**
     * Stops active BLE scanning.
     */
    fun stopScanning() {
        if (!isScanning) return
        try {
            bluetoothLeScanner?.stopScan(scanCallback)
        } catch (e: SecurityException) {
            Log.e(TAG, "Missing Bluetooth permissions to stop scanning", e)
        } catch (e: Exception) {
            Log.e(TAG, "Error stopping BLE scanner", e)
        } finally {
            isScanning = false
            onPacketReceivedCallback = null
        }
    }
}
