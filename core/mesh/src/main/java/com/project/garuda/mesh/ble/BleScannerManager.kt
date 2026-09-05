package com.project.garuda.mesh.ble

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.bluetooth.le.BluetoothLeScanner
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.util.Log

/**
 * Manages BLE Scanning for Project Garuda mesh network.
 * Reliably captures all Garuda packets across all Android chipsets.
 */
class BleScannerManager(private val context: Context) {

    companion object {
        private const val TAG = "BleScannerManager"
    }

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

    private fun getScanner(): BluetoothLeScanner? {
        val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager
        val adapter = bluetoothManager?.adapter ?: BluetoothAdapter.getDefaultAdapter()
        return adapter?.bluetoothLeScanner
    }

    /**
     * Starts listening for Garuda BLE mesh broadcast packets.
     */
    fun startScanning(onPacketReceived: (deviceAddress: String, rawBytes: ByteArray) -> Unit) {
        val scanner = getScanner() ?: run {
            Log.e(TAG, "BluetoothLeScanner is unavailable on this device")
            return
        }

        if (isScanning) stopScanning()

        this.onPacketReceivedCallback = onPacketReceived

        // Use empty scan filters to ensure broad compatibility across all chipsets (Samsung/Qualcomm/MediaTek)
        // Software filtering on 0x4744 is performed in onScanResult
        val filters = emptyList<ScanFilter>()

        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .setReportDelay(0)
            .build()

        try {
            scanner.startScan(filters, settings, scanCallback)
            isScanning = true
            Log.d(TAG, "BLE Mesh Scanner started successfully")
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
            getScanner()?.stopScan(scanCallback)
        } catch (e: SecurityException) {
            Log.e(TAG, "Missing Bluetooth permissions to stop scanning", e)
        } catch (e: Exception) {
            Log.v(TAG, "Error stopping BLE scanner", e)
        } finally {
            isScanning = false
        }
    }
}
