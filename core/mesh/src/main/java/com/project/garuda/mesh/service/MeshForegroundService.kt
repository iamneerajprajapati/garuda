package com.project.garuda.mesh.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Binder
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.project.garuda.mesh.ble.BleAdvertiserManager
import com.project.garuda.mesh.ble.BleScannerManager
import com.project.garuda.mesh.engine.MeshRelayEngine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * Foreground Service running the background BLE Mesh Engine.
 * Displays ongoing notification "Garuda Disaster Mesh Active" and manages adaptive duty-cycling.
 */
class MeshForegroundService : Service() {

    enum class DutyCycleMode {
        HIGH_ALERT,          // Continuous scan & advertise (0ms delay)
        BACKGROUND_STANDBY   // 5 seconds scan / 25 seconds idle battery saving cycle
    }

    companion object {
        const val CHANNEL_ID = "garuda_mesh_channel"
        const val NOTIFICATION_ID = 1001

        const val ACTION_START_HIGH_ALERT = "ACTION_START_HIGH_ALERT"
        const val ACTION_START_STANDBY = "ACTION_START_STANDBY"
        const val ACTION_STOP_SERVICE = "ACTION_STOP_SERVICE"

        const val STANDBY_SCAN_DURATION_MS = 5000L
        const val STANDBY_IDLE_DURATION_MS = 25000L
    }

    private val binder = LocalBinder()
    private val serviceScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private lateinit var advertiserManager: BleAdvertiserManager
    private lateinit var scannerManager: BleScannerManager
    lateinit var meshRelayEngine: MeshRelayEngine
        private set

    private var dutyCycleMode = DutyCycleMode.HIGH_ALERT
    private var dutyCycleJob: Job? = null

    inner class LocalBinder : Binder() {
        fun getService(): MeshForegroundService = this@MeshForegroundService
    }

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()

        advertiserManager = BleAdvertiserManager(this)
        scannerManager = BleScannerManager(this)
        meshRelayEngine = MeshRelayEngine(advertiserManager, scannerManager, serviceScope)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START_HIGH_ALERT -> startMeshService(DutyCycleMode.HIGH_ALERT)
            ACTION_START_STANDBY -> startMeshService(DutyCycleMode.BACKGROUND_STANDBY)
            ACTION_STOP_SERVICE -> stopMeshService()
            else -> startMeshService(DutyCycleMode.HIGH_ALERT)
        }
        return START_STICKY
    }

    private fun startMeshService(mode: DutyCycleMode) {
        val notification = createNotification("Garuda Disaster Mesh Active")
        startForeground(NOTIFICATION_ID, notification)

        setDutyCycleMode(mode)
    }

    fun setDutyCycleMode(mode: DutyCycleMode) {
        this.dutyCycleMode = mode
        dutyCycleJob?.cancel()

        when (mode) {
            DutyCycleMode.HIGH_ALERT -> {
                // Continuous scanning
                scannerManager.startScanning { rawBytes ->
                    meshRelayEngine.processIncomingRawBytes(rawBytes)
                }
            }
            DutyCycleMode.BACKGROUND_STANDBY -> {
                // Duty-cycled scanning: 5s active scan / 25s idle
                dutyCycleJob = serviceScope.launch {
                    while (isActive) {
                        scannerManager.startScanning { rawBytes ->
                            meshRelayEngine.processIncomingRawBytes(rawBytes)
                        }
                        delay(STANDBY_SCAN_DURATION_MS)

                        scannerManager.stopScanning()
                        delay(STANDBY_IDLE_DURATION_MS)
                    }
                }
            }
        }
    }

    private fun stopMeshService() {
        dutyCycleJob?.cancel()
        scannerManager.stopScanning()
        advertiserManager.stopAdvertising()
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onTaskRemoved(rootIntent: Intent?) {
        super.onTaskRemoved(rootIntent)
        android.util.Log.w(TAG, "App swiped away from Recents. Scheduling auto-restart of MeshForegroundService...")

        val restartIntent = Intent(this, MeshRestarterReceiver::class.java).apply {
            action = MeshRestarterReceiver.ACTION_RESTART_MESH
        }

        val flags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            android.app.PendingIntent.FLAG_ONE_SHOT or android.app.PendingIntent.FLAG_IMMUTABLE
        } else {
            android.app.PendingIntent.FLAG_ONE_SHOT
        }

        val pendingIntent = android.app.PendingIntent.getBroadcast(this, 101, restartIntent, flags)
        val alarmManager = getSystemService(Context.ALARM_SERVICE) as? android.app.AlarmManager
        alarmManager?.set(
            android.app.AlarmManager.ELAPSED_REALTIME_WAKEUP,
            android.os.SystemClock.elapsedRealtime() + 1000,
            pendingIntent
        )
    }

    override fun onDestroy() {
        stopMeshService()
        super.onDestroy()
    }


    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "Garuda Mesh Network",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Background BLE Multi-Hop Mesh Relay Engine for Disaster Emergency Telemetry"
            }
            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            manager.createNotificationChannel(channel)
        }
    }

    private fun createNotification(contentText: String): Notification {
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Garuda Mesh Active")
            .setContentText(contentText)
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }
}
