package com.project.garuda.mesh.service

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Build
import android.util.Log
import androidx.core.content.ContextCompat

/**
 * BroadcastReceiver that auto-restarts [MeshForegroundService] if the app is closed,
 * swiped away from Recents, or when the phone reboots/updates.
 */
class MeshRestarterReceiver : BroadcastReceiver() {

    companion object {
        private const val TAG = "MeshRestarterReceiver"
        const val ACTION_RESTART_MESH = "com.project.garuda.mesh.ACTION_RESTART_MESH"
    }

    override fun onReceive(context: Context, intent: Intent?) {
        Log.d(TAG, "Received broadcast action: ${intent?.action}. Restarting MeshForegroundService...")

        val serviceIntent = Intent(context, MeshForegroundService::class.java).apply {
            action = MeshForegroundService.ACTION_START_HIGH_ALERT
        }

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                ContextCompat.startForegroundService(context, serviceIntent)
            } else {
                context.startService(serviceIntent)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error starting MeshForegroundService from MeshRestarterReceiver", e)
        }
    }
}
