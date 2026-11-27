package com.example.media

import android.content.Context
import android.graphics.SurfaceTexture
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.view.TextureView
import org.webrtc.EglBase
import org.webrtc.EglRenderer
import org.webrtc.GlRectDrawer
import org.webrtc.VideoFrame
import org.webrtc.VideoSink
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private const val TEXTURE_RENDER_TAG = "WebRtcTextureView"

/**
 * Hardware-composited TextureView WebRTC VideoSink backed by EglRenderer.
 * Unlike SurfaceViewRenderer, TextureView participates natively in Jetpack Compose's
 * layer tree — eliminating SurfaceFlinger z-order conflicts, black holes behind overlays,
 * PiP rounded-corner clipping bugs, and stale dead surfaces when tracks mount/unmount.
 */
class WebRtcTextureViewRenderer(
    context: Context,
    rendererName: String = "UChatTextureRenderer"
) : TextureView(context), TextureView.SurfaceTextureListener, VideoSink {

    private val mainHandler = Handler(Looper.getMainLooper())
    private val eglRenderer = EglRenderer(rendererName)
    private val initialized = AtomicBoolean(false)
    private val released = AtomicBoolean(false)
    private val firstFrameNotified = AtomicBoolean(false)

    @Volatile
    private var mirrorEnabled: Boolean = false

    @Volatile
    private var onFirstFrameCallback: (() -> Unit)? = null

    init {
        isOpaque = false
        surfaceTextureListener = this
    }

    fun init(
        eglBaseContext: EglBase.Context,
        onFirstFrameRendered: (() -> Unit)? = null
    ) {
        onFirstFrameCallback = onFirstFrameRendered
        if (!initialized.compareAndSet(false, true)) return
        try {
            eglRenderer.init(eglBaseContext, EglBase.CONFIG_PLAIN, GlRectDrawer())
            eglRenderer.setMirror(mirrorEnabled)
            updateAspectRatio(width, height)
            if (isAvailable) {
                surfaceTexture?.let { st ->
                    eglRenderer.createEglSurface(st)
                }
            }
        } catch (e: Exception) {
            Log.e(TEXTURE_RENDER_TAG, "EglRenderer init failed: ${e.message}")
            initialized.set(false)
        }
    }

    fun setOnFirstFrameRenderedListener(listener: (() -> Unit)?) {
        onFirstFrameCallback = listener
    }

    fun setMirror(mirror: Boolean) {
        mirrorEnabled = mirror
        if (initialized.get() && !released.get()) {
            runCatching { eglRenderer.setMirror(mirror) }
        }
    }

    fun clearImage() {
        firstFrameNotified.set(false)
        if (initialized.get() && !released.get()) {
            runCatching { eglRenderer.clearImage() }
        }
    }

    fun release() {
        if (!released.compareAndSet(false, true)) return
        onFirstFrameCallback = null
        surfaceTextureListener = null
        if (initialized.get()) {
            runCatching { eglRenderer.clearImage() }
            runCatching { eglRenderer.release() }
        }
    }

    override fun onFrame(frame: VideoFrame) {
        if (released.get() || !initialized.get()) return
        if (firstFrameNotified.compareAndSet(false, true)) {
            mainHandler.post {
                if (!released.get()) {
                    onFirstFrameCallback?.invoke()
                }
            }
        }
        eglRenderer.onFrame(frame)
    }

    override fun onLayout(changed: Boolean, left: Int, top: Int, right: Int, bottom: Int) {
        super.onLayout(changed, left, top, right, bottom)
        updateAspectRatio(right - left, bottom - top)
    }

    private fun updateAspectRatio(w: Int, h: Int) {
        if (w > 0 && h > 0 && initialized.get() && !released.get()) {
            runCatching { eglRenderer.setLayoutAspectRatio(w.toFloat() / h.toFloat()) }
        }
    }

    override fun onSurfaceTextureAvailable(surface: SurfaceTexture, width: Int, height: Int) {
        if (released.get() || !initialized.get()) return
        runCatching {
            eglRenderer.createEglSurface(surface)
            updateAspectRatio(width, height)
        }
    }

    override fun onSurfaceTextureSizeChanged(surface: SurfaceTexture, width: Int, height: Int) {
        updateAspectRatio(width, height)
    }

    override fun onSurfaceTextureDestroyed(surface: SurfaceTexture): Boolean {
        if (initialized.get() && !released.get()) {
            val latch = CountDownLatch(1)
            runCatching {
                eglRenderer.releaseEglSurface { latch.countDown() }
                latch.await(450L, TimeUnit.MILLISECONDS)
            }
        }
        return true
    }

    override fun onSurfaceTextureUpdated(surface: SurfaceTexture) = Unit
}
