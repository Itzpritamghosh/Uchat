package com.example.media

import android.content.Context
import android.media.AudioFormat
import android.media.MediaRecorder
import android.os.Handler
import android.os.Looper
import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import org.webrtc.AudioSource
import org.webrtc.AudioTrack
import org.webrtc.Camera1Enumerator
import org.webrtc.Camera2Enumerator
import org.webrtc.CameraEnumerator
import org.webrtc.CameraVideoCapturer
import org.webrtc.DataChannel
import org.webrtc.EglBase
import org.webrtc.IceCandidate
import org.webrtc.MediaConstraints
import org.webrtc.MediaStream
import org.webrtc.MediaStreamTrack
import org.webrtc.PeerConnection
import org.webrtc.PeerConnectionFactory
import org.webrtc.RtpReceiver
import org.webrtc.RtpTransceiver
import org.webrtc.SdpObserver
import org.webrtc.SessionDescription
import org.webrtc.SoftwareVideoDecoderFactory
import org.webrtc.SoftwareVideoEncoderFactory
import org.webrtc.SurfaceTextureHelper
import org.webrtc.VideoSource
import org.webrtc.VideoTrack
import org.webrtc.audio.JavaAudioDeviceModule
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

private const val WEBRTC_TAG = "WebRtcEngine"
private const val CAPTURE_WIDTH = 640
private const val CAPTURE_HEIGHT = 480
private const val CAPTURE_FPS = 24
private const val FIRST_FRAME_WATCHDOG_MS = 2600L

data class WebRtcCallState(
    val iceState: String = "NEW",
    val peerState: String = "NEW",
    val localVideoTrack: VideoTrack? = null,
    val remoteVideoTrack: VideoTrack? = null,
    val hasLocalCamera: Boolean = false,
    val localFirstFrameReady: Boolean = false,
    val isFrontCamera: Boolean = true,
    val isCameraEnabled: Boolean = true,
    val isSwitchingCamera: Boolean = false,
    val localCandidatesSent: Int = 0,
    val remoteCandidatesAdded: Int = 0,
    val isConnected: Boolean = false,
    val errorMessage: String? = null
)

/**
 * Peer-to-peer WebRTC audio & video engine tuned for universal Android device compatibility:
 * - Uses SoftwareVideoEncoderFactory & SoftwareVideoDecoderFactory (VP8/VP9) to eliminate
 *   cross-chipset hardware MediaCodec OES black screens (ColorOS/MediaTek/Qualcomm/Exynos).
 * - Never mutates SDP payload strings or premature sender bitrate parameters before negotiation.
 * - Automatic Camera2 -> Camera1 fallback watchdog if Camera2 HAL stalls before first frame.
 * - Background-threaded camera open/flip/recovery so camera controls never hang.
 */
class WebRtcCallEngine(
    context: Context,
    private val isVideoCall: Boolean,
    private val canCaptureCamera: Boolean,
    private val onLocalIceCandidate: (IceCandidate) -> Unit,
    private val onIceConnected: () -> Unit,
    private val onIceDisconnectedOrFailed: (String) -> Unit
) {
    companion object {
        private val factoryInitialized = AtomicBoolean(false)

        val ICE_SERVERS: List<PeerConnection.IceServer> = listOf(
            PeerConnection.IceServer.builder("stun:stun.l.google.com:19302").createIceServer(),
            PeerConnection.IceServer.builder("stun:stun1.l.google.com:19302").createIceServer(),
            PeerConnection.IceServer.builder("stun:stun.relay.metered.ca:80").createIceServer(),
            PeerConnection.IceServer.builder("stun:stun.cloudflare.com:3478").createIceServer(),
            PeerConnection.IceServer.builder("turn:openrelay.metered.ca:80")
                .setUsername("openrelayproject")
                .setPassword("openrelayproject")
                .createIceServer(),
            PeerConnection.IceServer.builder("turn:openrelay.metered.ca:443")
                .setUsername("openrelayproject")
                .setPassword("openrelayproject")
                .createIceServer(),
            PeerConnection.IceServer.builder("turn:openrelay.metered.ca:443?transport=tcp")
                .setUsername("openrelayproject")
                .setPassword("openrelayproject")
                .createIceServer(),
            PeerConnection.IceServer.builder("turns:openrelay.metered.ca:443?transport=tcp")
                .setUsername("openrelayproject")
                .setPassword("openrelayproject")
                .createIceServer()
        )
    }

    private val appContext = context.applicationContext
    private val mainHandler = Handler(Looper.getMainLooper())
    private val cameraExecutor = Executors.newSingleThreadExecutor()
    val eglBase: EglBase = EglBase.create()

    private val _state = MutableStateFlow(WebRtcCallState(isCameraEnabled = isVideoCall && canCaptureCamera))
    val state: StateFlow<WebRtcCallState> = _state.asStateFlow()

    private var audioDeviceModule: JavaAudioDeviceModule? = null
    private var peerConnectionFactory: PeerConnectionFactory? = null
    private var peerConnection: PeerConnection? = null
    private var audioSource: AudioSource? = null
    private var localAudioTrack: AudioTrack? = null
    private var videoSource: VideoSource? = null
    private var localVideoTrack: VideoTrack? = null
    private var videoCapturer: CameraVideoCapturer? = null
    private var surfaceTextureHelper: SurfaceTextureHelper? = null
    private var cameraEnumerator: CameraEnumerator? = null
    private var currentCameraDevice: String? = null

    private val iceLock = Any()
    private var remoteDescriptionSet = false
    private val pendingRemoteCandidates = mutableListOf<IceCandidate>()
    private val addedCandidateKeys = mutableSetOf<String>()
    private val connectedNotified = AtomicBoolean(false)
    private val isSwitchingCamera = AtomicBoolean(false)
    private val captureGeneration = AtomicInteger(0)

    @Volatile
    private var useCamera1Fallback = false
    @Volatile
    private var cameraCapturing = false
    @Volatile
    private var firstFrameSeenForCurrentSession = false
    @Volatile
    private var desiredCameraEnabled = isVideoCall && canCaptureCamera
    @Volatile
    private var desiredFrontCamera = true
    @Volatile
    private var released = false

    private var firstFrameWatchdog: Runnable? = null

    init {
        initializeEngine()
    }

    private fun postMain(block: () -> Unit) {
        if (released) return
        if (Looper.myLooper() == Looper.getMainLooper()) {
            block()
        } else {
            mainHandler.post {
                if (!released) block()
            }
        }
    }

    private fun notifyConnectedOnce() {
        _state.value = _state.value.copy(isConnected = true, errorMessage = null)
        refreshRemoteVideoTrackFromTransceivers()
        // Re-check remote video track shortly after ICE connects in case tracks finish binding slightly later
        mainHandler.postDelayed({ refreshRemoteVideoTrackFromTransceivers() }, 500L)
        mainHandler.postDelayed({ refreshRemoteVideoTrackFromTransceivers() }, 1500L)
        if (connectedNotified.compareAndSet(false, true)) {
            postMain { onIceConnected() }
        }
    }

    private fun refreshRemoteVideoTrackFromTransceivers() {
        if (released || !isVideoCall) return
        runCatching {
            val remoteVid = peerConnection?.transceivers
                ?.mapNotNull { it.receiver?.track() as? VideoTrack }
                ?.firstOrNull()
            if (remoteVid != null) {
                remoteVid.setEnabled(true)
                if (_state.value.remoteVideoTrack !== remoteVid) {
                    _state.value = _state.value.copy(remoteVideoTrack = remoteVid)
                }
            }
        }
    }

    private fun initializeEngine() {
        try {
            if (factoryInitialized.compareAndSet(false, true)) {
                val initOptions = PeerConnectionFactory.InitializationOptions.builder(appContext)
                    .setEnableInternalTracer(false)
                    .createInitializationOptions()
                PeerConnectionFactory.initialize(initOptions)
            }

            val adm = JavaAudioDeviceModule.builder(appContext)
                .setAudioSource(MediaRecorder.AudioSource.VOICE_COMMUNICATION)
                .setAudioFormat(AudioFormat.ENCODING_PCM_16BIT)
                .setUseHardwareAcousticEchoCanceler(
                    JavaAudioDeviceModule.isBuiltInAcousticEchoCancelerSupported()
                )
                .setUseHardwareNoiseSuppressor(
                    JavaAudioDeviceModule.isBuiltInNoiseSuppressorSupported()
                )
                .setAudioRecordErrorCallback(object : JavaAudioDeviceModule.AudioRecordErrorCallback {
                    override fun onWebRtcAudioRecordInitError(errorMessage: String?) {
                        Log.e(WEBRTC_TAG, "AudioRecord init error: $errorMessage")
                    }
                    override fun onWebRtcAudioRecordStartError(
                        errorCode: JavaAudioDeviceModule.AudioRecordStartErrorCode?,
                        errorMessage: String?
                    ) {
                        Log.e(WEBRTC_TAG, "AudioRecord start error ($errorCode): $errorMessage")
                    }
                    override fun onWebRtcAudioRecordError(errorMessage: String?) {
                        Log.e(WEBRTC_TAG, "AudioRecord error: $errorMessage")
                    }
                })
                .setAudioTrackErrorCallback(object : JavaAudioDeviceModule.AudioTrackErrorCallback {
                    override fun onWebRtcAudioTrackInitError(errorMessage: String?) {
                        Log.e(WEBRTC_TAG, "AudioTrack init error: $errorMessage")
                    }
                    override fun onWebRtcAudioTrackStartError(
                        errorCode: JavaAudioDeviceModule.AudioTrackStartErrorCode?,
                        errorMessage: String?
                    ) {
                        Log.e(WEBRTC_TAG, "AudioTrack start error ($errorCode): $errorMessage")
                    }
                    override fun onWebRtcAudioTrackError(errorMessage: String?) {
                        Log.e(WEBRTC_TAG, "AudioTrack error: $errorMessage")
                    }
                })
                .createAudioDeviceModule()
            audioDeviceModule = adm

            // Universal software VP8/VP9 encoder + decoder eliminates vendor MediaCodec OES black frames
            val encoderFactory = SoftwareVideoEncoderFactory()
            val decoderFactory = SoftwareVideoDecoderFactory()

            val factory = PeerConnectionFactory.builder()
                .setAudioDeviceModule(adm)
                .setVideoEncoderFactory(encoderFactory)
                .setVideoDecoderFactory(decoderFactory)
                .createPeerConnectionFactory()
            peerConnectionFactory = factory

            val rtcConfig = PeerConnection.RTCConfiguration(ICE_SERVERS).apply {
                sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN
                bundlePolicy = PeerConnection.BundlePolicy.MAXBUNDLE
                rtcpMuxPolicy = PeerConnection.RtcpMuxPolicy.REQUIRE
                tcpCandidatePolicy = PeerConnection.TcpCandidatePolicy.ENABLED
                continualGatheringPolicy = PeerConnection.ContinualGatheringPolicy.GATHER_CONTINUALLY
                iceTransportsType = PeerConnection.IceTransportsType.ALL
                iceCandidatePoolSize = 4
            }

            peerConnection = factory.createPeerConnection(rtcConfig, object : PeerConnection.Observer {
                override fun onIceCandidate(candidate: IceCandidate) {
                    if (released) return
                    _state.value = _state.value.copy(
                        localCandidatesSent = _state.value.localCandidatesSent + 1
                    )
                    postMain { onLocalIceCandidate(candidate) }
                }

                override fun onIceConnectionChange(newState: PeerConnection.IceConnectionState) {
                    if (released) return
                    Log.d(WEBRTC_TAG, "ICE state -> $newState")
                    _state.value = _state.value.copy(iceState = newState.name)
                    when (newState) {
                        PeerConnection.IceConnectionState.CONNECTED,
                        PeerConnection.IceConnectionState.COMPLETED -> notifyConnectedOnce()
                        PeerConnection.IceConnectionState.FAILED ->
                            postMain { onIceDisconnectedOrFailed("Reconnecting network...") }
                        PeerConnection.IceConnectionState.DISCONNECTED ->
                            postMain { onIceDisconnectedOrFailed("Connection interrupted...") }
                        else -> Unit
                    }
                }

                override fun onConnectionChange(newState: PeerConnection.PeerConnectionState) {
                    if (released) return
                    Log.d(WEBRTC_TAG, "PeerConnection state -> $newState")
                    _state.value = _state.value.copy(peerState = newState.name)
                    when (newState) {
                        PeerConnection.PeerConnectionState.CONNECTED -> notifyConnectedOnce()
                        PeerConnection.PeerConnectionState.FAILED ->
                            postMain { onIceDisconnectedOrFailed("Reconnecting network...") }
                        else -> Unit
                    }
                }

                override fun onTrack(transceiver: RtpTransceiver?) {
                    if (released) return
                    val track = transceiver?.receiver?.track()
                    if (track is VideoTrack) {
                        track.setEnabled(true)
                        _state.value = _state.value.copy(remoteVideoTrack = track)
                    } else if (track is AudioTrack) {
                        track.setEnabled(true)
                        track.setVolume(1.0)
                    }
                }

                override fun onAddTrack(receiver: RtpReceiver?, mediaStreams: Array<out MediaStream>?) {
                    if (released) return
                    val track = receiver?.track()
                    if (track is VideoTrack) {
                        track.setEnabled(true)
                        _state.value = _state.value.copy(remoteVideoTrack = track)
                    } else if (track is AudioTrack) {
                        track.setEnabled(true)
                        track.setVolume(1.0)
                    }
                }

                override fun onSignalingChange(state: PeerConnection.SignalingState?) = Unit
                override fun onIceConnectionReceivingChange(receiving: Boolean) = Unit
                override fun onIceGatheringChange(state: PeerConnection.IceGatheringState?) = Unit
                override fun onIceCandidatesRemoved(candidates: Array<out IceCandidate>?) = Unit
                override fun onAddStream(stream: MediaStream?) {
                    if (released) return
                    stream?.audioTracks?.forEach {
                        it.setEnabled(true)
                        it.setVolume(1.0)
                    }
                    val remoteVid = stream?.videoTracks?.firstOrNull()
                    if (remoteVid != null) {
                        remoteVid.setEnabled(true)
                        _state.value = _state.value.copy(remoteVideoTrack = remoteVid)
                    }
                }
                override fun onRemoveStream(stream: MediaStream?) = Unit
                override fun onDataChannel(channel: DataChannel?) = Unit
                override fun onRenegotiationNeeded() = Unit
            })

            setupLocalMedia(factory)
        } catch (e: Exception) {
            Log.e(WEBRTC_TAG, "WebRTC initialization error", e)
            _state.value = _state.value.copy(errorMessage = e.localizedMessage ?: "WebRTC init failed")
        }
    }

    private fun setupLocalMedia(factory: PeerConnectionFactory) {
        val pc = peerConnection ?: return

        val audioConstraints = MediaConstraints().apply {
            mandatory.add(MediaConstraints.KeyValuePair("googEchoCancellation", "true"))
            mandatory.add(MediaConstraints.KeyValuePair("googAutoGainControl", "true"))
            mandatory.add(MediaConstraints.KeyValuePair("googNoiseSuppression", "true"))
            mandatory.add(MediaConstraints.KeyValuePair("googHighpassFilter", "true"))
        }
        val aSource = factory.createAudioSource(audioConstraints)
        val aTrack = factory.createAudioTrack("uchat_audio_0", aSource).apply {
            setEnabled(true)
            setVolume(1.0)
        }
        audioSource = aSource
        localAudioTrack = aTrack
        pc.addTrack(aTrack, listOf("uchat_stream"))

        if (isVideoCall) {
            val started = if (canCaptureCamera) setupLocalVideoTrack(factory, pc) else false
            if (!started) {
                runCatching {
                    pc.addTransceiver(
                        MediaStreamTrack.MediaType.MEDIA_TYPE_VIDEO,
                        RtpTransceiver.RtpTransceiverInit(RtpTransceiver.RtpTransceiverDirection.RECV_ONLY)
                    )
                }
            }
        }
    }

    /**
     * Allows starting the camera cleanly if CAMERA permission is granted slightly after
     * engine construction, without tearing down the active audio call.
     */
    fun enableCameraAfterPermissionGranted() {
        if (released || !isVideoCall) return
        if (localVideoTrack != null) {
            if (desiredCameraEnabled && !cameraCapturing) {
                setCameraEnabled(true)
            }
            return
        }
        val factory = peerConnectionFactory ?: return
        val pc = peerConnection ?: return
        desiredCameraEnabled = true
        setupLocalVideoTrack(factory, pc)
    }

    private fun cancelFirstFrameWatchdog() {
        firstFrameWatchdog?.let { mainHandler.removeCallbacks(it) }
        firstFrameWatchdog = null
    }

    private fun scheduleFirstFrameWatchdog(gen: Int) {
        cancelFirstFrameWatchdog()
        val runnable = Runnable {
            if (released || !desiredCameraEnabled) return@Runnable
            if (captureGeneration.get() != gen || firstFrameSeenForCurrentSession) return@Runnable
            if (!useCamera1Fallback) {
                Log.w(WEBRTC_TAG, "Camera2 delivered no frames within ${FIRST_FRAME_WATCHDOG_MS}ms — switching to Camera1 fallback")
                useCamera1Fallback = true
                cameraEnumerator = Camera1Enumerator(true)
                cameraExecutor.execute {
                    if (released || !desiredCameraEnabled) return@execute
                    val enumerator = cameraEnumerator ?: Camera1Enumerator(true).also { cameraEnumerator = it }
                    val targetDevice = pickCameraDevice(enumerator, desiredFrontCamera) ?: return@execute
                    openAndStartCapturerOnCameraThread(targetDevice)
                }
            } else {
                Log.w(WEBRTC_TAG, "Camera1 first-frame timeout — retrying capture cleanly")
                recoverCameraAsync()
            }
        }
        firstFrameWatchdog = runnable
        mainHandler.postDelayed(runnable, FIRST_FRAME_WATCHDOG_MS)
    }

    private fun buildCameraEventsHandler(gen: Int): CameraVideoCapturer.CameraEventsHandler =
        object : CameraVideoCapturer.CameraEventsHandler {
            override fun onCameraError(errorDescription: String?) {
                Log.w(WEBRTC_TAG, "Camera error: $errorDescription — recovering")
                if (!useCamera1Fallback) {
                    useCamera1Fallback = true
                    cameraEnumerator = Camera1Enumerator(true)
                }
                recoverCameraAsync()
            }
            override fun onCameraDisconnected() {
                Log.w(WEBRTC_TAG, "Camera disconnected — recovering")
                recoverCameraAsync()
            }
            override fun onCameraFreezed(errorDescription: String?) {
                Log.w(WEBRTC_TAG, "Camera freezed: $errorDescription — recovering")
                if (!useCamera1Fallback) {
                    useCamera1Fallback = true
                    cameraEnumerator = Camera1Enumerator(true)
                }
                recoverCameraAsync()
            }
            override fun onCameraOpening(cameraName: String?) {
                Log.d(WEBRTC_TAG, "Camera opening: $cameraName")
            }
            override fun onFirstFrameAvailable() {
                if (captureGeneration.get() == gen) {
                    firstFrameSeenForCurrentSession = true
                    cancelFirstFrameWatchdog()
                    _state.value = _state.value.copy(
                        localFirstFrameReady = true,
                        isSwitchingCamera = false
                    )
                }
                Log.d(WEBRTC_TAG, "Local camera first frame available (gen=$gen, cam1=$useCamera1Fallback)")
            }
            override fun onCameraClosed() {
                Log.d(WEBRTC_TAG, "Camera closed")
            }
        }

    private fun createCameraEnumerator(): CameraEnumerator =
        if (!useCamera1Fallback && Camera2Enumerator.isSupported(appContext)) {
            Camera2Enumerator(appContext)
        } else {
            useCamera1Fallback = true
            Camera1Enumerator(true)
        }

    private fun pickCameraDevice(enumerator: CameraEnumerator, preferFront: Boolean): String? {
        val devices = enumerator.deviceNames
        if (devices.isEmpty()) return null
        return if (preferFront) {
            devices.firstOrNull { enumerator.isFrontFacing(it) } ?: devices.firstOrNull()
        } else {
            devices.firstOrNull { enumerator.isBackFacing(it) } ?: devices.firstOrNull()
        }
    }

    private fun setupLocalVideoTrack(factory: PeerConnectionFactory, pc: PeerConnection): Boolean {
        return try {
            val enumerator = createCameraEnumerator()
            cameraEnumerator = enumerator
            val chosenDevice = pickCameraDevice(enumerator, preferFront = true)
            if (chosenDevice == null) {
                Log.w(WEBRTC_TAG, "No camera found on device; proceeding with receive-only video.")
                return false
            }
            val isFront = enumerator.isFrontFacing(chosenDevice)
            desiredFrontCamera = isFront
            currentCameraDevice = chosenDevice

            val textureHelper = SurfaceTextureHelper.create("UChatCaptureThread", eglBase.eglBaseContext)
            val vSource = factory.createVideoSource(false)
            val vTrack = factory.createVideoTrack("uchat_video_0", vSource).apply { setEnabled(true) }

            surfaceTextureHelper = textureHelper
            videoSource = vSource
            localVideoTrack = vTrack

            pc.addTrack(vTrack, listOf("uchat_stream"))

            _state.value = _state.value.copy(
                localVideoTrack = vTrack,
                hasLocalCamera = true,
                isFrontCamera = isFront,
                isCameraEnabled = true
            )

            cameraExecutor.execute {
                if (released) return@execute
                openAndStartCapturerOnCameraThread(chosenDevice)
            }
            true
        } catch (e: Exception) {
            Log.w(WEBRTC_TAG, "Camera setup unavailable: ${e.message}")
            _state.value = _state.value.copy(hasLocalCamera = false)
            false
        }
    }

    private fun openAndStartCapturerOnCameraThread(deviceName: String) {
        if (released) return
        val enumerator = cameraEnumerator ?: createCameraEnumerator().also { cameraEnumerator = it }
        val textureHelper = surfaceTextureHelper ?: return
        val vSource = videoSource ?: return

        cancelFirstFrameWatchdog()
        val gen = captureGeneration.incrementAndGet()
        firstFrameSeenForCurrentSession = false

        runCatching {
            if (cameraCapturing) {
                videoCapturer?.stopCapture()
            }
        }
        runCatching { videoCapturer?.dispose() }
        videoCapturer = null
        cameraCapturing = false

        try {
            val capturer = enumerator.createCapturer(deviceName, buildCameraEventsHandler(gen))
                ?: run {
                    if (!useCamera1Fallback) {
                        useCamera1Fallback = true
                        val cam1 = Camera1Enumerator(true).also { cameraEnumerator = it }
                        val fallbackDev = pickCameraDevice(cam1, desiredFrontCamera) ?: return
                        openAndStartCapturerOnCameraThread(fallbackDev)
                    }
                    return
                }
            capturer.initialize(textureHelper, appContext, vSource.capturerObserver)
            try {
                capturer.startCapture(CAPTURE_WIDTH, CAPTURE_HEIGHT, CAPTURE_FPS)
            } catch (_: Exception) {
                capturer.startCapture(480, 360, 20)
            }
            videoCapturer = capturer
            currentCameraDevice = deviceName
            cameraCapturing = true
            val isFront = enumerator.isFrontFacing(deviceName)
            desiredFrontCamera = isFront
            localVideoTrack?.setEnabled(desiredCameraEnabled)
            _state.value = _state.value.copy(
                isFrontCamera = isFront,
                hasLocalCamera = true,
                isSwitchingCamera = false
            )
            scheduleFirstFrameWatchdog(gen)
        } catch (e: Exception) {
            Log.e(WEBRTC_TAG, "Failed starting camera $deviceName: ${e.message}")
            cameraCapturing = false
            if (!useCamera1Fallback) {
                useCamera1Fallback = true
                val cam1 = Camera1Enumerator(true).also { cameraEnumerator = it }
                val fallbackDev = pickCameraDevice(cam1, desiredFrontCamera)
                if (fallbackDev != null) {
                    openAndStartCapturerOnCameraThread(fallbackDev)
                    return
                }
            }
            _state.value = _state.value.copy(isSwitchingCamera = false)
        }
    }

    private fun recoverCameraAsync() {
        if (released || !desiredCameraEnabled) return
        cameraExecutor.execute {
            if (released || !desiredCameraEnabled) return@execute
            try {
                Thread.sleep(250L)
            } catch (_: InterruptedException) {
                return@execute
            }
            val enumerator = cameraEnumerator ?: createCameraEnumerator().also { cameraEnumerator = it }
            val targetDevice = pickCameraDevice(enumerator, desiredFrontCamera)
                ?: currentCameraDevice
                ?: return@execute
            openAndStartCapturerOnCameraThread(targetDevice)
        }
    }

    fun createOffer(iceRestart: Boolean = false, onOfferCreated: (String) -> Unit) {
        val pc = peerConnection ?: return
        val constraints = MediaConstraints().apply {
            mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveAudio", "true"))
            mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveVideo", if (isVideoCall) "true" else "false"))
            if (iceRestart) {
                mandatory.add(MediaConstraints.KeyValuePair("IceRestart", "true"))
            }
        }
        pc.createOffer(object : SimpleSdpObserver("createOffer") {
            override fun onCreateSuccess(desc: SessionDescription?) {
                if (released || desc == null) return
                pc.setLocalDescription(object : SimpleSdpObserver("setLocalOffer") {
                    override fun onSetSuccess() {
                        if (!released) postMain { onOfferCreated(desc.description) }
                    }
                }, desc)
            }
        }, constraints)
    }

    fun handleRemoteOfferAndCreateAnswer(
        remoteSdpOffer: String,
        onAnswerCreated: (String) -> Unit
    ) {
        val pc = peerConnection ?: return
        val offerDesc = SessionDescription(SessionDescription.Type.OFFER, remoteSdpOffer)
        pc.setRemoteDescription(object : SimpleSdpObserver("setRemoteOffer") {
            override fun onSetSuccess() {
                if (released) return
                drainPendingRemoteCandidates()
                refreshRemoteVideoTrackFromTransceivers()
                val constraints = MediaConstraints().apply {
                    mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveAudio", "true"))
                    mandatory.add(MediaConstraints.KeyValuePair("OfferToReceiveVideo", if (isVideoCall) "true" else "false"))
                }
                pc.createAnswer(object : SimpleSdpObserver("createAnswer") {
                    override fun onCreateSuccess(answerDesc: SessionDescription?) {
                        if (released || answerDesc == null) return
                        pc.setLocalDescription(object : SimpleSdpObserver("setLocalAnswer") {
                            override fun onSetSuccess() {
                                if (!released) postMain { onAnswerCreated(answerDesc.description) }
                            }
                        }, answerDesc)
                    }
                }, constraints)
            }
        }, offerDesc)
    }

    fun setRemoteAnswer(remoteSdpAnswer: String) {
        if (released) return
        val pc = peerConnection ?: return
        val answerDesc = SessionDescription(SessionDescription.Type.ANSWER, remoteSdpAnswer)
        pc.setRemoteDescription(object : SimpleSdpObserver("setRemoteAnswer") {
            override fun onSetSuccess() {
                if (released) return
                drainPendingRemoteCandidates()
                refreshRemoteVideoTrackFromTransceivers()
            }
        }, answerDesc)
    }

    fun addRemoteIceCandidate(sdpMid: String, sdpMLineIndex: Int, sdp: String) {
        if (released || sdp.isBlank()) return
        val key = "$sdpMid|$sdpMLineIndex|$sdp"
        val candidate = IceCandidate(sdpMid, sdpMLineIndex, sdp)
        val shouldAddNow = synchronized(iceLock) {
            if (!addedCandidateKeys.add(key)) return
            if (!remoteDescriptionSet) {
                pendingRemoteCandidates.add(candidate)
                false
            } else {
                true
            }
        }
        if (shouldAddNow) {
            runCatching { peerConnection?.addIceCandidate(candidate) }
            _state.value = _state.value.copy(
                remoteCandidatesAdded = _state.value.remoteCandidatesAdded + 1
            )
        }
    }

    private fun drainPendingRemoteCandidates() {
        val pc = peerConnection ?: return
        val toAdd = synchronized(iceLock) {
            remoteDescriptionSet = true
            val snapshot = pendingRemoteCandidates.toList()
            pendingRemoteCandidates.clear()
            snapshot
        }
        var added = 0
        for (c in toAdd) {
            runCatching { pc.addIceCandidate(c) }
            added++
        }
        if (added > 0) {
            _state.value = _state.value.copy(
                remoteCandidatesAdded = _state.value.remoteCandidatesAdded + added
            )
        }
    }

    fun setMicMuted(muted: Boolean) {
        runCatching { localAudioTrack?.setEnabled(!muted) }
        runCatching { audioDeviceModule?.setMicrophoneMute(muted) }
    }

    fun setCameraEnabled(enabled: Boolean) {
        if (released || !isVideoCall) return
        desiredCameraEnabled = enabled
        runCatching { localVideoTrack?.setEnabled(enabled) }
        _state.value = _state.value.copy(isCameraEnabled = enabled)
        cameraExecutor.execute {
            if (released) return@execute
            if (enabled) {
                val capturer = videoCapturer
                if (capturer != null && !cameraCapturing) {
                    val gen = captureGeneration.incrementAndGet()
                    firstFrameSeenForCurrentSession = false
                    try {
                        capturer.startCapture(CAPTURE_WIDTH, CAPTURE_HEIGHT, CAPTURE_FPS)
                        cameraCapturing = true
                        scheduleFirstFrameWatchdog(gen)
                    } catch (e: Exception) {
                        Log.w(WEBRTC_TAG, "Restarting camera after re-enable: ${e.message}")
                        val enumerator = cameraEnumerator ?: createCameraEnumerator().also { cameraEnumerator = it }
                        val dev = pickCameraDevice(enumerator, desiredFrontCamera) ?: currentCameraDevice
                        if (dev != null) openAndStartCapturerOnCameraThread(dev)
                    }
                } else if (capturer == null) {
                    val enumerator = cameraEnumerator ?: createCameraEnumerator().also { cameraEnumerator = it }
                    val dev = pickCameraDevice(enumerator, desiredFrontCamera) ?: currentCameraDevice
                    if (dev != null) openAndStartCapturerOnCameraThread(dev)
                }
            } else {
                cancelFirstFrameWatchdog()
                if (cameraCapturing) {
                    runCatching { videoCapturer?.stopCapture() }
                    cameraCapturing = false
                }
            }
        }
    }

    fun switchCamera() {
        if (released || !isVideoCall || !desiredCameraEnabled) return
        if (!isSwitchingCamera.compareAndSet(false, true)) return

        _state.value = _state.value.copy(isSwitchingCamera = true)
        val targetFront = !desiredFrontCamera
        desiredFrontCamera = targetFront

        // Clean stop -> reopen on dedicated camera thread avoids Camera2 switchCamera HAL hangs across all OEMs
        cameraExecutor.execute {
            try {
                if (released) return@execute
                val enumerator = cameraEnumerator ?: createCameraEnumerator().also { cameraEnumerator = it }
                val targetDevice = pickCameraDevice(enumerator, preferFront = targetFront)
                if (targetDevice != null) {
                    openAndStartCapturerOnCameraThread(targetDevice)
                } else {
                    _state.value = _state.value.copy(isSwitchingCamera = false)
                }
            } finally {
                isSwitchingCamera.set(false)
                _state.value = _state.value.copy(isSwitchingCamera = false)
            }
        }
    }

    fun release() {
        if (released) return
        released = true
        cancelFirstFrameWatchdog()
        mainHandler.removeCallbacksAndMessages(null)
        _state.value = _state.value.copy(localVideoTrack = null, remoteVideoTrack = null, isSwitchingCamera = false)
        runCatching { cameraExecutor.shutdownNow() }
        runCatching { if (cameraCapturing) videoCapturer?.stopCapture() }
        cameraCapturing = false
        runCatching { videoCapturer?.dispose() }
        videoCapturer = null
        runCatching { localVideoTrack?.dispose() }
        localVideoTrack = null
        runCatching { videoSource?.dispose() }
        videoSource = null
        runCatching { surfaceTextureHelper?.dispose() }
        surfaceTextureHelper = null
        runCatching { localAudioTrack?.dispose() }
        localAudioTrack = null
        runCatching { audioSource?.dispose() }
        audioSource = null
        runCatching { peerConnection?.close() }
        runCatching { peerConnection?.dispose() }
        peerConnection = null
        runCatching { peerConnectionFactory?.dispose() }
        peerConnectionFactory = null
        runCatching { audioDeviceModule?.release() }
        audioDeviceModule = null
        runCatching { eglBase.release() }
    }

    private open class SimpleSdpObserver(private val label: String) : SdpObserver {
        override fun onCreateSuccess(desc: SessionDescription?) = Unit
        override fun onSetSuccess() = Unit
        override fun onCreateFailure(error: String?) {
            Log.e(WEBRTC_TAG, "$label createFailure: $error")
        }
        override fun onSetFailure(error: String?) {
            Log.e(WEBRTC_TAG, "$label setFailure: $error")
        }
    }
}
