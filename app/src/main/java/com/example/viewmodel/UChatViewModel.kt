package com.example.viewmodel

import android.content.Context
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.data.cache.LocalChatCache
import com.example.data.model.AppTimestamp
import com.example.data.model.BlockedUser
import com.example.data.model.CallRecord
import com.example.data.model.Channel
import com.example.data.model.ChannelPost
import com.example.data.model.ChatFilter
import com.example.data.model.ChatMessage
import com.example.data.model.ChatPref
import com.example.data.model.ContactEntry
import com.example.data.model.Conversation
import com.example.data.model.IceCandidateRecord
import com.example.data.model.LocalStorageSummary
import com.example.data.model.MainTab
import com.example.data.model.UiState
import com.example.data.model.UserProfile
import com.example.data.model.UserSettings
import com.example.data.repository.CallRepository
import com.example.data.repository.ChannelRepository
import com.example.data.repository.ChatRepository
import com.example.data.repository.ProfileRepository
import com.example.data.update.AppUpdateChecker
import com.example.data.update.AppUpdateState
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.UUID

private const val VM_TAG = "UChatVM"
private const val STOP_TIMEOUT_MS = 5000L

@OptIn(ExperimentalCoroutinesApi::class)
class UChatViewModel(
    val currentUserId: String,
    private val profileRepo: ProfileRepository,
    private val chatRepo: ChatRepository,
    private val channelRepo: ChannelRepository,
    private val callRepo: CallRepository,
    private val localCache: LocalChatCache? = null,
    private val updateChecker: AppUpdateChecker? = null
) : ViewModel() {

    private val _selectedTab = MutableStateFlow(MainTab.CHATS)
    val selectedTab: StateFlow<MainTab> = _selectedTab.asStateFlow()

    private val _chatFilter = MutableStateFlow(ChatFilter.ALL)
    val chatFilter: StateFlow<ChatFilter> = _chatFilter.asStateFlow()

    private val _chatSearchQuery = MutableStateFlow("")
    val chatSearchQuery: StateFlow<String> = _chatSearchQuery.asStateFlow()

    private val _channelSearchQuery = MutableStateFlow("")
    val channelSearchQuery: StateFlow<String> = _channelSearchQuery.asStateFlow()

    private val _activeConversationId = MutableStateFlow<String?>(null)
    val activeConversationId: StateFlow<String?> = _activeConversationId.asStateFlow()

    private val _activeChannelId = MutableStateFlow<String?>(null)
    val activeChannelId: StateFlow<String?> = _activeChannelId.asStateFlow()

    private val _statusBanner = MutableStateFlow<String?>(null)
    val statusBanner: StateFlow<String?> = _statusBanner.asStateFlow()

    private val _updateState = MutableStateFlow<AppUpdateState>(AppUpdateState.Idle)
    val updateState: StateFlow<AppUpdateState> = _updateState.asStateFlow()

    private val _configuredUpdateRepo = MutableStateFlow(updateChecker?.getConfiguredRepo().orEmpty())
    val configuredUpdateRepo: StateFlow<String> = _configuredUpdateRepo.asStateFlow()

    val currentVersionLabel: String
        get() = updateChecker?.let { "v${it.currentVersionName} (Build #${it.currentBuildNumber})" } ?: "v1.1"

    private val _messagePageLimit = MutableStateFlow(60)
    val messagePageLimit: StateFlow<Int> = _messagePageLimit.asStateFlow()

    private val _storageSummary = MutableStateFlow(LocalStorageSummary())
    val storageSummary: StateFlow<LocalStorageSummary> = _storageSummary.asStateFlow()

    private val _storageLoading = MutableStateFlow(false)
    val storageLoading: StateFlow<Boolean> = _storageLoading.asStateFlow()

    private fun <T> Flow<T>.toUiState(
        label: String,
        initial: UiState<T> = UiState.Loading,
        started: SharingStarted = SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS)
    ): StateFlow<UiState<T>> =
        this.map<T, UiState<T>> { UiState.Success(it) }
            .catch { err ->
                Log.w(VM_TAG, "Error observing $label", err)
                emit(UiState.Error(err.message ?: "Unable to sync $label"))
            }
            .stateIn(
                scope = viewModelScope,
                started = started,
                initialValue = initial
            )

    private val _profileRetryTick = MutableStateFlow(0)

    private val initialProfile: UiState<UserProfile?> =
        localCache?.getMyProfile(currentUserId)?.let { UiState.Success(it) } ?: UiState.Loading

    val myProfileState: StateFlow<UiState<UserProfile?>> =
        _profileRetryTick.flatMapLatest {
            profileRepo.observeMyProfile(currentUserId)
                .map<UserProfile?, UiState<UserProfile?>> { UiState.Success(it) }
                .catch { err ->
                    Log.w(VM_TAG, "Error observing profile", err)
                    emit(UiState.Error(err.message ?: "Unable to sync profile"))
                }
        }.stateIn(
            scope = viewModelScope,
            started = SharingStarted.Eagerly,
            initialValue = initialProfile
        )

    fun retryProfileSync() {
        _profileRetryTick.value += 1
    }

    private val initialDiscoverable: UiState<List<UserProfile>> =
        localCache?.getDiscoverableProfiles(currentUserId)?.let { UiState.Success(it) } ?: UiState.Loading

    val discoverableProfilesState: StateFlow<UiState<List<UserProfile>>> =
        _profileRetryTick.flatMapLatest { profileRepo.observeDiscoverableProfiles() }
            .toUiState("users", initial = initialDiscoverable)

    val settingsState: StateFlow<UserSettings> =
        _profileRetryTick.flatMapLatest {
            profileRepo.observeSettings(currentUserId)
                .catch { err ->
                    Log.w(VM_TAG, "Settings fallback", err)
                    emit(localCache?.getSettings(currentUserId) ?: UserSettings())
                }
        }.stateIn(
            viewModelScope,
            SharingStarted.Eagerly,
            localCache?.getSettings(currentUserId) ?: UserSettings()
        )

    val contactsState: StateFlow<List<ContactEntry>> =
        _profileRetryTick.flatMapLatest {
            profileRepo.observeContacts(currentUserId)
                .catch { emit(emptyList()) }
        }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), emptyList())

    val presenceByUidState: StateFlow<Map<String, UserProfile>> =
        combine(
            discoverableProfilesState.map { (it as? UiState.Success)?.data ?: emptyList() },
            contactsState.flatMapLatest { contacts ->
                profileRepo.observeProfilesByIds(contacts.map { it.contactUid })
                    .catch { emit(emptyMap()) }
            }
        ) { discoverable, contactProfiles ->
            buildMap {
                discoverable.forEach { put(it.userId, it) }
                putAll(contactProfiles)
            }
        }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), emptyMap())

    val presenceClockMs: StateFlow<Long> =
        flow {
            while (true) {
                emit(System.currentTimeMillis())
                delay(30_000L)
            }
        }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), System.currentTimeMillis())

    val blockedUsersState: StateFlow<List<BlockedUser>> =
        _profileRetryTick.flatMapLatest {
            profileRepo.observeBlockedUsers(currentUserId)
                .catch { emit(emptyList()) }
        }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), emptyList())

    private val initialConversations: UiState<List<Conversation>> =
        localCache?.getConversations(currentUserId)?.let { UiState.Success(it) } ?: UiState.Loading

    val conversationsState: StateFlow<UiState<List<Conversation>>> =
        _profileRetryTick.flatMapLatest { chatRepo.observeConversations(currentUserId) }
            .toUiState("conversations", initial = initialConversations, started = SharingStarted.Eagerly)

    val chatPrefsState: StateFlow<Map<String, ChatPref>> =
        _profileRetryTick.flatMapLatest {
            chatRepo.observeChatPrefs(currentUserId)
                .catch { emit(localCache?.getChatPrefs(currentUserId) ?: emptyMap()) }
        }.stateIn(
            viewModelScope,
            SharingStarted.Eagerly,
            localCache?.getChatPrefs(currentUserId) ?: emptyMap()
        )

    val activeConversationTotalMessages: StateFlow<Int> =
        _activeConversationId
            .flatMapLatest { convId ->
                if (convId.isNullOrBlank()) flowOf(0)
                else chatRepo.observeMessageCount(currentUserId, convId)
            }
            .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), 0)

    val activeMessagesState: StateFlow<UiState<List<ChatMessage>>> =
        combine(_activeConversationId, _messagePageLimit, _profileRetryTick) { convId, limit, _ ->
            convId to limit
        }.flatMapLatest { (convId, limit) ->
            if (convId.isNullOrBlank()) {
                flowOf(UiState.Success(emptyList()))
            } else {
                chatRepo.observeMessages(convId, currentUserId, limit)
                    .map<List<ChatMessage>, UiState<List<ChatMessage>>> { UiState.Success(it) }
                    .catch { err ->
                        Log.w(VM_TAG, "Error observing messages", err)
                        emit(UiState.Error(err.message ?: "Unable to load messages"))
                    }
            }
        }.stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS),
            initialValue = UiState.Loading
        )

    private val initialChannels: UiState<List<Channel>> =
        localCache?.getChannels(currentUserId)?.let { UiState.Success(it) } ?: UiState.Loading

    val channelsState: StateFlow<UiState<List<Channel>>> =
        _profileRetryTick.flatMapLatest { channelRepo.observePublicChannels() }
            .toUiState("channels", initial = initialChannels, started = SharingStarted.Eagerly)

    val followedChannelIdsState: StateFlow<Set<String>> =
        _profileRetryTick.flatMapLatest {
            channelRepo.observeFollowedChannels(currentUserId)
                .catch { emit(emptySet()) }
        }.stateIn(viewModelScope, SharingStarted.Eagerly, emptySet())

    val activeChannelPostsState: StateFlow<UiState<List<ChannelPost>>> =
        combine(_activeChannelId, _profileRetryTick) { chanId, _ -> chanId }
            .flatMapLatest { chanId ->
                if (chanId.isNullOrBlank()) flowOf(emptyList())
                else channelRepo.observeChannelPosts(chanId)
            }
            .toUiState("channel posts")

    private val initialCalls: UiState<List<CallRecord>> =
        localCache?.getCalls(currentUserId)?.let { UiState.Success(it) } ?: UiState.Loading

    val callsState: StateFlow<UiState<List<CallRecord>>> =
        _profileRetryTick.flatMapLatest { callRepo.observeCalls(currentUserId) }
            .toUiState("calls", initial = initialCalls, started = SharingStarted.Eagerly)

    init {
        viewModelScope.launch {
            profileRepo.setOnlineStatus(true)
        }
    }

    fun selectTab(tab: MainTab) {
        _selectedTab.value = tab
    }

    fun setChatFilter(filter: ChatFilter) {
        _chatFilter.value = filter
    }

    fun setChatSearchQuery(q: String) {
        _chatSearchQuery.value = q
    }

    fun setChannelSearchQuery(q: String) {
        _channelSearchQuery.value = q
    }

    fun openConversation(conversationId: String?) {
        _messagePageLimit.value = 60
        _activeConversationId.value = conversationId
        if (!conversationId.isNullOrBlank()) {
            viewModelScope.launch {
                chatRepo.updateChatPref(conversationId, unreadCount = 0L, hidden = false)
            }
        }
    }

    fun loadEarlierMessages() {
        _messagePageLimit.update { (it + 60).coerceAtMost(2000) }
    }

    fun openChannel(channelId: String?) {
        _activeChannelId.value = channelId
    }

    fun showStatus(message: String) {
        _statusBanner.value = message
    }

    fun clearStatus() {
        _statusBanner.value = null
    }

    fun validateUsername(raw: String): String? = profileRepo.validateUsernameFormat(raw)

    suspend fun checkUsernameAvailable(raw: String): Result<Boolean> =
        profileRepo.isUsernameAvailable(raw)

    suspend fun completeProfileSetup(
        username: String,
        displayName: String,
        about: String,
        avatarBase64: String?
    ): Result<Unit> = profileRepo.createInitialProfile(username, displayName, about, avatarBase64)

    fun updateMyProfile(
        currentUsername: String,
        newUsername: String,
        displayName: String,
        about: String,
        avatarBase64: String?,
        onDone: (Result<Unit>) -> Unit
    ) {
        viewModelScope.launch {
            val res = profileRepo.updateProfile(currentUsername, newUsername, displayName, about, avatarBase64)
            onDone(res)
        }
    }

    fun updatePrivacy(
        showLastSeen: Boolean,
        showOnline: Boolean,
        showAbout: Boolean,
        readReceipts: Boolean,
        discoverable: Boolean
    ) {
        viewModelScope.launch {
            profileRepo.updatePrivacy(showLastSeen, showOnline, showAbout, readReceipts, discoverable)
                .onFailure { showStatus(it.message ?: "Failed to save privacy settings") }
        }
    }

    fun updateSettings(updated: UserSettings) {
        viewModelScope.launch {
            profileRepo.updateSettings(updated)
                .onFailure { showStatus(it.message ?: "Failed to save settings") }
        }
    }

    fun setPresenceOnline(online: Boolean) {
        viewModelScope.launch {
            profileRepo.setOnlineStatus(online)
        }
    }

    suspend fun markOfflineBeforeSignOut() {
        profileRepo.setOnlineStatus(false)
        localCache?.clearUserCache(currentUserId)
    }

    fun clearUserLocalCache() {
        localCache?.clearUserCache(currentUserId)
    }

    fun checkForAppUpdate(silent: Boolean = false) {
        val checker = updateChecker ?: return
        if (_updateState.value is AppUpdateState.Checking) return
        if (silent && checker.getConfiguredRepo().isBlank()) return
        viewModelScope.launch {
            if (!silent) {
                _updateState.value = AppUpdateState.Checking
            }
            val result = checker.checkForUpdate()
            if (silent && result is AppUpdateState.Error) {
                // Keep UI quiet on background startup check if offline
                return@launch
            }
            _updateState.value = result
            if (!silent) {
                when (result) {
                    is AppUpdateState.UpdateAvailable ->
                        showStatus("Update available: Build #${result.release.latestBuildNumber}")
                    is AppUpdateState.UpToDate ->
                        showStatus("UChat is up to date")
                    else -> Unit
                }
            }
        }
    }

    fun saveUpdateRepo(rawInput: String): Result<String> {
        val checker = updateChecker ?: return Result.failure(IllegalStateException("Update checker unavailable"))
        val res = checker.saveCustomRepo(rawInput)
        res.onSuccess { saved ->
            _configuredUpdateRepo.value = saved
        }
        return res
    }

    fun openAppUpdateUrl(context: Context, url: String) {
        val checker = updateChecker ?: return
        checker.openUpdateUrl(context, url).onFailure { err ->
            showStatus(err.message ?: "Unable to open update link")
        }
    }

    fun addContact(profile: UserProfile) {
        viewModelScope.launch {
            profileRepo.addContact(profile)
                .onSuccess { showStatus("Added ${profile.displayName} to contacts") }
                .onFailure { showStatus(it.message ?: "Could not add contact") }
        }
    }

    fun blockUser(profile: UserProfile) {
        viewModelScope.launch {
            profileRepo.blockUser(profile)
                .onSuccess { showStatus("Blocked @${profile.username}") }
                .onFailure { showStatus(it.message ?: "Could not block user") }
        }
    }

    fun unblockUser(blockedUid: String) {
        viewModelScope.launch {
            profileRepo.unblockUser(blockedUid)
                .onSuccess { showStatus("User unblocked") }
                .onFailure { showStatus(it.message ?: "Could not unblock user") }
        }
    }

    fun submitReport(category: String, details: String, targetUid: String? = null) {
        viewModelScope.launch {
            profileRepo.submitReport(category, details, targetUid)
                .onSuccess { showStatus("Report submitted. Thank you.") }
                .onFailure { showStatus(it.message ?: "Could not submit report") }
        }
    }

    fun startDirectChat(peer: UserProfile) {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            chatRepo.getOrCreateDirectConversation(me, peer)
                .onSuccess { convId ->
                    _selectedTab.value = MainTab.CHATS
                    _activeConversationId.value = convId
                }
                .onFailure { showStatus(it.message ?: "Unable to start chat") }
        }
    }

    fun openSelfChat() {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            chatRepo.getOrCreateDirectConversation(me, me)
                .onSuccess { convId ->
                    _selectedTab.value = MainTab.CHATS
                    _activeConversationId.value = convId
                }
                .onFailure { showStatus(it.message ?: "Unable to open Saved Messages") }
        }
    }

    fun createGroup(
        title: String,
        description: String,
        avatarBase64: String?,
        members: List<UserProfile>,
        onCreated: () -> Unit
    ) {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            chatRepo.createGroupConversation(me, title, description, avatarBase64, members)
                .onSuccess { convId ->
                    _selectedTab.value = MainTab.CHATS
                    _activeConversationId.value = convId
                    onCreated()
                }
                .onFailure { showStatus(it.message ?: "Unable to create group") }
        }
    }

    fun updateGroup(
        conversation: Conversation,
        title: String,
        description: String,
        memberUids: List<String>,
        adminUids: List<String>,
        memberNames: Map<String, String>,
        memberUsernames: Map<String, String>
    ) {
        viewModelScope.launch {
            chatRepo.updateGroupInfo(
                conversation, title, description, memberUids, adminUids, memberNames, memberUsernames
            ).onSuccess {
                showStatus("Group updated")
            }.onFailure {
                showStatus(it.message ?: "Failed to update group")
            }
        }
    }

    fun sendMessage(
        conversation: Conversation,
        messageType: String = "text",
        text: String = "",
        mediaBase64: String? = null,
        mediaName: String? = null,
        mediaDurationSec: Int? = null,
        waveform: List<Int>? = null,
        latitude: Double? = null,
        longitude: Double? = null,
        contactName: String? = null,
        contactUsername: String? = null,
        replyTo: ChatMessage? = null,
        forwarded: Boolean = false
    ) {
        val me = (myProfileState.value as? UiState.Success)?.data
        val senderName = me?.displayName?.ifBlank { me.username } ?: "UChat User"
        val cleanText = text.trim().take(4000)
        if (messageType == "text" && cleanText.isBlank()) return

        viewModelScope.launch {
            val result = chatRepo.sendMessage(
                conversation = conversation,
                senderName = senderName,
                messageType = messageType,
                text = cleanText,
                mediaBase64 = mediaBase64,
                mediaName = mediaName,
                mediaDurationSec = mediaDurationSec,
                waveform = waveform,
                latitude = latitude,
                longitude = longitude,
                contactName = contactName,
                contactUsername = contactUsername,
                replyTo = replyTo,
                forwarded = forwarded
            )
            if (result.isFailure) {
                showStatus("Saved locally — will send when back online (tap to retry)")
            }
        }
    }

    fun retryMessage(messageId: String) {
        viewModelScope.launch {
            chatRepo.retryMessage(currentUserId, messageId)
                .onSuccess { showStatus("Message sent") }
                .onFailure { showStatus(it.message ?: "Still offline — queued locally") }
        }
    }

    fun redownloadMessageMedia(messageId: String) {
        viewModelScope.launch {
            chatRepo.redownloadMessageMedia(currentUserId, messageId)
                .onSuccess { showStatus("Media downloaded to device") }
                .onFailure { showStatus(it.message ?: "Media is no longer available on server") }
        }
    }

    suspend fun readUriBytesForAttachment(uri: android.net.Uri): Pair<String, ByteArray?> =
        chatRepo.mediaStore.readUriBytesForAttachment(uri)

    fun refreshStorageSummary() {
        viewModelScope.launch {
            _storageLoading.value = true
            val conversations = (conversationsState.value as? UiState.Success)?.data ?: emptyList()
            _storageSummary.value = chatRepo.computeStorageSummary(currentUserId, conversations)
            _storageLoading.value = false
        }
    }

    fun deleteLocalMediaItems(mediaIds: List<String>) {
        if (mediaIds.isEmpty()) return
        viewModelScope.launch {
            val count = chatRepo.deleteLocalMediaItems(currentUserId, mediaIds)
            refreshStorageSummary()
            if (count > 0) {
                showStatus(if (count == 1) "Deleted 1 media file (messages kept)" else "Deleted $count media files (messages kept)")
            }
        }
    }

    fun clearLocalMediaCategory(category: String, label: String) {
        viewModelScope.launch {
            val count = chatRepo.clearLocalMediaByCategory(currentUserId, category)
            refreshStorageSummary()
            showStatus(if (count > 0) "Cleared $count $label file(s) • Messages kept" else "No $label files to clear")
        }
    }

    fun clearLocalMediaConversation(conversationId: String, title: String) {
        viewModelScope.launch {
            val count = chatRepo.clearLocalMediaByConversation(currentUserId, conversationId)
            refreshStorageSummary()
            showStatus(if (count > 0) "Cleared $count media file(s) in $title • Messages kept" else "No media files in $title")
        }
    }

    fun clearAllDownloadedMedia() {
        viewModelScope.launch {
            val count = chatRepo.clearAllDownloadedMedia(currentUserId)
            refreshStorageSummary()
            showStatus(if (count > 0) "Cleared $count downloaded media files • Messages kept" else "No downloaded media files to clear")
        }
    }

    fun setTyping(conversationId: String, typing: Boolean) {
        viewModelScope.launch {
            chatRepo.setTyping(conversationId, typing)
        }
    }

    fun editMessage(conversationId: String, messageId: String, newText: String) {
        viewModelScope.launch {
            chatRepo.editMessage(conversationId, messageId, newText)
                .onFailure { showStatus(it.message ?: "Could not edit message") }
        }
    }

    fun deleteMessage(conversationId: String, messageId: String) {
        viewModelScope.launch {
            chatRepo.softDeleteMessage(conversationId, messageId)
                .onFailure { showStatus(it.message ?: "Could not delete message") }
        }
    }

    fun reactToMessage(conversationId: String, message: ChatMessage, emoji: String) {
        viewModelScope.launch {
            chatRepo.reactToMessage(conversationId, message, emoji)
        }
    }

    fun toggleStarMessage(conversationId: String, message: ChatMessage) {
        viewModelScope.launch {
            chatRepo.toggleStarMessage(conversationId, message)
        }
    }

    fun markConversationRead(conversationId: String, messages: List<ChatMessage>) {
        val me = (myProfileState.value as? UiState.Success)?.data
        val readReceipts = me?.readReceipts ?: true
        viewModelScope.launch {
            chatRepo.markMessagesDeliveredAndRead(conversationId, messages, readReceipts)
        }
    }

    fun togglePin(conversationId: String, current: Boolean) {
        viewModelScope.launch {
            chatRepo.updateChatPref(conversationId, pinned = !current)
        }
    }

    fun toggleArchive(conversationId: String, current: Boolean) {
        viewModelScope.launch {
            chatRepo.updateChatPref(conversationId, archived = !current)
        }
    }

    fun toggleMute(conversationId: String, current: Boolean) {
        viewModelScope.launch {
            chatRepo.updateChatPref(conversationId, muted = !current)
        }
    }

    fun hideConversation(conversationId: String) {
        viewModelScope.launch {
            chatRepo.updateChatPref(conversationId, hidden = true)
            if (_activeConversationId.value == conversationId) {
                _activeConversationId.value = null
            }
            showStatus("Conversation removed from your chat list")
        }
    }

    fun createChannel(name: String, description: String, avatarBase64: String?, onDone: () -> Unit) {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            channelRepo.createChannel(name, description, me.displayName.ifBlank { me.username }, avatarBase64)
                .onSuccess { chanId ->
                    _activeChannelId.value = chanId
                    onDone()
                }
                .onFailure { showStatus(it.message ?: "Unable to create channel") }
        }
    }

    fun toggleFollowChannel(channel: Channel, currentlyFollowing: Boolean) {
        viewModelScope.launch {
            channelRepo.toggleFollowChannel(channel, currentlyFollowing)
                .onFailure { showStatus(it.message ?: "Unable to update follow status") }
        }
    }

    fun publishChannelPost(channel: Channel, text: String, imageBase64: String?) {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            channelRepo.publishPost(channel, me.displayName.ifBlank { me.username }, text, imageBase64)
                .onFailure { showStatus(it.message ?: "Unable to publish post") }
        }
    }

    fun reactToChannelPost(channelId: String, post: ChannelPost, emoji: String) {
        viewModelScope.launch {
            channelRepo.reactToPost(channelId, post, emoji)
        }
    }

    fun observeCallCandidates(callId: String): Flow<List<IceCandidateRecord>> =
        callRepo.observeIceCandidates(callId, currentUserId)
            .catch {
                emit(emptyList())
            }

    fun initiateCall(peer: UserProfile, callType: String) {
        val me = (myProfileState.value as? UiState.Success)?.data ?: return
        if (peer.userId == currentUserId) {
            showStatus("Cannot start a call with yourself")
            return
        }
        viewModelScope.launch {
            callRepo.startCall(me, peer, callType)
                .onFailure { showStatus(it.message ?: "Unable to place call") }
        }
    }

    fun publishSdpOffer(callId: String, sdpOffer: String) {
        viewModelScope.launch {
            callRepo.publishSdpOffer(callId, sdpOffer)
                .onFailure { showStatus(it.message ?: "Unable to send WebRTC offer") }
        }
    }

    fun acceptCall(call: CallRecord, sdpAnswer: String) {
        viewModelScope.launch {
            callRepo.acceptCall(call, sdpAnswer)
                .onFailure { showStatus(it.message ?: "Unable to send WebRTC answer") }
        }
    }

    fun markCallConnected(callId: String) {
        viewModelScope.launch {
            callRepo.markCallConnected(callId)
        }
    }

    fun sendIceCandidate(call: CallRecord, sdp: String, sdpMid: String, sdpMLineIndex: Int) {
        viewModelScope.launch {
            callRepo.addIceCandidate(call, sdp, sdpMid, sdpMLineIndex)
        }
    }

    fun finishCall(callId: String, status: String, durationSec: Long) {
        viewModelScope.launch {
            callRepo.finishCall(callId, status, durationSec)
        }
    }

    fun deleteSelectedCallLogs(callIds: Set<String>) {
        if (callIds.isEmpty()) return
        val calls = (callsState.value as? UiState.Success)?.data ?: return
        val targets = calls.filter { it.id in callIds }
        if (targets.isEmpty()) return
        viewModelScope.launch {
            callRepo.deleteCallLogs(targets)
                .onSuccess { deleted ->
                    if (deleted > 0) {
                        showStatus(if (deleted == 1) "Call log deleted" else "$deleted call logs deleted")
                    }
                }
                .onFailure { showStatus(it.message ?: "Could not delete selected call logs") }
        }
    }

    fun clearCallHistory() {
        val calls = (callsState.value as? UiState.Success)?.data ?: return
        viewModelScope.launch {
            callRepo.clearCallHistory(calls)
                .onSuccess { showStatus("Call history cleared") }
                .onFailure { showStatus(it.message ?: "Could not clear call history") }
        }
    }
}
