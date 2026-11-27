package com.example.data.repository

import android.util.Base64
import android.util.Log
import com.example.data.cache.LocalChatCache
import com.example.data.local.LocalMappers.toEntity
import com.example.data.local.LocalMappers.toModel
import com.example.data.local.LocalMediaStore
import com.example.data.local.MessageEntity
import com.example.data.local.UChatDatabase
import com.example.data.model.AppTimestamp
import com.example.data.model.ChatMessage
import com.example.data.model.ChatPref
import com.example.data.model.Conversation
import com.example.data.model.LocalStorageSummary
import com.example.data.model.UserProfile
import com.example.data.supabase.SupabaseClient
import com.example.data.supabase.SupabaseClient.Companion.jsonArrayToLongList
import com.example.data.supabase.SupabaseClient.Companion.jsonArrayToStringList
import com.example.data.supabase.SupabaseClient.Companion.jsonObjectToStringMap
import com.example.data.supabase.SupabaseClient.Companion.listToJsonArray
import com.example.data.supabase.SupabaseClient.Companion.mapToJsonObject
import com.example.data.supabase.SupabaseClient.Companion.optNullableString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

private const val CHAT_REPO_TAG = "ChatRepository"
// Conservative multi-device retention windows before pruning temporary server copies
// AFTER all intended recipients have acknowledged local persistence.
private const val SERVER_MEDIA_RETENTION_GRACE_MS = 24L * 60L * 60L * 1000L
private const val SERVER_TEXT_RETENTION_GRACE_MS = 72L * 60L * 60L * 1000L
private const val PRUNE_COOLDOWN_MS = 15L * 60L * 1000L

class ChatRepository(
    private val supabase: SupabaseClient,
    private val cache: LocalChatCache? = null,
    private val database: UChatDatabase,
    val mediaStore: LocalMediaStore
) {
    private val conversationDao = database.conversationDao()
    private val messageDao = database.messageDao()
    private val migratedUsers = ConcurrentHashMap.newKeySet<String>()
    private val lastPruneByConversation = ConcurrentHashMap<String, Long>()
    private val activeMediaDownloads = ConcurrentHashMap.newKeySet<String>()

    private fun requireUserId(): String = supabase.requireUserId()

    private fun nowIso(epochMillis: Long = System.currentTimeMillis()): String =
        SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US).apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }.format(Date(epochMillis))

    /**
     * One-time non-destructive migration of any pre-existing JSON cache into Room SQLite
     * so upgrading users keep their locally cached conversations and messages intact.
     */
    private suspend fun ensureLegacyCacheMigrated(userId: String) {
        if (!migratedUsers.add(userId)) return
        withContext(Dispatchers.IO) {
            runCatching {
                val existingCount = conversationDao.countConversations(userId)
                if (existingCount == 0 && cache != null) {
                    val cachedConvs = cache.getConversations(userId).orEmpty()
                    if (cachedConvs.isNotEmpty()) {
                        conversationDao.upsertAll(cachedConvs.map { it.toEntity(userId) })
                        cachedConvs.forEach { conv ->
                            val cachedMsgs = cache.getMessages(userId, conv.id).orEmpty()
                            if (cachedMsgs.isNotEmpty()) {
                                messageDao.upsertAll(cachedMsgs.map { it.toEntity(userId) })
                            }
                        }
                    }
                }
            }.onFailure {
                Log.w(CHAT_REPO_TAG, "Legacy cache migration skipped: ${it.message}")
            }
        }
    }

    /**
     * Local-first conversation stream backed by Room SQLite with background Supabase Realtime sync.
     * Works seamlessly offline and instantaneously on startup.
     */
    fun observeConversations(userId: String): Flow<List<Conversation>> = channelFlow {
        ensureLegacyCacheMigrated(userId)

        val localJob = launch(Dispatchers.IO) {
            conversationDao.observeConversations(userId)
                .map { entities -> entities.map { it.toModel() } }
                .collectLatest { send(it) }
        }

        val remoteJob = launch(Dispatchers.IO) {
            try {
                supabase.observeTable(
                    tables = setOf("conversations"),
                    fallbackPollMs = 8_000L
                ) {
                    val rows = supabase.restGet(
                        table = "conversations",
                        queryParams = listOf(
                            "member_uids" to "cs.{\"$userId\"}",
                            "order" to "last_message_at.desc"
                        )
                    )
                    List(rows.length()) { idx -> parseConversation(rows.getJSONObject(idx)) }
                }.collect { remoteConversations ->
                    val merged = remoteConversations.map { remote ->
                        val local = conversationDao.getById(userId, remote.id)
                        val remoteEntity = remote.toEntity(userId)
                        if (local != null && local.lastMessageAtMs > remoteEntity.lastMessageAtMs) {
                            remoteEntity.copy(
                                lastMessageText = local.lastMessageText,
                                lastMessageType = local.lastMessageType,
                                lastMessageSenderId = local.lastMessageSenderId,
                                lastMessageAtMs = local.lastMessageAtMs
                            )
                        } else {
                            remoteEntity
                        }
                    }
                    if (merged.isNotEmpty()) {
                        conversationDao.upsertAll(merged)
                    }
                    flushPendingMessages(userId)
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (e: Exception) {
                // Stay resilient offline: Room continues serving local history uninterrupted
                Log.w(CHAT_REPO_TAG, "Conversations background sync paused: ${e.message}")
            }
        }

        localJob.join()
        remoteJob.cancel()
    }

    fun observeChatPrefs(userId: String): Flow<Map<String, ChatPref>> =
        supabase.observeTable(
            tables = setOf("chat_prefs"),
            fallbackPollMs = 15_000L,
            initialCached = { cache?.getChatPrefs(userId) }
        ) {
            val rows = supabase.restGet(
                table = "chat_prefs",
                queryParams = listOf("owner_id" to "eq.$userId")
            )
            buildMap {
                for (i in 0 until rows.length()) {
                    val pref = parseChatPref(rows.getJSONObject(i))
                    if (pref.conversationId.isNotBlank()) put(pref.conversationId, pref)
                }
            }.also { cache?.putChatPrefs(userId, it) }
        }

    fun observeMessageCount(userId: String, conversationId: String): Flow<Int> =
        messageDao.observeMessageCount(userId, conversationId)

    /**
     * Local-first paginated message stream from Room SQLite with background Supabase sync,
     * automatic local media download & deduplication, delivery acknowledgements, and
     * conservative post-delivery temporary server cleanup.
     */
    fun observeMessages(
        conversationId: String,
        userId: String,
        limit: Int = 60
    ): Flow<List<ChatMessage>> = channelFlow {
        ensureLegacyCacheMigrated(userId)

        val localJob = launch(Dispatchers.IO) {
            messageDao.observeRecentMessages(userId, conversationId, limit)
                .map { entities -> entities.map { it.toModel() } }
                .collectLatest { send(it) }
        }

        val remoteJob = launch(Dispatchers.IO) {
            try {
                supabase.observeTable(
                    tables = setOf("messages"),
                    fallbackPollMs = 4_000L
                ) {
                    val fetchLimit = limit.coerceIn(60, 300)
                    val rows = supabase.restGet(
                        table = "messages",
                        queryParams = listOf(
                            "conversation_id" to "eq.$conversationId",
                            "member_uids" to "cs.{\"$userId\"}",
                            "order" to "created_at.desc",
                            "limit" to "$fetchLimit"
                        )
                    )
                    List(rows.length()) { idx -> parseMessage(rows.getJSONObject(idx)) }.asReversed()
                }.collect { remoteMessages ->
                    if (remoteMessages.isNotEmpty()) {
                        val toUpsert = mutableListOf<MessageEntity>()
                        for (msg in remoteMessages) {
                            if (msg.id.isBlank()) continue
                            val existing = messageDao.getById(userId, msg.id)
                            toUpsert.add(msg.toEntity(userId, existing = existing))
                        }
                        if (toUpsert.isNotEmpty()) {
                            messageDao.upsertAll(toUpsert)
                        }

                        // Cache incoming media locally & avoid repeated downloads
                        toUpsert.forEach { entity ->
                            if (LocalMediaStore.isMediaMessageType(entity.messageType) &&
                                !entity.deleted &&
                                !entity.mediaClearedLocally &&
                                !mediaStore.isLocalFileValid(entity.localMediaPath, entity.messageType == "image") &&
                                !entity.remoteMediaUrl.isNullOrBlank() &&
                                activeMediaDownloads.add(entity.id)
                            ) {
                                launch(Dispatchers.IO) {
                                    try {
                                        mediaStore.ensureMediaCachedLocally(userId, entity)
                                    } finally {
                                        activeMediaDownloads.remove(entity.id)
                                    }
                                }
                            }
                        }

                        // Acknowledge delivery for incoming messages once persisted locally in Room
                        val undeliveredIncoming = remoteMessages.filter { msg ->
                            msg.senderId != userId && userId !in msg.deliveredTo
                        }
                        if (undeliveredIncoming.isNotEmpty()) {
                            launch(Dispatchers.IO) {
                                acknowledgeLocalDelivery(conversationId, userId, undeliveredIncoming)
                            }
                        }

                        // Conservatively prune eligible temporary server copies after multi-device grace window
                        launch(Dispatchers.IO) {
                            pruneDeliveredServerCopiesIfEligible(userId, conversationId)
                        }
                    }
                    flushPendingMessages(userId)
                }
            } catch (ce: CancellationException) {
                throw ce
            } catch (e: Exception) {
                // Offline mode: Room continues serving complete local message history
                Log.w(CHAT_REPO_TAG, "Message sync paused offline: ${e.message}")
            }
        }

        localJob.join()
        remoteJob.cancel()
    }

    private suspend fun acknowledgeLocalDelivery(
        conversationId: String,
        userId: String,
        messages: List<ChatMessage>
    ) {
        val now = nowIso()
        messages.take(25).forEach { msg ->
            runCatching {
                val updatedDelivered = (msg.deliveredTo + userId).distinct().take(50)
                val body = JSONObject().apply {
                    put("delivered_to", listToJsonArray(updatedDelivered))
                    put("updated_at", now)
                }
                supabase.restPatch(
                    "messages",
                    listOf("id" to "eq.${msg.id}", "conversation_id" to "eq.$conversationId"),
                    body
                )
            }
        }
    }

    /**
     * Conservative temporary server retention cleanup:
     * Only deletes a sender's server-side temporary copy from Supabase when:
     * 1. It is a multi-user conversation (never auto-prune Saved Messages self-chat),
     * 2. ALL intended recipients in memberUids have acknowledged local persistence (delivered_to),
     * 3. The message is safely persisted in local Room storage, AND
     * 4. The multi-device sync grace window has elapsed (24h media / 72h text) so secondary devices can sync.
     * Local Room history and local device media files are NEVER deleted by this cleanup.
     */
    private suspend fun pruneDeliveredServerCopiesIfEligible(userId: String, conversationId: String) {
        val nowMs = System.currentTimeMillis()
        val lastRun = lastPruneByConversation[conversationId] ?: 0L
        if (nowMs - lastRun < PRUNE_COOLDOWN_MS) return
        lastPruneByConversation[conversationId] = nowMs

        val candidates = messageDao.getSentUnprunedMessages(userId, conversationId)
        for (entity in candidates.take(15)) {
            val members = runCatching { jsonArrayToStringList(JSONArray(entity.memberUidsJson)) }
                .getOrDefault(emptyList())
                .distinct()
            if (members.size <= 1) continue // Preserve Saved Messages self-chat for cross-device notes

            val delivered = runCatching { jsonArrayToStringList(JSONArray(entity.deliveredToJson)) }
                .getOrDefault(emptyList())
                .toSet()
            val allRecipientsDelivered = members.all { it in delivered }
            if (!allRecipientsDelivered) continue // Never prune while any recipient is still offline

            val ageMs = nowMs - entity.createdAtMs
            val hasMedia = LocalMediaStore.isMediaMessageType(entity.messageType) && !entity.remoteMediaUrl.isNullOrBlank()
            val requiredGrace = if (hasMedia) SERVER_MEDIA_RETENTION_GRACE_MS else SERVER_TEXT_RETENTION_GRACE_MS
            if (ageMs < requiredGrace) continue

            // Confirm local copy exists before cleaning up temporary server copy
            val localMediaReady = !hasMedia || entity.mediaClearedLocally ||
                mediaStore.isLocalFileValid(entity.localMediaPath, entity.messageType == "image")
            if (!localMediaReady) continue

            var storageCleaned = true
            if (hasMedia && entity.remoteMediaUrl?.startsWith("http") == true) {
                storageCleaned = supabase.deleteStorageObjectByPublicUrl(entity.remoteMediaUrl)
            }
            if (storageCleaned) {
                runCatching {
                    supabase.restDelete(
                        "messages",
                        listOf("id" to "eq.${entity.id}", "sender_id" to "eq.$userId")
                    )
                    messageDao.markServerPruned(userId, entity.id)
                }
            }
        }
    }

    suspend fun getOrCreateDirectConversation(
        myProfile: UserProfile,
        peerProfile: UserProfile
    ): Result<String> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        val sortedIds = listOf(uid, peerProfile.userId).sorted()
        val convId = "dm_${sortedIds[0].take(20)}_${sortedIds[1].take(20)}"
        return try {
            val isSelf = uid == peerProfile.userId
            val members = if (isSelf) listOf(uid) else listOf(uid, peerProfile.userId)
            val memberNames = if (isSelf) {
                mapOf(uid to myProfile.displayName.ifBlank { myProfile.username })
            } else {
                mapOf(
                    uid to myProfile.displayName.ifBlank { myProfile.username },
                    peerProfile.userId to peerProfile.displayName.ifBlank { peerProfile.username }
                )
            }
            val memberUsernames = if (isSelf) {
                mapOf(uid to myProfile.username)
            } else {
                mapOf(
                    uid to myProfile.username,
                    peerProfile.userId to peerProfile.username
                )
            }
            val nowTs = AppTimestamp.now()
            val localConv = Conversation(
                id = convId,
                kind = "direct",
                memberUids = members,
                adminUids = listOf(uid),
                title = if (isSelf) "Saved Messages (You)" else "",
                description = if (isSelf) "Personal notes & saved messages" else "",
                memberNames = memberNames,
                memberUsernames = memberUsernames,
                lastMessageText = "",
                lastMessageType = "text",
                lastMessageSenderId = uid,
                lastMessageAt = nowTs,
                createdBy = uid,
                createdAt = nowTs,
                updatedAt = nowTs
            )
            if (conversationDao.getById(uid, convId) == null) {
                conversationDao.upsert(localConv.toEntity(uid))
            }

            val existing = supabase.restGet(
                table = "conversations",
                queryParams = listOf("id" to "eq.$convId", "limit" to "1")
            )
            if (existing.length() == 0) {
                val now = nowIso()
                val payload = JSONObject().apply {
                    put("id", convId)
                    put("kind", "direct")
                    put("member_uids", listToJsonArray(members))
                    put("admin_uids", listToJsonArray(listOf(uid)))
                    put("title", if (isSelf) "Saved Messages (You)" else "")
                    put("description", if (isSelf) "Personal notes & saved messages" else "")
                    put("member_names", mapToJsonObject(memberNames))
                    put("member_usernames", mapToJsonObject(memberUsernames))
                    put("last_message_text", "")
                    put("last_message_type", "text")
                    put("last_message_sender_id", uid)
                    put("last_message_at", now)
                    put("created_by", uid)
                    put("updated_at", now)
                }
                supabase.restPost("conversations", payload, upsert = true, onConflict = "id")
            } else {
                val remoteConv = parseConversation(existing.getJSONObject(0))
                conversationDao.upsert(remoteConv.toEntity(uid))
                updateChatPref(convId, hidden = false, archived = if (isSelf) false else null)
            }
            Result.success(convId)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun createGroupConversation(
        myProfile: UserProfile,
        title: String,
        description: String,
        avatarBase64: String?,
        members: List<UserProfile>
    ): Result<String> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        val cleanTitle = title.trim().take(80)
        if (cleanTitle.isBlank()) return Result.failure(IllegalArgumentException("Group name is required."))
        val allProfiles = (listOf(myProfile) + members).distinctBy { it.userId }.take(50)
        val convId = "grp_${UUID.randomUUID()}"
        return try {
            val storedAvatar = supabase.uploadBase64ToStorageOrFallback(
                ownerUid = uid,
                folder = "groups",
                mediaValue = avatarBase64,
                extension = "jpg",
                contentType = "image/jpeg"
            )
            val nowMs = System.currentTimeMillis()
            val now = nowIso(nowMs)
            val memberUids = allProfiles.map { it.userId }
            val memberNames = allProfiles.associate { it.userId to it.displayName.ifBlank { it.username } }
            val memberUsernames = allProfiles.associate { it.userId to it.username }
            val createdMsg = "${myProfile.displayName} created group \"$cleanTitle\""
            val payload = JSONObject().apply {
                put("id", convId)
                put("kind", "group")
                put("member_uids", listToJsonArray(memberUids))
                put("admin_uids", listToJsonArray(listOf(uid)))
                put("title", cleanTitle)
                put("description", description.trim().take(300))
                if (!storedAvatar.isNullOrBlank()) put("avatar_url", storedAvatar)
                put("member_names", mapToJsonObject(memberNames))
                put("member_usernames", mapToJsonObject(memberUsernames))
                put("last_message_text", createdMsg)
                put("last_message_type", "text")
                put("last_message_sender_id", uid)
                put("last_message_at", now)
                put("created_by", uid)
                put("updated_at", now)
            }
            supabase.restPost("conversations", payload)
            conversationDao.upsert(
                Conversation(
                    id = convId,
                    kind = "group",
                    memberUids = memberUids,
                    adminUids = listOf(uid),
                    title = cleanTitle,
                    description = description.trim().take(300),
                    avatarBase64 = storedAvatar,
                    memberNames = memberNames,
                    memberUsernames = memberUsernames,
                    lastMessageText = createdMsg,
                    lastMessageType = "text",
                    lastMessageSenderId = uid,
                    lastMessageAt = AppTimestamp(nowMs),
                    createdBy = uid,
                    createdAt = AppTimestamp(nowMs),
                    updatedAt = AppTimestamp(nowMs)
                ).toEntity(uid)
            )
            Result.success(convId)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun updateGroupInfo(
        conversation: Conversation,
        title: String,
        description: String,
        memberUids: List<String>,
        adminUids: List<String>,
        memberNames: Map<String, String>,
        memberUsernames: Map<String, String>
    ): Result<Unit> {
        return try {
            val uid = requireUserId()
            val cleanTitle = title.trim().take(80).ifBlank { "Group Chat" }
            val cleanDesc = description.trim().take(300)
            val body = JSONObject().apply {
                put("title", cleanTitle)
                put("description", cleanDesc)
                put("member_uids", listToJsonArray(memberUids.distinct().take(50)))
                put("admin_uids", listToJsonArray(adminUids.distinct().take(20)))
                put("member_names", mapToJsonObject(memberNames))
                put("member_usernames", mapToJsonObject(memberUsernames))
                put("updated_at", nowIso())
            }
            supabase.restPatch("conversations", listOf("id" to "eq.${conversation.id}"), body)
            conversationDao.upsert(
                conversation.copy(
                    title = cleanTitle,
                    description = cleanDesc,
                    memberUids = memberUids.distinct().take(50),
                    adminUids = adminUids.distinct().take(20),
                    memberNames = memberNames,
                    memberUsernames = memberUsernames,
                    updatedAt = AppTimestamp.now()
                ).toEntity(uid)
            )
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    /**
     * Local-first outgoing send:
     * 1. Persists outgoing message + local media file into Room immediately (`syncStatus = "pending"`).
     * 2. Deduplicates media upload by content hash if already uploaded.
     * 3. Sends idempotently to Supabase with explicit client UUID (`onConflict = "id"`).
     * 4. Marks `"sent"` on success, or `"failed"` on offline error for later automatic/manual retry.
     */
    suspend fun sendMessage(
        conversation: Conversation,
        senderName: String,
        messageType: String,
        text: String,
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
    ): Result<Unit> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        val cleanText = text.trim().take(4000)
        if (messageType == "text" && cleanText.isBlank()) {
            return Result.failure(IllegalArgumentException("Message cannot be empty."))
        }
        if (mediaBase64 != null && mediaBase64.length > 11_000_000) {
            return Result.failure(IllegalArgumentException("Attachment exceeds 8MB upload limit."))
        }

        val messageId = UUID.randomUUID().toString()
        val nowMs = System.currentTimeMillis()
        val cleanSenderName = senderName.trim().take(60).ifBlank { "UChat User" }
        val preview = buildConversationPreview(messageType, cleanText, mediaName, mediaDurationSec, contactName)

        // 1. Save outgoing message locally in Room immediately
        val initialEntity = MessageEntity(
            ownerUid = uid,
            id = messageId,
            conversationId = conversation.id,
            senderId = uid,
            senderName = cleanSenderName,
            memberUidsJson = listToJsonArray(conversation.memberUids).toString(),
            messageType = messageType,
            text = cleanText,
            remoteMediaUrl = mediaBase64?.takeIf { it.startsWith("https://") || it.startsWith("http://") },
            localMediaPath = null,
            mediaName = mediaName?.take(160),
            mediaMimeType = null,
            mediaSizeBytes = null,
            mediaDurationSec = mediaDurationSec?.coerceIn(0, 3600)?.toLong(),
            waveformJson = waveform?.take(64)?.let { listToJsonArray(it).toString() },
            latitude = latitude,
            longitude = longitude,
            contactName = contactName?.take(80),
            contactUsername = contactUsername?.take(30),
            replyToId = replyTo?.id?.take(128),
            replyToText = replyTo?.let { it.text.ifBlank { "[${it.messageType}]" }.take(200) },
            replyToSender = replyTo?.senderName?.take(60),
            forwarded = forwarded,
            edited = false,
            deleted = false,
            deliveredToJson = listToJsonArray(listOf(uid)).toString(),
            readByJson = listToJsonArray(listOf(uid)).toString(),
            starredByJson = "[]",
            reactionsJson = "{}",
            syncStatus = "pending",
            mediaClearedLocally = false,
            serverPruned = false,
            createdAtMs = nowMs,
            updatedAtMs = nowMs,
            persistedAtMs = nowMs
        )
        messageDao.upsert(initialEntity)
        if (conversationDao.getById(uid, conversation.id) == null) {
            conversationDao.upsert(conversation.toEntity(uid))
        }
        conversationDao.updateLastMessage(uid, conversation.id, preview, messageType, uid, nowMs)

        // 2. Save outgoing media locally into app-private internal storage immediately
        val savedLocalMedia = if (!mediaBase64.isNullOrBlank() && LocalMediaStore.isMediaMessageType(messageType)) {
            mediaStore.saveOutgoingMedia(
                ownerUid = uid,
                conversationId = conversation.id,
                messageId = messageId,
                messageType = messageType,
                mediaValue = mediaBase64,
                fileName = mediaName
            )
        } else {
            null
        }

        // 3. Dispatch to Supabase (reusing uploaded media URL if identical content already uploaded)
        val refreshedEntity = messageDao.getById(uid, messageId) ?: initialEntity.copy(
            localMediaPath = savedLocalMedia?.localPath,
            mediaSizeBytes = savedLocalMedia?.sizeBytes
        )
        return syncMessageEntityToSupabase(uid, refreshedEntity, inlineMediaBase64 = mediaBase64)
    }

    suspend fun retryMessage(userId: String, messageId: String): Result<Unit> {
        val entity = messageDao.getById(userId, messageId)
            ?: return Result.failure(IllegalStateException("Message not found locally."))
        messageDao.updateSyncState(userId, messageId, "pending", entity.remoteMediaUrl, System.currentTimeMillis())
        return syncMessageEntityToSupabase(userId, entity.copy(syncStatus = "pending"), inlineMediaBase64 = null)
    }

    suspend fun flushPendingMessages(userId: String) {
        val pending = withContext(Dispatchers.IO) { messageDao.getPendingOrFailedMessages(userId) }
        for (entity in pending.take(10)) {
            if (entity.syncStatus == "pending" || (System.currentTimeMillis() - entity.updatedAtMs) > 8_000L) {
                syncMessageEntityToSupabase(userId, entity, inlineMediaBase64 = null)
            }
        }
    }

    private suspend fun syncMessageEntityToSupabase(
        uid: String,
        entity: MessageEntity,
        inlineMediaBase64: String?
    ): Result<Unit> {
        return try {
            var storedMediaUrl = entity.remoteMediaUrl?.takeIf {
                it.startsWith("https://") || it.startsWith("http://")
            }
            if (storedMediaUrl == null && LocalMediaStore.isMediaMessageType(entity.messageType)) {
                val base64Source = inlineMediaBase64 ?: entity.localMediaPath?.let { path ->
                    withContext(Dispatchers.IO) {
                        val file = File(path)
                        if (file.exists() && file.length() > 0L) {
                            Base64.encodeToString(file.readBytes(), Base64.NO_WRAP)
                        } else null
                    }
                }
                if (!base64Source.isNullOrBlank()) {
                    // Avoid re-uploading identical media if already uploaded
                    storedMediaUrl = mediaStore.findReusableRemoteUrlForBase64(uid, base64Source)
                        ?: run {
                            val category = LocalMediaStore.categoryForMessageType(entity.messageType)
                            val ext = LocalMediaStore.defaultExtensionForCategory(category, entity.mediaName)
                            val mime = LocalMediaStore.defaultMimeForCategory(category)
                            supabase.uploadBase64ToStorageOrFallback(
                                ownerUid = uid,
                                folder = category,
                                mediaValue = base64Source,
                                extension = ext,
                                contentType = mime
                            )
                        }
                }
            }

            val nowMs = System.currentTimeMillis()
            val nowStr = nowIso(nowMs)
            val members = runCatching { jsonArrayToStringList(JSONArray(entity.memberUidsJson)) }
                .getOrDefault(listOf(uid))
            val waveform = entity.waveformJson?.let {
                runCatching { jsonArrayToLongList(JSONArray(it)) }.getOrNull()
            }

            val msgPayload = JSONObject().apply {
                put("id", entity.id)
                put("conversation_id", entity.conversationId)
                put("sender_id", uid)
                put("sender_name", entity.senderName)
                put("member_uids", listToJsonArray(members))
                put("message_type", entity.messageType)
                put("text", entity.text)
                if (!storedMediaUrl.isNullOrBlank()) put("media_url", storedMediaUrl)
                if (!entity.mediaName.isNullOrBlank()) put("media_name", entity.mediaName)
                if (entity.mediaDurationSec != null) put("media_duration_sec", entity.mediaDurationSec)
                if (!waveform.isNullOrEmpty()) put("waveform", listToJsonArray(waveform))
                if (entity.latitude != null) put("latitude", entity.latitude)
                if (entity.longitude != null) put("longitude", entity.longitude)
                if (!entity.contactName.isNullOrBlank()) put("contact_name", entity.contactName)
                if (!entity.contactUsername.isNullOrBlank()) put("contact_username", entity.contactUsername)
                if (!entity.replyToId.isNullOrBlank()) {
                    put("reply_to_id", entity.replyToId)
                    put("reply_to_text", entity.replyToText.orEmpty())
                    put("reply_to_sender", entity.replyToSender.orEmpty())
                }
                put("forwarded", entity.forwarded)
                put("edited", entity.edited)
                put("deleted", entity.deleted)
                put("delivered_to", listToJsonArray(listOf(uid)))
                put("read_by", listToJsonArray(listOf(uid)))
                put("starred_by", listToJsonArray(emptyList()))
                put("reactions", JSONObject())
                put("created_at", nowIso(entity.createdAtMs))
                put("updated_at", nowStr)
            }
            supabase.restPost("messages", msgPayload, upsert = true, onConflict = "id")

            messageDao.updateSyncState(
                ownerUid = uid,
                messageId = entity.id,
                syncStatus = "sent",
                remoteUrl = storedMediaUrl,
                updatedAtMs = nowMs
            )

            val preview = buildConversationPreview(
                entity.messageType,
                entity.text,
                entity.mediaName,
                entity.mediaDurationSec?.toInt(),
                entity.contactName
            )
            val convUpdate = JSONObject().apply {
                put("last_message_text", preview)
                put("last_message_type", entity.messageType)
                put("last_message_sender_id", uid)
                put("last_message_at", nowStr)
                put("typing_uid", "")
                put("updated_at", nowStr)
            }
            runCatching {
                supabase.restPatch("conversations", listOf("id" to "eq.${entity.conversationId}"), convUpdate)
            }
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            messageDao.updateSyncState(
                ownerUid = uid,
                messageId = entity.id,
                syncStatus = "failed",
                remoteUrl = entity.remoteMediaUrl,
                updatedAtMs = System.currentTimeMillis()
            )
            Result.failure(e)
        }
    }

    private fun buildConversationPreview(
        messageType: String,
        cleanText: String,
        mediaName: String?,
        mediaDurationSec: Int?,
        contactName: String?
    ): String = when (messageType) {
        "image" -> if (cleanText.isNotBlank()) "📷 $cleanText" else "📷 Photo"
        "video" -> if (cleanText.isNotBlank()) "🎬 $cleanText" else "🎬 Video"
        "voice" -> "🎤 Voice message (${mediaDurationSec ?: 0}s)"
        "document" -> "📄 ${mediaName ?: "Document"}"
        "audio" -> "🎵 ${mediaName ?: "Audio clip"}"
        "location" -> "📍 Shared location"
        "contact" -> "👤 Contact: ${contactName ?: "UChat User"}"
        else -> cleanText
    }.take(300)

    suspend fun setTyping(conversationId: String, typing: Boolean): Result<Unit> {
        val uid = supabase.currentUserId() ?: return Result.success(Unit)
        return try {
            val now = nowIso()
            val body = JSONObject().apply {
                put("typing_uid", if (typing) uid else "")
                put("typing_at", now)
                put("updated_at", now)
            }
            supabase.restPatch("conversations", listOf("id" to "eq.$conversationId"), body)
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun editMessage(conversationId: String, messageId: String, newText: String): Result<Unit> {
        val clean = newText.trim().take(4000)
        if (clean.isBlank()) return Result.failure(IllegalArgumentException("Message cannot be empty."))
        return try {
            val uid = requireUserId()
            val nowMs = System.currentTimeMillis()
            messageDao.updateEditedText(uid, messageId, clean, nowMs)
            val body = JSONObject().apply {
                put("text", clean)
                put("edited", true)
                put("updated_at", nowIso(nowMs))
            }
            supabase.restPatch(
                "messages",
                listOf("id" to "eq.$messageId", "conversation_id" to "eq.$conversationId"),
                body
            )
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun softDeleteMessage(conversationId: String, messageId: String): Result<Unit> {
        return try {
            val uid = requireUserId()
            val nowMs = System.currentTimeMillis()
            val existing = messageDao.getById(uid, messageId)
            messageDao.markSoftDeleted(uid, messageId, nowMs)
            val body = JSONObject().apply {
                put("text", "This message was deleted")
                put("deleted", true)
                put("media_url", JSONObject.NULL)
                put("updated_at", nowIso(nowMs))
            }
            runCatching {
                supabase.restPatch(
                    "messages",
                    listOf("id" to "eq.$messageId", "conversation_id" to "eq.$conversationId"),
                    body
                )
            }
            if (existing?.remoteMediaUrl?.startsWith("http") == true) {
                runCatching { supabase.deleteStorageObjectByPublicUrl(existing.remoteMediaUrl) }
            }
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun reactToMessage(
        conversationId: String,
        message: ChatMessage,
        emoji: String
    ): Result<Unit> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        return try {
            val nextReactions = message.reactions.toMutableMap()
            if (nextReactions[uid] == emoji) nextReactions.remove(uid) else nextReactions[uid] = emoji
            val existing = messageDao.getById(uid, message.id)
            if (existing != null) {
                messageDao.upsert(
                    existing.copy(
                        reactionsJson = mapToJsonObject(nextReactions).toString(),
                        updatedAtMs = System.currentTimeMillis()
                    )
                )
            }
            val body = JSONObject().apply {
                put("reactions", mapToJsonObject(nextReactions))
                put("updated_at", nowIso())
            }
            supabase.restPatch(
                "messages",
                listOf("id" to "eq.${message.id}", "conversation_id" to "eq.$conversationId"),
                body
            )
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun toggleStarMessage(conversationId: String, message: ChatMessage): Result<Unit> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        return try {
            val nextStarred = message.starredBy.toMutableList()
            if (uid in nextStarred) nextStarred.remove(uid) else nextStarred.add(uid)
            val distinctStarred = nextStarred.distinct().take(50)
            val existing = messageDao.getById(uid, message.id)
            if (existing != null) {
                messageDao.upsert(
                    existing.copy(
                        starredByJson = listToJsonArray(distinctStarred).toString(),
                        updatedAtMs = System.currentTimeMillis()
                    )
                )
            }
            val body = JSONObject().apply {
                put("starred_by", listToJsonArray(distinctStarred))
                put("updated_at", nowIso())
            }
            supabase.restPatch(
                "messages",
                listOf("id" to "eq.${message.id}", "conversation_id" to "eq.$conversationId"),
                body
            )
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun markMessagesDeliveredAndRead(
        conversationId: String,
        messages: List<ChatMessage>,
        sendReadReceipts: Boolean
    ): Result<Unit> {
        val uid = supabase.currentUserId() ?: return Result.success(Unit)
        val targets = messages.filter { msg ->
            msg.senderId != uid && (uid !in msg.deliveredTo || (sendReadReceipts && uid !in msg.readBy))
        }.take(20)
        if (targets.isEmpty()) return Result.success(Unit)
        return try {
            val nowMs = System.currentTimeMillis()
            val now = nowIso(nowMs)
            targets.forEach { msg ->
                val delivered = (msg.deliveredTo + uid).distinct().take(50)
                val read = if (sendReadReceipts) (msg.readBy + uid).distinct().take(50) else msg.readBy
                val existing = messageDao.getById(uid, msg.id)
                if (existing != null) {
                    messageDao.upsert(
                        existing.copy(
                            deliveredToJson = listToJsonArray(delivered).toString(),
                            readByJson = listToJsonArray(read).toString(),
                            syncStatus = if (read.size > 1) "read" else "delivered",
                            updatedAtMs = nowMs
                        )
                    )
                }
                val body = JSONObject().apply {
                    put("delivered_to", listToJsonArray(delivered))
                    if (sendReadReceipts) {
                        put("read_by", listToJsonArray(read))
                    }
                    put("updated_at", now)
                }
                supabase.restPatch(
                    "messages",
                    listOf("id" to "eq.${msg.id}", "conversation_id" to "eq.$conversationId"),
                    body
                )
            }
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    suspend fun redownloadMessageMedia(userId: String, messageId: String): Result<String> {
        val entity = messageDao.getById(userId, messageId)
            ?: return Result.failure(IllegalStateException("Message record not found locally."))
        val downloadedPath = mediaStore.ensureMediaCachedLocally(userId, entity, forceRedownload = true)
        return if (!downloadedPath.isNullOrBlank()) {
            Result.success(downloadedPath)
        } else {
            Result.failure(IllegalStateException("Media is no longer available on the server."))
        }
    }

    suspend fun computeStorageSummary(
        userId: String,
        conversations: List<Conversation>
    ): LocalStorageSummary = mediaStore.computeStorageSummary(userId, conversations)

    suspend fun deleteLocalMediaItems(userId: String, mediaIds: List<String>): Int =
        mediaStore.deleteMediaItems(userId, mediaIds)

    suspend fun clearLocalMediaByCategory(userId: String, category: String): Int =
        mediaStore.clearMediaByCategory(userId, category)

    suspend fun clearLocalMediaByConversation(userId: String, conversationId: String): Int =
        mediaStore.clearMediaByConversation(userId, conversationId)

    suspend fun clearAllDownloadedMedia(userId: String): Int =
        mediaStore.clearAllDownloadedMedia(userId)

    suspend fun updateChatPref(
        conversationId: String,
        pinned: Boolean? = null,
        archived: Boolean? = null,
        muted: Boolean? = null,
        hidden: Boolean? = null,
        unreadCount: Long? = null
    ): Result<Unit> {
        val uid = try { requireUserId() } catch (e: Exception) { return Result.failure(e) }
        return try {
            val existing = supabase.restGet(
                table = "chat_prefs",
                queryParams = listOf(
                    "owner_id" to "eq.$uid",
                    "conversation_id" to "eq.$conversationId",
                    "limit" to "1"
                )
            )
            if (existing.length() > 0) {
                val updates = JSONObject().apply {
                    if (pinned != null) put("pinned", pinned)
                    if (archived != null) put("archived", archived)
                    if (muted != null) put("muted", muted)
                    if (hidden != null) put("hidden", hidden)
                    if (unreadCount != null) put("unread_count", unreadCount.coerceAtLeast(0L))
                    put("updated_at", nowIso())
                }
                supabase.restPatch(
                    "chat_prefs",
                    listOf("owner_id" to "eq.$uid", "conversation_id" to "eq.$conversationId"),
                    updates
                )
            } else {
                val payload = JSONObject().apply {
                    put("owner_id", uid)
                    put("conversation_id", conversationId)
                    put("pinned", pinned ?: false)
                    put("archived", archived ?: false)
                    put("muted", muted ?: false)
                    put("hidden", hidden ?: false)
                    put("unread_count", (unreadCount ?: 0L).coerceAtLeast(0L))
                    put("updated_at", nowIso())
                }
                supabase.restPost("chat_prefs", payload, upsert = true, onConflict = "owner_id,conversation_id")
            }
            Result.success(Unit)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    private fun parseConversation(obj: JSONObject): Conversation =
        Conversation(
            id = obj.optString("id", ""),
            kind = obj.optString("kind", "direct"),
            memberUids = jsonArrayToStringList(obj.optJSONArray("member_uids")),
            adminUids = jsonArrayToStringList(obj.optJSONArray("admin_uids")),
            title = obj.optString("title", ""),
            description = obj.optString("description", ""),
            avatarBase64 = optNullableString(obj, "avatar_url"),
            memberNames = jsonObjectToStringMap(obj.optJSONObject("member_names")),
            memberUsernames = jsonObjectToStringMap(obj.optJSONObject("member_usernames")),
            lastMessageText = obj.optString("last_message_text", ""),
            lastMessageType = obj.optString("last_message_type", "text"),
            lastMessageSenderId = obj.optString("last_message_sender_id", ""),
            lastMessageAt = AppTimestamp.parseIso(optNullableString(obj, "last_message_at")),
            typingUid = optNullableString(obj, "typing_uid"),
            typingAt = AppTimestamp.parseIso(optNullableString(obj, "typing_at")),
            createdBy = obj.optString("created_by", ""),
            createdAt = AppTimestamp.parseIso(optNullableString(obj, "created_at")),
            updatedAt = AppTimestamp.parseIso(optNullableString(obj, "updated_at"))
        )

    private fun parseChatPref(obj: JSONObject): ChatPref =
        ChatPref(
            id = obj.optString("conversation_id", ""),
            conversationId = obj.optString("conversation_id", ""),
            pinned = obj.optBoolean("pinned", false),
            archived = obj.optBoolean("archived", false),
            muted = obj.optBoolean("muted", false),
            hidden = obj.optBoolean("hidden", false),
            unreadCount = obj.optLong("unread_count", 0L),
            createdAt = AppTimestamp.parseIso(optNullableString(obj, "created_at")),
            updatedAt = AppTimestamp.parseIso(optNullableString(obj, "updated_at"))
        )

    private fun parseMessage(obj: JSONObject): ChatMessage =
        ChatMessage(
            id = obj.optString("id", ""),
            conversationId = obj.optString("conversation_id", ""),
            senderId = obj.optString("sender_id", ""),
            senderName = obj.optString("sender_name", ""),
            memberUids = jsonArrayToStringList(obj.optJSONArray("member_uids")),
            messageType = obj.optString("message_type", "text"),
            text = obj.optString("text", ""),
            mediaBase64 = optNullableString(obj, "media_url"),
            mediaName = optNullableString(obj, "media_name"),
            mediaDurationSec = if (obj.isNull("media_duration_sec")) null else obj.optLong("media_duration_sec"),
            waveform = jsonArrayToLongList(obj.optJSONArray("waveform")),
            latitude = if (obj.isNull("latitude")) null else obj.optDouble("latitude"),
            longitude = if (obj.isNull("longitude")) null else obj.optDouble("longitude"),
            contactName = optNullableString(obj, "contact_name"),
            contactUsername = optNullableString(obj, "contact_username"),
            replyToId = optNullableString(obj, "reply_to_id"),
            replyToText = optNullableString(obj, "reply_to_text"),
            replyToSender = optNullableString(obj, "reply_to_sender"),
            forwarded = obj.optBoolean("forwarded", false),
            edited = obj.optBoolean("edited", false),
            deleted = obj.optBoolean("deleted", false),
            deliveredTo = jsonArrayToStringList(obj.optJSONArray("delivered_to")),
            readBy = jsonArrayToStringList(obj.optJSONArray("read_by")),
            starredBy = jsonArrayToStringList(obj.optJSONArray("starred_by")),
            reactions = jsonObjectToStringMap(obj.optJSONObject("reactions")),
            syncStatus = "sent",
            createdAt = AppTimestamp.parseIso(optNullableString(obj, "created_at")),
            updatedAt = AppTimestamp.parseIso(optNullableString(obj, "updated_at"))
        )
}
