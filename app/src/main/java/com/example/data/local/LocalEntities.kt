package com.example.data.local

import androidx.room.Entity
import androidx.room.Index
import com.example.data.model.AppTimestamp
import com.example.data.model.ChatMessage
import com.example.data.model.Conversation
import com.example.data.supabase.SupabaseClient.Companion.jsonArrayToLongList
import com.example.data.supabase.SupabaseClient.Companion.jsonArrayToStringList
import com.example.data.supabase.SupabaseClient.Companion.jsonObjectToStringMap
import com.example.data.supabase.SupabaseClient.Companion.listToJsonArray
import com.example.data.supabase.SupabaseClient.Companion.mapToJsonObject
import org.json.JSONArray
import org.json.JSONObject

@Entity(
    tableName = "conversations",
    primaryKeys = ["ownerUid", "id"],
    indices = [
        Index(value = ["ownerUid", "lastMessageAtMs"])
    ]
)
data class ConversationEntity(
    val ownerUid: String,
    val id: String,
    val kind: String,
    val memberUidsJson: String,
    val adminUidsJson: String,
    val title: String,
    val description: String,
    val avatarUrl: String?,
    val memberNamesJson: String,
    val memberUsernamesJson: String,
    val lastMessageText: String,
    val lastMessageType: String,
    val lastMessageSenderId: String,
    val lastMessageAtMs: Long,
    val typingUid: String?,
    val typingAtMs: Long?,
    val createdBy: String,
    val createdAtMs: Long,
    val updatedAtMs: Long
)

@Entity(
    tableName = "messages",
    primaryKeys = ["ownerUid", "id"],
    indices = [
        Index(value = ["ownerUid", "conversationId", "createdAtMs"]),
        Index(value = ["ownerUid", "syncStatus"])
    ]
)
data class MessageEntity(
    val ownerUid: String,
    val id: String,
    val conversationId: String,
    val senderId: String,
    val senderName: String,
    val memberUidsJson: String,
    val messageType: String,
    val text: String,
    val remoteMediaUrl: String?,
    val localMediaPath: String?,
    val mediaName: String?,
    val mediaMimeType: String?,
    val mediaSizeBytes: Long?,
    val mediaDurationSec: Long?,
    val waveformJson: String?,
    val latitude: Double?,
    val longitude: Double?,
    val contactName: String?,
    val contactUsername: String?,
    val replyToId: String?,
    val replyToText: String?,
    val replyToSender: String?,
    val forwarded: Boolean,
    val edited: Boolean,
    val deleted: Boolean,
    val deliveredToJson: String,
    val readByJson: String,
    val starredByJson: String,
    val reactionsJson: String,
    val syncStatus: String,
    val mediaClearedLocally: Boolean,
    val serverPruned: Boolean,
    val createdAtMs: Long,
    val updatedAtMs: Long,
    val persistedAtMs: Long
)

@Entity(
    tableName = "local_media",
    primaryKeys = ["id"],
    indices = [
        Index(value = ["ownerUid", "remoteUrl"]),
        Index(value = ["ownerUid", "contentHash"]),
        Index(value = ["ownerUid", "conversationId"]),
        Index(value = ["ownerUid", "category"]),
        Index(value = ["ownerUid", "messageId"])
    ]
)
data class LocalMediaEntity(
    val id: String,
    val ownerUid: String,
    val messageId: String,
    val conversationId: String,
    val category: String,
    val remoteUrl: String?,
    val contentHash: String,
    val localPath: String,
    val fileName: String,
    val mimeType: String,
    val sizeBytes: Long,
    val downloadedAtMs: Long,
    val status: String
)

object LocalMappers {
    private fun parseStringList(json: String?): List<String> =
        if (json.isNullOrBlank()) emptyList()
        else runCatching { jsonArrayToStringList(JSONArray(json)) }.getOrDefault(emptyList())

    private fun parseLongList(json: String?): List<Long>? =
        if (json.isNullOrBlank()) null
        else runCatching { jsonArrayToLongList(JSONArray(json)) }.getOrNull()

    private fun parseStringMap(json: String?): Map<String, String> =
        if (json.isNullOrBlank()) emptyMap()
        else runCatching { jsonObjectToStringMap(JSONObject(json)) }.getOrDefault(emptyMap())

    fun Conversation.toEntity(ownerUid: String): ConversationEntity {
        val now = System.currentTimeMillis()
        val lastMs = lastMessageAt?.epochMillis ?: updatedAt?.epochMillis ?: createdAt?.epochMillis ?: now
        return ConversationEntity(
            ownerUid = ownerUid,
            id = id,
            kind = kind,
            memberUidsJson = listToJsonArray(memberUids).toString(),
            adminUidsJson = listToJsonArray(adminUids).toString(),
            title = title,
            description = description,
            avatarUrl = avatarBase64,
            memberNamesJson = mapToJsonObject(memberNames).toString(),
            memberUsernamesJson = mapToJsonObject(memberUsernames).toString(),
            lastMessageText = lastMessageText,
            lastMessageType = lastMessageType,
            lastMessageSenderId = lastMessageSenderId,
            lastMessageAtMs = lastMs,
            typingUid = typingUid,
            typingAtMs = typingAt?.epochMillis,
            createdBy = createdBy,
            createdAtMs = createdAt?.epochMillis ?: now,
            updatedAtMs = updatedAt?.epochMillis ?: lastMs
        )
    }

    fun ConversationEntity.toModel(): Conversation = Conversation(
        id = id,
        kind = kind,
        memberUids = parseStringList(memberUidsJson),
        adminUids = parseStringList(adminUidsJson),
        title = title,
        description = description,
        avatarBase64 = avatarUrl,
        memberNames = parseStringMap(memberNamesJson),
        memberUsernames = parseStringMap(memberUsernamesJson),
        lastMessageText = lastMessageText,
        lastMessageType = lastMessageType,
        lastMessageSenderId = lastMessageSenderId,
        lastMessageAt = AppTimestamp(lastMessageAtMs),
        typingUid = typingUid,
        typingAt = typingAtMs?.let { AppTimestamp(it) },
        createdBy = createdBy,
        createdAt = AppTimestamp(createdAtMs),
        updatedAt = AppTimestamp(updatedAtMs)
    )

    fun ChatMessage.toEntity(
        ownerUid: String,
        existing: MessageEntity? = null,
        forcedSyncStatus: String? = null
    ): MessageEntity {
        val now = System.currentTimeMillis()
        val createdMs = createdAt?.epochMillis ?: existing?.createdAtMs ?: now
        val updatedMs = updatedAt?.epochMillis ?: createdMs

        val mergedDelivered = (existing?.let { parseStringList(it.deliveredToJson) }.orEmpty() + deliveredTo).distinct()
        val mergedRead = (existing?.let { parseStringList(it.readByJson) }.orEmpty() + readBy).distinct()

        val computedSyncStatus = forcedSyncStatus ?: when {
            existing?.syncStatus == "pending" && syncStatus == "pending" -> "pending"
            existing?.syncStatus == "failed" && syncStatus == "failed" -> "failed"
            mergedRead.size > 1 -> "read"
            mergedDelivered.size > 1 -> "delivered"
            syncStatus.isNotBlank() -> syncStatus
            else -> "sent"
        }

        // Preserve local media path and cleared flag across remote sync refreshes
        val effectiveLocalPath = localMediaPath ?: existing?.localMediaPath
        val effectiveCleared = mediaClearedLocally || (existing?.mediaClearedLocally == true && effectiveLocalPath == null)
        val effectiveSize = mediaSizeBytes ?: existing?.mediaSizeBytes
        val effectiveRemoteUrl = mediaBase64?.takeIf {
            it.startsWith("https://") || it.startsWith("http://") || effectiveLocalPath == null
        } ?: existing?.remoteMediaUrl

        return MessageEntity(
            ownerUid = ownerUid,
            id = id,
            conversationId = conversationId,
            senderId = senderId,
            senderName = senderName,
            memberUidsJson = listToJsonArray(memberUids).toString(),
            messageType = messageType,
            text = text,
            remoteMediaUrl = effectiveRemoteUrl,
            localMediaPath = effectiveLocalPath,
            mediaName = mediaName ?: existing?.mediaName,
            mediaMimeType = existing?.mediaMimeType,
            mediaSizeBytes = effectiveSize,
            mediaDurationSec = mediaDurationSec ?: existing?.mediaDurationSec,
            waveformJson = waveform?.let { listToJsonArray(it).toString() } ?: existing?.waveformJson,
            latitude = latitude ?: existing?.latitude,
            longitude = longitude ?: existing?.longitude,
            contactName = contactName ?: existing?.contactName,
            contactUsername = contactUsername ?: existing?.contactUsername,
            replyToId = replyToId ?: existing?.replyToId,
            replyToText = replyToText ?: existing?.replyToText,
            replyToSender = replyToSender ?: existing?.replyToSender,
            forwarded = forwarded,
            edited = edited,
            deleted = deleted,
            deliveredToJson = listToJsonArray(mergedDelivered).toString(),
            readByJson = listToJsonArray(mergedRead).toString(),
            starredByJson = listToJsonArray(starredBy).toString(),
            reactionsJson = mapToJsonObject(reactions).toString(),
            syncStatus = computedSyncStatus,
            mediaClearedLocally = effectiveCleared,
            serverPruned = existing?.serverPruned ?: false,
            createdAtMs = createdMs,
            updatedAtMs = updatedMs,
            persistedAtMs = existing?.persistedAtMs ?: now
        )
    }

    fun MessageEntity.toModel(): ChatMessage = ChatMessage(
        id = id,
        conversationId = conversationId,
        senderId = senderId,
        senderName = senderName,
        memberUids = parseStringList(memberUidsJson),
        messageType = messageType,
        text = text,
        mediaBase64 = remoteMediaUrl,
        localMediaPath = localMediaPath,
        mediaName = mediaName,
        mediaSizeBytes = mediaSizeBytes,
        mediaDurationSec = mediaDurationSec,
        waveform = parseLongList(waveformJson),
        latitude = latitude,
        longitude = longitude,
        contactName = contactName,
        contactUsername = contactUsername,
        replyToId = replyToId,
        replyToText = replyToText,
        replyToSender = replyToSender,
        forwarded = forwarded,
        edited = edited,
        deleted = deleted,
        deliveredTo = parseStringList(deliveredToJson),
        readBy = parseStringList(readByJson),
        starredBy = parseStringList(starredByJson),
        reactions = parseStringMap(reactionsJson),
        syncStatus = syncStatus,
        mediaClearedLocally = mediaClearedLocally,
        createdAt = AppTimestamp(createdAtMs),
        updatedAt = AppTimestamp(updatedAtMs)
    )
}
