package com.example.data.local

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface ConversationDao {
    @Query(
        """
        SELECT * FROM conversations
        WHERE ownerUid = :ownerUid
        ORDER BY lastMessageAtMs DESC, updatedAtMs DESC
        """
    )
    fun observeConversations(ownerUid: String): Flow<List<ConversationEntity>>

    @Query("SELECT * FROM conversations WHERE ownerUid = :ownerUid ORDER BY lastMessageAtMs DESC")
    suspend fun getConversationsSync(ownerUid: String): List<ConversationEntity>

    @Query("SELECT COUNT(*) FROM conversations WHERE ownerUid = :ownerUid")
    suspend fun countConversations(ownerUid: String): Int

    @Query("SELECT * FROM conversations WHERE ownerUid = :ownerUid AND id = :conversationId LIMIT 1")
    suspend fun getById(ownerUid: String, conversationId: String): ConversationEntity?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(entity: ConversationEntity)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(entities: List<ConversationEntity>)

    @Query(
        """
        UPDATE conversations
        SET lastMessageText = :lastText,
            lastMessageType = :lastType,
            lastMessageSenderId = :senderId,
            lastMessageAtMs = :timestampMs,
            updatedAtMs = :timestampMs
        WHERE ownerUid = :ownerUid AND id = :conversationId
        """
    )
    suspend fun updateLastMessage(
        ownerUid: String,
        conversationId: String,
        lastText: String,
        lastType: String,
        senderId: String,
        timestampMs: Long
    )
}

@Dao
interface MessageDao {
    @Query(
        """
        SELECT * FROM (
            SELECT * FROM messages
            WHERE ownerUid = :ownerUid AND conversationId = :conversationId
            ORDER BY createdAtMs DESC
            LIMIT :limit
        ) ORDER BY createdAtMs ASC
        """
    )
    fun observeRecentMessages(ownerUid: String, conversationId: String, limit: Int): Flow<List<MessageEntity>>

    @Query("SELECT COUNT(*) FROM messages WHERE ownerUid = :ownerUid AND conversationId = :conversationId")
    fun observeMessageCount(ownerUid: String, conversationId: String): Flow<Int>

    @Query("SELECT * FROM messages WHERE ownerUid = :ownerUid AND id = :messageId LIMIT 1")
    suspend fun getById(ownerUid: String, messageId: String): MessageEntity?

    @Query("SELECT * FROM messages WHERE ownerUid = :ownerUid AND conversationId = :conversationId ORDER BY createdAtMs ASC")
    suspend fun getMessagesForConversationSync(ownerUid: String, conversationId: String): List<MessageEntity>

    @Query(
        """
        SELECT * FROM messages
        WHERE ownerUid = :ownerUid AND syncStatus IN ('pending', 'failed')
        ORDER BY createdAtMs ASC
        """
    )
    suspend fun getPendingOrFailedMessages(ownerUid: String): List<MessageEntity>

    @Query(
        """
        SELECT * FROM messages
        WHERE ownerUid = :ownerUid
          AND conversationId = :conversationId
          AND senderId = :ownerUid
          AND serverPruned = 0
          AND syncStatus IN ('sent', 'delivered', 'read')
        ORDER BY createdAtMs ASC
        """
    )
    suspend fun getSentUnprunedMessages(ownerUid: String, conversationId: String): List<MessageEntity>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(entity: MessageEntity)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(entities: List<MessageEntity>)

    @Query(
        """
        UPDATE messages
        SET syncStatus = :syncStatus,
            remoteMediaUrl = COALESCE(:remoteUrl, remoteMediaUrl),
            updatedAtMs = :updatedAtMs
        WHERE ownerUid = :ownerUid AND id = :messageId
        """
    )
    suspend fun updateSyncState(
        ownerUid: String,
        messageId: String,
        syncStatus: String,
        remoteUrl: String?,
        updatedAtMs: Long
    )

    @Query(
        """
        UPDATE messages
        SET localMediaPath = :localPath,
            mediaSizeBytes = COALESCE(:sizeBytes, mediaSizeBytes),
            mediaMimeType = COALESCE(:mimeType, mediaMimeType),
            mediaClearedLocally = 0
        WHERE ownerUid = :ownerUid AND id = :messageId
        """
    )
    suspend fun attachLocalMedia(
        ownerUid: String,
        messageId: String,
        localPath: String,
        sizeBytes: Long?,
        mimeType: String?
    )

    @Query(
        """
        UPDATE messages
        SET localMediaPath = NULL,
            mediaClearedLocally = 1
        WHERE ownerUid = :ownerUid AND id IN (:messageIds)
        """
    )
    suspend fun clearLocalMediaForMessages(ownerUid: String, messageIds: List<String>)

    @Query(
        """
        UPDATE messages
        SET localMediaPath = NULL,
            mediaClearedLocally = 1
        WHERE ownerUid = :ownerUid AND conversationId = :conversationId AND localMediaPath IS NOT NULL
        """
    )
    suspend fun clearLocalMediaForConversation(ownerUid: String, conversationId: String)

    @Query(
        """
        UPDATE messages
        SET localMediaPath = NULL,
            mediaClearedLocally = 1
        WHERE ownerUid = :ownerUid AND localMediaPath IS NOT NULL
        """
    )
    suspend fun clearAllLocalMediaForUser(ownerUid: String)

    @Query(
        """
        UPDATE messages
        SET serverPruned = 1
        WHERE ownerUid = :ownerUid AND id = :messageId
        """
    )
    suspend fun markServerPruned(ownerUid: String, messageId: String)

    @Query(
        """
        UPDATE messages
        SET text = :newText,
            edited = 1,
            updatedAtMs = :updatedAtMs
        WHERE ownerUid = :ownerUid AND id = :messageId
        """
    )
    suspend fun updateEditedText(ownerUid: String, messageId: String, newText: String, updatedAtMs: Long)

    @Query(
        """
        UPDATE messages
        SET deleted = 1,
            text = 'This message was deleted',
            remoteMediaUrl = NULL,
            localMediaPath = NULL,
            updatedAtMs = :updatedAtMs
        WHERE ownerUid = :ownerUid AND id = :messageId
        """
    )
    suspend fun markSoftDeleted(ownerUid: String, messageId: String, updatedAtMs: Long)
}

@Dao
interface LocalMediaDao {
    @Query("SELECT * FROM local_media WHERE ownerUid = :ownerUid ORDER BY downloadedAtMs DESC")
    fun observeAllMedia(ownerUid: String): Flow<List<LocalMediaEntity>>

    @Query("SELECT * FROM local_media WHERE ownerUid = :ownerUid ORDER BY downloadedAtMs DESC")
    suspend fun getAllMediaSync(ownerUid: String): List<LocalMediaEntity>

    @Query(
        """
        SELECT * FROM local_media
        WHERE ownerUid = :ownerUid AND remoteUrl = :remoteUrl AND status = 'available'
        LIMIT 1
        """
    )
    suspend fun findAvailableByRemoteUrl(ownerUid: String, remoteUrl: String): LocalMediaEntity?

    @Query(
        """
        SELECT * FROM local_media
        WHERE ownerUid = :ownerUid AND contentHash = :contentHash AND status = 'available'
        LIMIT 1
        """
    )
    suspend fun findAvailableByContentHash(ownerUid: String, contentHash: String): LocalMediaEntity?

    @Query("SELECT * FROM local_media WHERE ownerUid = :ownerUid AND id IN (:ids)")
    suspend fun getByIds(ownerUid: String, ids: List<String>): List<LocalMediaEntity>

    @Query("SELECT * FROM local_media WHERE ownerUid = :ownerUid AND category = :category")
    suspend fun getByCategory(ownerUid: String, category: String): List<LocalMediaEntity>

    @Query("SELECT * FROM local_media WHERE ownerUid = :ownerUid AND conversationId = :conversationId")
    suspend fun getByConversation(ownerUid: String, conversationId: String): List<LocalMediaEntity>

    @Query("SELECT COUNT(*) FROM local_media WHERE ownerUid = :ownerUid AND localPath = :localPath")
    suspend fun countReferencesToPath(ownerUid: String, localPath: String): Int

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(entity: LocalMediaEntity)

    @Query("UPDATE local_media SET status = :status WHERE id = :id")
    suspend fun updateStatus(id: String, status: String)

    @Query("DELETE FROM local_media WHERE ownerUid = :ownerUid AND id IN (:ids)")
    suspend fun deleteByIds(ownerUid: String, ids: List<String>)

    @Query("DELETE FROM local_media WHERE ownerUid = :ownerUid AND category = :category")
    suspend fun deleteByCategory(ownerUid: String, category: String)

    @Query("DELETE FROM local_media WHERE ownerUid = :ownerUid AND conversationId = :conversationId")
    suspend fun deleteByConversation(ownerUid: String, conversationId: String)

    @Query("DELETE FROM local_media WHERE ownerUid = :ownerUid")
    suspend fun deleteAllForUser(ownerUid: String)
}
