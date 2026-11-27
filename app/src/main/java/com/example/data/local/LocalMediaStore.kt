package com.example.data.local

import android.content.Context
import android.graphics.BitmapFactory
import android.util.Base64
import android.util.Log
import com.example.data.model.Conversation
import com.example.data.model.ConversationStorageUsage
import com.example.data.model.LocalMediaItem
import com.example.data.model.LocalStorageSummary
import com.example.data.supabase.SupabaseClient
import com.example.media.MediaUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.security.MessageDigest
import java.util.UUID

private const val MEDIA_STORE_TAG = "LocalMediaStore"

/**
 * App-private internal media storage manager for local-first messaging.
 * Organizes downloaded and sent files into:
 *   filesDir/uchat_media/<safeUid>/{images, videos, documents, audio, voice}
 * Deduplicates by SHA-256 hash, verifies file integrity, and allows clearing media
 * without ever deleting message history records from Room.
 */
class LocalMediaStore(
    private val appContext: Context,
    private val database: UChatDatabase,
    private val supabase: SupabaseClient
) {
    companion object {
        const val CATEGORY_IMAGES = "images"
        const val CATEGORY_VIDEOS = "videos"
        const val CATEGORY_DOCUMENTS = "documents"
        const val CATEGORY_AUDIO = "audio"
        const val CATEGORY_VOICE = "voice"

        val ALL_CATEGORIES = listOf(
            CATEGORY_IMAGES,
            CATEGORY_VIDEOS,
            CATEGORY_AUDIO,
            CATEGORY_VOICE,
            CATEGORY_DOCUMENTS
        )

        fun categoryForMessageType(messageType: String): String = when (messageType.lowercase()) {
            "image" -> CATEGORY_IMAGES
            "video" -> CATEGORY_VIDEOS
            "audio" -> CATEGORY_AUDIO
            "voice" -> CATEGORY_VOICE
            else -> CATEGORY_DOCUMENTS
        }

        fun defaultExtensionForCategory(category: String, fileName: String?): String {
            val fromName = fileName
                ?.substringAfterLast('.', "")
                ?.lowercase()
                ?.replace(Regex("[^a-z0-9]"), "")
                ?.take(8)
            if (!fromName.isNullOrBlank()) return fromName
            return when (category) {
                CATEGORY_IMAGES -> "jpg"
                CATEGORY_VIDEOS -> "mp4"
                CATEGORY_AUDIO -> "mp3"
                CATEGORY_VOICE -> "m4a"
                else -> "bin"
            }
        }

        fun defaultMimeForCategory(category: String): String = when (category) {
            CATEGORY_IMAGES -> "image/jpeg"
            CATEGORY_VIDEOS -> "video/mp4"
            CATEGORY_AUDIO -> "audio/mpeg"
            CATEGORY_VOICE -> "audio/mp4"
            else -> "application/octet-stream"
        }

        fun categoryDisplayName(category: String): String = when (category) {
            CATEGORY_IMAGES -> "Images"
            CATEGORY_VIDEOS -> "Videos"
            CATEGORY_AUDIO -> "Audio"
            CATEGORY_VOICE -> "Voice Messages"
            CATEGORY_DOCUMENTS -> "Documents"
            else -> "Media"
        }

        fun isMediaMessageType(messageType: String): Boolean =
            messageType in setOf("image", "video", "document", "audio", "voice")
    }

    private val mediaDao = database.localMediaDao()
    private val messageDao = database.messageDao()

    private fun safeUid(ownerUid: String): String =
        ownerUid.replace(Regex("[^a-zA-Z0-9_-]"), "_").take(80).ifBlank { "user" }

    private fun categoryDir(ownerUid: String, category: String): File {
        val safeCat = if (category in ALL_CATEGORIES) category else CATEGORY_DOCUMENTS
        return File(File(File(appContext.filesDir, "uchat_media"), safeUid(ownerUid)), safeCat).apply {
            if (!exists()) mkdirs()
        }
    }

    private fun sha256Hex(bytes: ByteArray): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
        return digest.joinToString("") { "%02x".format(it) }
    }

    fun isLocalFileValid(path: String?, isImage: Boolean = false): Boolean {
        if (path.isNullOrBlank()) return false
        val file = File(path)
        if (!file.exists() || !file.isFile || file.length() <= 0L) return false
        if (isImage) {
            return runCatching {
                val opts = BitmapFactory.Options().apply { inJustDecodeBounds = true }
                BitmapFactory.decodeFile(file.absolutePath, opts)
                opts.outWidth > 0 && opts.outHeight > 0
            }.getOrDefault(false)
        }
        return true
    }

    private fun writeBytesAtomically(target: File, bytes: ByteArray) {
        target.parentFile?.mkdirs()
        val tmp = File(target.parentFile, "${target.name}.tmp")
        tmp.writeBytes(bytes)
        if (!tmp.renameTo(target)) {
            target.writeBytes(bytes)
            tmp.delete()
        }
    }

    /**
     * Checks if an identical media payload was already uploaded previously for this user
     * so we avoid duplicate Supabase Storage uploads.
     */
    suspend fun findReusableRemoteUrlForBase64(ownerUid: String, base64OrUrl: String?): String? =
        withContext(Dispatchers.IO) {
            if (base64OrUrl.isNullOrBlank()) return@withContext null
            if (base64OrUrl.startsWith("https://") || base64OrUrl.startsWith("http://")) {
                return@withContext base64OrUrl
            }
            val bytes = runCatching { Base64.decode(base64OrUrl, Base64.DEFAULT) }.getOrNull() ?: return@withContext null
            if (bytes.isEmpty()) return@withContext null
            val hash = sha256Hex(bytes)
            val existing = mediaDao.findAvailableByContentHash(ownerUid, hash)
            existing?.remoteUrl?.takeIf { it.startsWith("https://") || it.startsWith("http://") }
        }

    /**
     * Saves outgoing media bytes directly into app-private internal storage before upload,
     * deduplicating by SHA-256 hash so identical files are never stored twice on disk.
     */
    suspend fun saveOutgoingMedia(
        ownerUid: String,
        conversationId: String,
        messageId: String,
        messageType: String,
        mediaValue: String?,
        fileName: String? = null,
        remoteUrl: String? = null
    ): LocalMediaEntity? = withContext(Dispatchers.IO) {
        if (mediaValue.isNullOrBlank() || !isMediaMessageType(messageType)) return@withContext null
        val category = categoryForMessageType(messageType)
        val bytes = when {
            mediaValue.startsWith("https://") || mediaValue.startsWith("http://") -> {
                supabase.downloadBytes(mediaValue)
            }
            else -> runCatching { Base64.decode(mediaValue, Base64.DEFAULT) }.getOrNull()
        } ?: return@withContext null

        if (bytes.isEmpty()) return@withContext null
        saveBytesAsLocalMedia(
            ownerUid = ownerUid,
            conversationId = conversationId,
            messageId = messageId,
            category = category,
            bytes = bytes,
            fileName = fileName,
            remoteUrl = remoteUrl ?: mediaValue.takeIf { it.startsWith("https://") || it.startsWith("http://") }
        )
    }

    /**
     * Ensures incoming media is downloaded and verified on device once.
     * Avoids repeated downloads by checking existing valid local path, remoteUrl index, and SHA-256 hash.
     */
    suspend fun ensureMediaCachedLocally(
        ownerUid: String,
        entity: MessageEntity,
        forceRedownload: Boolean = false
    ): String? = withContext(Dispatchers.IO) {
        if (!isMediaMessageType(entity.messageType) || entity.deleted) return@withContext null
        if (entity.mediaClearedLocally && !forceRedownload) return@withContext null

        val isImage = entity.messageType == "image"
        // 1. Already present and valid on disk
        if (!entity.localMediaPath.isNullOrBlank() && isLocalFileValid(entity.localMediaPath, isImage)) {
            return@withContext entity.localMediaPath
        }

        val remote = entity.remoteMediaUrl?.takeIf { it.isNotBlank() } ?: return@withContext null
        val category = categoryForMessageType(entity.messageType)

        // 2. Deduplicate by remoteUrl if already downloaded for another message
        if (remote.startsWith("https://") || remote.startsWith("http://")) {
            val cachedByUrl = mediaDao.findAvailableByRemoteUrl(ownerUid, remote)
            if (cachedByUrl != null && isLocalFileValid(cachedByUrl.localPath, isImage)) {
                messageDao.attachLocalMedia(
                    ownerUid = ownerUid,
                    messageId = entity.id,
                    localPath = cachedByUrl.localPath,
                    sizeBytes = cachedByUrl.sizeBytes,
                    mimeType = cachedByUrl.mimeType
                )
                return@withContext cachedByUrl.localPath
            }
        }

        // 3. Download remote bytes (or decode legacy inline base64) and persist atomically
        val bytes = try {
            if (remote.startsWith("https://") || remote.startsWith("http://")) {
                supabase.downloadBytes(remote)
            } else {
                Base64.decode(remote, Base64.DEFAULT)
            }
        } catch (e: Exception) {
            Log.w(MEDIA_STORE_TAG, "Media download/decode skipped: ${e.message}")
            null
        } ?: return@withContext null

        if (bytes.isEmpty()) return@withContext null
        if (isImage) {
            val check = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, check)
            if (check.outWidth <= 0 || check.outHeight <= 0) {
                Log.w(MEDIA_STORE_TAG, "Rejected corrupted image payload for message ${entity.id}")
                return@withContext null
            }
        }

        val saved = saveBytesAsLocalMedia(
            ownerUid = ownerUid,
            conversationId = entity.conversationId,
            messageId = entity.id,
            category = category,
            bytes = bytes,
            fileName = entity.mediaName,
            remoteUrl = remote.takeIf { it.startsWith("https://") || it.startsWith("http://") }
        )
        saved?.localPath
    }

    private suspend fun saveBytesAsLocalMedia(
        ownerUid: String,
        conversationId: String,
        messageId: String,
        category: String,
        bytes: ByteArray,
        fileName: String?,
        remoteUrl: String?
    ): LocalMediaEntity? {
        val hash = sha256Hex(bytes)
        val ext = defaultExtensionForCategory(category, fileName)
        val mime = defaultMimeForCategory(category)
        val cleanName = fileName?.takeIf { it.isNotBlank() } ?: "${category}_${hash.take(8)}.$ext"

        val existingByHash = mediaDao.findAvailableByContentHash(ownerUid, hash)
        val targetFile: File = if (existingByHash != null && isLocalFileValid(existingByHash.localPath, category == CATEGORY_IMAGES)) {
            File(existingByHash.localPath)
        } else {
            val file = File(categoryDir(ownerUid, category), "${hash.take(24)}.$ext")
            runCatching { writeBytesAtomically(file, bytes) }.onFailure { e ->
                Log.w(MEDIA_STORE_TAG, "Failed writing local media file: ${e.message}")
                return null
            }
            file
        }

        val record = LocalMediaEntity(
            id = "${messageId}_${category}",
            ownerUid = ownerUid,
            messageId = messageId,
            conversationId = conversationId,
            category = category,
            remoteUrl = remoteUrl ?: existingByHash?.remoteUrl,
            contentHash = hash,
            localPath = targetFile.absolutePath,
            fileName = cleanName,
            mimeType = mime,
            sizeBytes = targetFile.length().coerceAtLeast(bytes.size.toLong()),
            downloadedAtMs = System.currentTimeMillis(),
            status = "available"
        )
        mediaDao.upsert(record)
        messageDao.attachLocalMedia(
            ownerUid = ownerUid,
            messageId = messageId,
            localPath = record.localPath,
            sizeBytes = record.sizeBytes,
            mimeType = record.mimeType
        )
        return record
    }

    suspend fun computeStorageSummary(
        ownerUid: String,
        conversations: List<Conversation>
    ): LocalStorageSummary = withContext(Dispatchers.IO) {
        val dbBytes = UChatDatabase.getDatabaseSizeBytes(appContext)
        val cacheBytes = MediaUtils.calculateCacheBytes(appContext)
        val titleMap = conversations.associate { it.id to it.displayTitle(ownerUid) }
        val records = mediaDao.getAllMediaSync(ownerUid)

        val uniqueCountedPaths = mutableSetOf<String>()
        var imagesBytes = 0L
        var imagesCount = 0
        var videosBytes = 0L
        var videosCount = 0
        var audioBytes = 0L
        var audioCount = 0
        var voiceBytes = 0L
        var voiceCount = 0
        var documentsBytes = 0L
        var documentsCount = 0

        val items = mutableListOf<LocalMediaItem>()
        for (rec in records) {
            val file = File(rec.localPath)
            val exists = file.exists() && file.isFile && file.length() > 0L
            val actualSize = if (exists) file.length() else 0L
            if (!exists && rec.status == "available") {
                mediaDao.updateStatus(rec.id, "missing")
            }
            val item = LocalMediaItem(
                id = rec.id,
                messageId = rec.messageId,
                conversationId = rec.conversationId,
                conversationTitle = titleMap[rec.conversationId] ?: "Conversation",
                category = rec.category,
                fileName = rec.fileName,
                mimeType = rec.mimeType,
                localPath = rec.localPath,
                sizeBytes = actualSize,
                downloadedAtMs = rec.downloadedAtMs,
                fileExists = exists
            )
            if (exists) {
                items.add(item)
                val addBytes = if (uniqueCountedPaths.add(rec.localPath)) actualSize else 0L
                when (rec.category) {
                    CATEGORY_IMAGES -> {
                        imagesBytes += addBytes
                        imagesCount += 1
                    }
                    CATEGORY_VIDEOS -> {
                        videosBytes += addBytes
                        videosCount += 1
                    }
                    CATEGORY_AUDIO -> {
                        audioBytes += addBytes
                        audioCount += 1
                    }
                    CATEGORY_VOICE -> {
                        voiceBytes += addBytes
                        voiceCount += 1
                    }
                    else -> {
                        documentsBytes += addBytes
                        documentsCount += 1
                    }
                }
            }
        }

        val byConversation = items
            .groupBy { it.conversationId }
            .map { (convId, convItems) ->
                val dedupConvBytes = convItems.distinctBy { it.localPath }.sumOf { it.sizeBytes }
                ConversationStorageUsage(
                    conversationId = convId,
                    title = titleMap[convId] ?: "Conversation",
                    totalBytes = dedupConvBytes,
                    fileCount = convItems.size,
                    items = convItems.sortedByDescending { it.sizeBytes }
                )
            }
            .sortedByDescending { it.totalBytes }

        val mediaTotal = imagesBytes + videosBytes + audioBytes + voiceBytes + documentsBytes
        LocalStorageSummary(
            totalBytes = dbBytes + cacheBytes + mediaTotal,
            databaseBytes = dbBytes,
            cacheBytes = cacheBytes,
            imagesBytes = imagesBytes,
            imagesCount = imagesCount,
            videosBytes = videosBytes,
            videosCount = videosCount,
            audioBytes = audioBytes,
            audioCount = audioCount,
            voiceBytes = voiceBytes,
            voiceCount = voiceCount,
            documentsBytes = documentsBytes,
            documentsCount = documentsCount,
            byConversation = byConversation,
            allItems = items
        )
    }

    /**
     * Deletes only the selected downloaded media files while preserving all Room message records.
     */
    suspend fun deleteMediaItems(ownerUid: String, mediaIds: List<String>): Int = withContext(Dispatchers.IO) {
        if (mediaIds.isEmpty()) return@withContext 0
        val targets = mediaDao.getByIds(ownerUid, mediaIds)
        if (targets.isEmpty()) return@withContext 0
        val messageIds = targets.map { it.messageId }.distinct()
        mediaDao.deleteByIds(ownerUid, targets.map { it.id })
        if (messageIds.isNotEmpty()) {
            messageDao.clearLocalMediaForMessages(ownerUid, messageIds)
        }
        // Only delete physical disk file when no other media rows still point to that deduplicated path
        targets.map { it.localPath }.distinct().forEach { path ->
            if (mediaDao.countReferencesToPath(ownerUid, path) == 0) {
                runCatching { File(path).delete() }
            }
        }
        targets.size
    }

    suspend fun clearMediaByCategory(ownerUid: String, category: String): Int = withContext(Dispatchers.IO) {
        val targets = mediaDao.getByCategory(ownerUid, category)
        deleteMediaItems(ownerUid, targets.map { it.id })
    }

    suspend fun clearMediaByConversation(ownerUid: String, conversationId: String): Int = withContext(Dispatchers.IO) {
        val targets = mediaDao.getByConversation(ownerUid, conversationId)
        val count = deleteMediaItems(ownerUid, targets.map { it.id })
        messageDao.clearLocalMediaForConversation(ownerUid, conversationId)
        count
    }

    /**
     * Clears ALL downloaded media files for this user without deleting any chat messages in Room.
     */
    suspend fun clearAllDownloadedMedia(ownerUid: String): Int = withContext(Dispatchers.IO) {
        val all = mediaDao.getAllMediaSync(ownerUid)
        mediaDao.deleteAllForUser(ownerUid)
        messageDao.clearAllLocalMediaForUser(ownerUid)
        val userMediaRoot = File(File(appContext.filesDir, "uchat_media"), safeUid(ownerUid))
        runCatching { userMediaRoot.deleteRecursively() }
        all.size
    }

    /**
     * Copies a picked local video/document/audio URI into app-private bytes with a safe size ceiling.
     */
    suspend fun readUriBytesForAttachment(uri: android.net.Uri, maxBytes: Int = 8 * 1024 * 1024): Pair<String, ByteArray?> =
        withContext(Dispatchers.IO) {
            val name = MediaUtils.queryDisplayName(appContext, uri) ?: "media_${UUID.randomUUID().toString().take(6)}"
            val bytes = runCatching {
                appContext.contentResolver.openInputStream(uri)?.use { input ->
                    val data = input.readBytes()
                    if (data.isNotEmpty() && data.size <= maxBytes) data else null
                }
            }.getOrNull()
            name to bytes
        }
}
