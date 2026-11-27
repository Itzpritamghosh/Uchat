package com.example.ui.screens

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.automirrored.filled.KeyboardArrowRight
import androidx.compose.material.icons.filled.AudioFile
import androidx.compose.material.icons.filled.CheckCircle
import androidx.compose.material.icons.filled.CleaningServices
import androidx.compose.material.icons.filled.DeleteOutline
import androidx.compose.material.icons.filled.Description
import androidx.compose.material.icons.filled.Image
import androidx.compose.material.icons.filled.Mic
import androidx.compose.material.icons.filled.RadioButtonUnchecked
import androidx.compose.material.icons.filled.Storage
import androidx.compose.material.icons.filled.Videocam
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberModalBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import coil.compose.AsyncImage
import com.example.data.local.LocalMediaStore
import com.example.data.model.ConversationStorageUsage
import com.example.data.model.LocalMediaItem
import com.example.data.model.LocalStorageSummary
import com.example.media.MediaUtils
import com.example.ui.theme.LocalUChatColors
import com.example.viewmodel.UChatViewModel
import java.io.File

private data class MediaCategorySpec(
    val key: String,
    val label: String,
    val icon: ImageVector,
    val tint: Color,
    val bytes: Long,
    val count: Int
)

/**
 * Settings -> Storage and Data manager.
 * Displays local SQLite chat database size, app-private media breakdown by category and conversation,
 * and allows deleting selected media files or clearing downloaded media while keeping all chat history intact.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun StorageAndDataSheet(
    viewModel: UChatViewModel,
    onDismiss: () -> Unit
) {
    val context = LocalContext.current
    val summary by viewModel.storageSummary.collectAsStateWithLifecycle()
    val loading by viewModel.storageLoading.collectAsStateWithLifecycle()

    var inspectedConversationId by remember { mutableStateOf<String?>(null) }
    var selectedMediaIds by remember { mutableStateOf<Set<String>>(emptySet()) }
    var confirmClearAll by remember { mutableStateOf(false) }
    var confirmClearCategory by remember { mutableStateOf<Pair<String, String>?>(null) }
    var confirmClearConversation by remember { mutableStateOf<Pair<String, String>?>(null) }

    LaunchedEffect(Unit) {
        viewModel.refreshStorageSummary()
    }

    val inspectedConversation: ConversationStorageUsage? = remember(summary, inspectedConversationId) {
        inspectedConversationId?.let { id -> summary.byConversation.firstOrNull { it.conversationId == id } }
    }

    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
        sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .fillMaxHeight(0.88f)
                .padding(horizontal = 16.dp)
                .testTag("storage_and_data_sheet")
        ) {
            // Header
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(bottom = 12.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                if (inspectedConversation != null) {
                    IconButton(
                        onClick = {
                            inspectedConversationId = null
                            selectedMediaIds = emptySet()
                        },
                        modifier = Modifier.testTag("storage_conv_back_button")
                    ) {
                        Icon(
                            Icons.AutoMirrored.Filled.ArrowBack,
                            contentDescription = "Back to storage overview"
                        )
                    }
                } else {
                    Icon(
                        Icons.Filled.Storage,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.size(24.dp)
                    )
                    Spacer(Modifier.width(10.dp))
                }
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        text = inspectedConversation?.title ?: "Storage and Data",
                        style = MaterialTheme.typography.titleLarge,
                        fontWeight = FontWeight.Bold,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                    Text(
                        text = if (inspectedConversation != null) {
                            "${inspectedConversation.fileCount} downloaded file(s) • ${MediaUtils.formatBytes(inspectedConversation.totalBytes)}"
                        } else {
                            "Local messages and downloaded media on this device"
                        },
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                if (loading) {
                    CircularProgressIndicator(
                        modifier = Modifier.size(20.dp),
                        strokeWidth = 2.dp,
                        color = MaterialTheme.colorScheme.primary
                    )
                }
            }

            if (inspectedConversation != null) {
                ConversationMediaDetailList(
                    usage = inspectedConversation,
                    selectedIds = selectedMediaIds,
                    onToggleSelect = { id ->
                        selectedMediaIds = if (id in selectedMediaIds) selectedMediaIds - id else selectedMediaIds + id
                    },
                    onDeleteSelected = {
                        val ids = selectedMediaIds.toList()
                        selectedMediaIds = emptySet()
                        viewModel.deleteLocalMediaItems(ids)
                    },
                    onClearConversation = {
                        confirmClearConversation = inspectedConversation.conversationId to inspectedConversation.title
                    }
                )
            } else {
                StorageOverviewContent(
                    summary = summary,
                    onClearCategory = { key, label -> confirmClearCategory = key to label },
                    onInspectConversation = { convId ->
                        selectedMediaIds = emptySet()
                        inspectedConversationId = convId
                    },
                    onClearCache = {
                        MediaUtils.clearAppCache(context)
                        viewModel.refreshStorageSummary()
                        viewModel.showStatus("Temporary app cache cleared")
                    },
                    onClearAllMedia = { confirmClearAll = true }
                )
            }
        }
    }

    if (confirmClearAll) {
        AlertDialog(
            onDismissRequest = { confirmClearAll = false },
            containerColor = MaterialTheme.colorScheme.surfaceVariant,
            title = { Text("Clear Downloaded Media?") },
            text = {
                Text(
                    "This removes all downloaded images, videos, audio, voice notes, and documents from this device (${MediaUtils.formatBytes(summary.totalMediaBytes)}). Your chat message history stays safely stored in your local database."
                )
            },
            confirmButton = {
                Button(
                    onClick = {
                        confirmClearAll = false
                        viewModel.clearAllDownloadedMedia()
                    },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = Color.White
                    ),
                    modifier = Modifier.testTag("confirm_clear_all_media_button")
                ) {
                    Text("Clear Media")
                }
            },
            dismissButton = {
                TextButton(onClick = { confirmClearAll = false }) { Text("Cancel") }
            }
        )
    }

    confirmClearCategory?.let { (catKey, catLabel) ->
        AlertDialog(
            onDismissRequest = { confirmClearCategory = null },
            containerColor = MaterialTheme.colorScheme.surfaceVariant,
            title = { Text("Clear $catLabel?") },
            text = {
                Text("Remove all locally downloaded $catLabel files to free space? Message history will not be deleted.")
            },
            confirmButton = {
                Button(
                    onClick = {
                        confirmClearCategory = null
                        viewModel.clearLocalMediaCategory(catKey, catLabel)
                    },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = Color.White
                    )
                ) { Text("Clear") }
            },
            dismissButton = {
                TextButton(onClick = { confirmClearCategory = null }) { Text("Cancel") }
            }
        )
    }

    confirmClearConversation?.let { (convId, convTitle) ->
        AlertDialog(
            onDismissRequest = { confirmClearConversation = null },
            containerColor = MaterialTheme.colorScheme.surfaceVariant,
            title = { Text("Clear Media in $convTitle?") },
            text = {
                Text("Remove downloaded media files for this chat? All text messages and conversation history will remain intact.")
            },
            confirmButton = {
                Button(
                    onClick = {
                        confirmClearConversation = null
                        selectedMediaIds = emptySet()
                        viewModel.clearLocalMediaConversation(convId, convTitle)
                        inspectedConversationId = null
                    },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = Color.White
                    )
                ) { Text("Clear Media") }
            },
            dismissButton = {
                TextButton(onClick = { confirmClearConversation = null }) { Text("Cancel") }
            }
        )
    }
}

@Composable
private fun StorageOverviewContent(
    summary: LocalStorageSummary,
    onClearCategory: (String, String) -> Unit,
    onInspectConversation: (String) -> Unit,
    onClearCache: () -> Unit,
    onClearAllMedia: () -> Unit
) {
    val categories = remember(summary) {
        listOf(
            MediaCategorySpec(LocalMediaStore.CATEGORY_IMAGES, "Images", Icons.Filled.Image, Color(0xFF25D366), summary.imagesBytes, summary.imagesCount),
            MediaCategorySpec(LocalMediaStore.CATEGORY_VIDEOS, "Videos", Icons.Filled.Videocam, Color(0xFF29B6F6), summary.videosBytes, summary.videosCount),
            MediaCategorySpec(LocalMediaStore.CATEGORY_VOICE, "Voice Messages", Icons.Filled.Mic, Color(0xFF00BFA5), summary.voiceBytes, summary.voiceCount),
            MediaCategorySpec(LocalMediaStore.CATEGORY_AUDIO, "Audio", Icons.Filled.AudioFile, Color(0xFFFFB300), summary.audioBytes, summary.audioCount),
            MediaCategorySpec(LocalMediaStore.CATEGORY_DOCUMENTS, "Documents", Icons.Filled.Description, Color(0xFF9C6BFF), summary.documentsBytes, summary.documentsCount)
        )
    }

    LazyColumn(
        contentPadding = PaddingValues(bottom = 32.dp),
        verticalArrangement = Arrangement.spacedBy(14.dp)
    ) {
        // Total Storage Summary Card
        item {
            Surface(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(20.dp),
                modifier = Modifier
                    .fillMaxWidth()
                    .border(1.dp, MaterialTheme.colorScheme.outline, RoundedCornerShape(20.dp))
            ) {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text(
                        text = "Total Local Storage Used",
                        style = MaterialTheme.typography.labelLarge,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    Spacer(Modifier.height(4.dp))
                    Text(
                        text = MediaUtils.formatBytes(summary.totalBytes),
                        style = MaterialTheme.typography.headlineMedium,
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colorScheme.primary
                    )
                    Spacer(Modifier.height(12.dp))

                    // Segmented bar
                    val denom = summary.totalBytes.coerceAtLeast(1L).toFloat()
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(10.dp)
                            .clip(RoundedCornerShape(6.dp))
                            .background(Color.Black.copy(alpha = 0.35f))
                    ) {
                        val segments = listOf(
                            summary.databaseBytes to Color(0xFF78909C),
                            summary.imagesBytes to Color(0xFF25D366),
                            summary.videosBytes to Color(0xFF29B6F6),
                            summary.voiceBytes to Color(0xFF00BFA5),
                            summary.audioBytes to Color(0xFFFFB300),
                            summary.documentsBytes to Color(0xFF9C6BFF),
                            summary.cacheBytes to Color(0xFF546E7A)
                        ).filter { it.first > 0L }
                        segments.forEach { (bytes, color) ->
                            val frac = (bytes / denom).coerceAtLeast(0.03f)
                            Box(
                                modifier = Modifier
                                    .weight(frac)
                                    .fillMaxHeight()
                                    .background(color)
                            )
                        }
                    }

                    Spacer(Modifier.height(14.dp))
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween
                    ) {
                        StorageStatPill("Chat Database", MediaUtils.formatBytes(summary.databaseBytes))
                        StorageStatPill("Downloaded Media", MediaUtils.formatBytes(summary.totalMediaBytes))
                        StorageStatPill("Temp Cache", MediaUtils.formatBytes(summary.cacheBytes))
                    }
                }
            }
        }

        // Media Breakdown by Category
        item {
            Text(
                text = "Media Breakdown",
                style = MaterialTheme.typography.labelLarge,
                color = MaterialTheme.colorScheme.primary,
                modifier = Modifier.padding(start = 4.dp)
            )
            Spacer(Modifier.height(6.dp))
            Surface(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(20.dp),
                modifier = Modifier
                    .fillMaxWidth()
                    .border(1.dp, MaterialTheme.colorScheme.outline, RoundedCornerShape(20.dp))
            ) {
                Column {
                    categories.forEachIndexed { idx, cat ->
                        Row(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(horizontal = 14.dp, vertical = 12.dp),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            Surface(
                                color = cat.tint.copy(alpha = 0.16f),
                                shape = CircleShape,
                                modifier = Modifier.size(38.dp)
                            ) {
                                Box(contentAlignment = Alignment.Center) {
                                    Icon(cat.icon, contentDescription = null, tint = cat.tint, modifier = Modifier.size(20.dp))
                                }
                            }
                            Spacer(Modifier.width(12.dp))
                            Column(modifier = Modifier.weight(1f)) {
                                Text(cat.label, style = MaterialTheme.typography.titleSmall)
                                Text(
                                    text = "${cat.count} file(s) • ${MediaUtils.formatBytes(cat.bytes)}",
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant
                                )
                            }
                            if (cat.count > 0) {
                                TextButton(
                                    onClick = { onClearCategory(cat.key, cat.label) },
                                    modifier = Modifier.testTag("clear_category_${cat.key}")
                                ) {
                                    Text("Clear", color = MaterialTheme.colorScheme.error)
                                }
                            }
                        }
                        if (idx < categories.lastIndex) {
                            HorizontalDivider(
                                color = LocalUChatColors.current.divider,
                                modifier = Modifier.padding(horizontal = 14.dp)
                            )
                        }
                    }
                }
            }
        }

        // Storage by Conversation
        item {
            Text(
                text = "Storage by Conversation",
                style = MaterialTheme.typography.labelLarge,
                color = MaterialTheme.colorScheme.primary,
                modifier = Modifier.padding(start = 4.dp)
            )
            Spacer(Modifier.height(6.dp))
            Surface(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(20.dp),
                modifier = Modifier
                    .fillMaxWidth()
                    .border(1.dp, MaterialTheme.colorScheme.outline, RoundedCornerShape(20.dp))
            ) {
                if (summary.byConversation.isEmpty()) {
                    Text(
                        text = "No downloaded media files stored on this device yet.",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(16.dp)
                    )
                } else {
                    Column {
                        summary.byConversation.forEachIndexed { idx, conv ->
                            Row(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clickable { onInspectConversation(conv.conversationId) }
                                    .padding(horizontal = 14.dp, vertical = 12.dp)
                                    .testTag("storage_conv_row_${conv.conversationId}"),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Column(modifier = Modifier.weight(1f)) {
                                    Text(
                                        text = conv.title,
                                        style = MaterialTheme.typography.titleSmall,
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                    Text(
                                        text = "${conv.fileCount} media file(s)",
                                        style = MaterialTheme.typography.bodySmall,
                                        color = MaterialTheme.colorScheme.onSurfaceVariant
                                    )
                                }
                                Text(
                                    text = MediaUtils.formatBytes(conv.totalBytes),
                                    style = MaterialTheme.typography.labelLarge,
                                    color = MaterialTheme.colorScheme.primary
                                )
                                Spacer(Modifier.width(4.dp))
                                Icon(
                                    Icons.AutoMirrored.Filled.KeyboardArrowRight,
                                    contentDescription = "Manage conversation media",
                                    tint = MaterialTheme.colorScheme.onSurfaceVariant
                                )
                            }
                            if (idx < summary.byConversation.lastIndex) {
                                HorizontalDivider(
                                    color = LocalUChatColors.current.divider,
                                    modifier = Modifier.padding(horizontal = 14.dp)
                                )
                            }
                        }
                    }
                }
            }
        }

        // Cleanup Actions (strictly media & cache only — never deletes messages)
        item {
            Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                OutlinedButton(
                    onClick = onClearCache,
                    shape = RoundedCornerShape(14.dp),
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(48.dp)
                        .testTag("storage_clear_temp_cache_button")
                ) {
                    Icon(Icons.Filled.CleaningServices, contentDescription = null, modifier = Modifier.size(18.dp))
                    Spacer(Modifier.width(8.dp))
                    Text("Clear Temp Cache (${MediaUtils.formatBytes(summary.cacheBytes)})")
                }

                Button(
                    onClick = onClearAllMedia,
                    enabled = summary.totalMediaCount > 0,
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error.copy(alpha = 0.18f),
                        contentColor = MaterialTheme.colorScheme.error
                    ),
                    shape = RoundedCornerShape(14.dp),
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(50.dp)
                        .testTag("storage_clear_all_media_button")
                ) {
                    Icon(Icons.Filled.DeleteOutline, contentDescription = null, modifier = Modifier.size(18.dp))
                    Spacer(Modifier.width(8.dp))
                    Text("Clear All Downloaded Media (Keep Messages)", fontWeight = FontWeight.SemiBold)
                }
            }
        }
    }
}

@Composable
private fun StorageStatPill(label: String, value: String) {
    Column {
        Text(
            text = label,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        Text(
            text = value,
            style = MaterialTheme.typography.titleSmall,
            fontWeight = FontWeight.SemiBold
        )
    }
}

@Composable
private fun ConversationMediaDetailList(
    usage: ConversationStorageUsage,
    selectedIds: Set<String>,
    onToggleSelect: (String) -> Unit,
    onDeleteSelected: () -> Unit,
    onClearConversation: () -> Unit
) {
    Column(modifier = Modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(bottom = 10.dp),
            horizontalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            if (selectedIds.isNotEmpty()) {
                Button(
                    onClick = onDeleteSelected,
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = Color.White
                    ),
                    shape = RoundedCornerShape(12.dp),
                    modifier = Modifier
                        .weight(1f)
                        .testTag("storage_delete_selected_media_button")
                ) {
                    Icon(Icons.Filled.DeleteOutline, contentDescription = null, modifier = Modifier.size(18.dp))
                    Spacer(Modifier.width(6.dp))
                    Text("Delete Selected (${selectedIds.size})")
                }
            }
            OutlinedButton(
                onClick = onClearConversation,
                shape = RoundedCornerShape(12.dp),
                modifier = Modifier
                    .weight(1f)
                    .testTag("storage_clear_conversation_media_button")
            ) {
                Text("Clear All in Chat", color = MaterialTheme.colorScheme.error)
            }
        }

        LazyColumn(
            contentPadding = PaddingValues(bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            items(usage.items, key = { it.id }) { item ->
                val selected = item.id in selectedIds
                MediaFileRow(
                    item = item,
                    selected = selected,
                    onClick = { onToggleSelect(item.id) }
                )
            }
        }
    }
}

@Composable
private fun MediaFileRow(
    item: LocalMediaItem,
    selected: Boolean,
    onClick: () -> Unit
) {
    Surface(
        color = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.14f) else MaterialTheme.colorScheme.surfaceVariant,
        shape = RoundedCornerShape(14.dp),
        modifier = Modifier
            .fillMaxWidth()
            .border(
                width = 1.dp,
                color = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.outline,
                shape = RoundedCornerShape(14.dp)
            )
            .clickable(onClick = onClick)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(10.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            if (item.category == LocalMediaStore.CATEGORY_IMAGES && item.fileExists) {
                AsyncImage(
                    model = File(item.localPath),
                    contentDescription = item.fileName,
                    contentScale = ContentScale.Crop,
                    modifier = Modifier
                        .size(46.dp)
                        .clip(RoundedCornerShape(10.dp))
                )
            } else {
                val icon = when (item.category) {
                    LocalMediaStore.CATEGORY_VIDEOS -> Icons.Filled.Videocam
                    LocalMediaStore.CATEGORY_VOICE -> Icons.Filled.Mic
                    LocalMediaStore.CATEGORY_AUDIO -> Icons.Filled.AudioFile
                    else -> Icons.Filled.Description
                }
                Surface(
                    color = MaterialTheme.colorScheme.primary.copy(alpha = 0.16f),
                    shape = RoundedCornerShape(10.dp),
                    modifier = Modifier.size(46.dp)
                ) {
                    Box(contentAlignment = Alignment.Center) {
                        Icon(icon, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
                    }
                }
            }

            Spacer(Modifier.width(12.dp))
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    text = item.fileName,
                    style = MaterialTheme.typography.titleSmall,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                Text(
                    text = "${LocalMediaStore.categoryDisplayName(item.category)} • ${MediaUtils.formatBytes(item.sizeBytes)}",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            Icon(
                imageVector = if (selected) Icons.Filled.CheckCircle else Icons.Filled.RadioButtonUnchecked,
                contentDescription = if (selected) "Selected" else "Not selected",
                tint = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}
