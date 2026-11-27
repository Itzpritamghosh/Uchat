package com.example.data.local

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.migration.Migration
import java.io.File

private const val DB_NAME = "uchat_local.db"

@Database(
    entities = [
        ConversationEntity::class,
        MessageEntity::class,
        LocalMediaEntity::class
    ],
    version = 1,
    exportSchema = false
)
abstract class UChatDatabase : RoomDatabase() {
    abstract fun conversationDao(): ConversationDao
    abstract fun messageDao(): MessageDao
    abstract fun localMediaDao(): LocalMediaDao

    companion object {
        @Volatile
        private var instance: UChatDatabase? = null

        /**
         * Explicit non-destructive Room migrations array.
         * Never use fallbackToDestructiveMigration() so local chat history is preserved across app updates.
         */
        private val MIGRATIONS: Array<Migration> = emptyArray()

        fun getInstance(context: Context): UChatDatabase {
            return instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    UChatDatabase::class.java,
                    DB_NAME
                )
                    .setJournalMode(JournalMode.WRITE_AHEAD_LOGGING)
                    .addMigrations(*MIGRATIONS)
                    .build()
                    .also { instance = it }
            }
        }

        fun getDatabaseSizeBytes(context: Context): Long {
            val dbFile = context.getDatabasePath(DB_NAME)
            val walFile = File("${dbFile.absolutePath}-wal")
            val shmFile = File("${dbFile.absolutePath}-shm")
            return listOf(dbFile, walFile, shmFile).sumOf { f ->
                if (f.exists() && f.isFile) f.length() else 0L
            }
        }
    }
}
