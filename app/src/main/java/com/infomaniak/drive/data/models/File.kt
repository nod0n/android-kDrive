/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2026 Infomaniak Network SA
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.infomaniak.drive.data.models

import android.content.Context
import android.net.Uri
import android.os.Parcelable
import androidx.core.content.FileProvider
import androidx.core.net.toUri
import com.google.gson.annotations.Expose
import com.google.gson.annotations.SerializedName
import com.infomaniak.core.legacy.utils.contains
import com.infomaniak.core.legacy.utils.guessMimeType
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.ui.fileList.DownloadProgressViewModel.Companion.PROGRESS_COMPLETE
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.documentprovider.CloudStorageProvider
import com.infomaniak.drive.data.models.drive.Category
import com.infomaniak.drive.data.models.file.FileConversion
import com.infomaniak.drive.data.models.file.FileExternalImport
import com.infomaniak.drive.data.models.file.FileExternalImport.FileExternalImportStatus
import com.infomaniak.drive.data.models.file.FileVersion
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.PublicationOutcome
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.RealmListParceler.FileCategoryRealmListParceler
import com.infomaniak.drive.utils.RealmListParceler.FileRealmListParceler
import com.infomaniak.drive.utils.RealmListParceler.IntRealmListParceler
import com.infomaniak.drive.utils.RealmListParceler.StringRealmListParceler
import com.infomaniak.drive.utils.Utils.INDETERMINATE_PROGRESS
import com.infomaniak.drive.utils.Utils.ROOT_ID
import com.infomaniak.drive.utils.downloadFile
import com.infomaniak.drive.utils.isUrlFile
import com.infomaniak.drive.utils.isWeblocFile
import io.realm.RealmList
import io.realm.RealmObject
import io.realm.RealmResults
import io.realm.annotations.Ignore
import io.realm.annotations.LinkingObjects
import io.realm.annotations.PrimaryKey
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import kotlinx.parcelize.IgnoredOnParcel
import kotlinx.parcelize.Parcelize
import kotlinx.parcelize.WriteWith
import java.util.Date

@Parcelize
open class File(
    @PrimaryKey
    @Expose(serialize = false, deserialize = false)
    var uid: String = "", // Need migration for any update
    var id: Int = 0,
    @SerializedName("parent_id")
    var parentId: Int = 0,
    @SerializedName("drive_id")
    var driveId: Int = 0,
    var name: String = "",
    @SerializedName("sorted_name")
    var sortedName: String = name,
    var path: String = "", // remote path or Uri for uploadFile
    var type: String = "file",
    var status: String? = null,
    var visibility: String = "",
    @SerializedName("created_by")
    var createdBy: Int = 0,
    @SerializedName("created_at")
    var createdAt: Long = 0,
    @SerializedName("added_at")
    var addedAt: Long = 0,
    @SerializedName("last_modified_at")
    var lastModifiedAt: Long = 0,
    /**
     * LOCAL byte-provenance, never a server field: the modification baseline the bytes at
     * this row's offline path were actually PUBLISHED under (issue: round 15 #6). Edit
     * detection judges the copy against this baseline — not against [lastModifiedAt], which
     * accepted server metadata advances — so retained old-clean bytes are not reclassified
     * as a local edit after a backward-dated newer generation commits. Null (legacy rows)
     * falls back to [lastModifiedAt]. Preserved across metadata updates, advanced only by a
     * successful publication of new bytes.
     */
    var publishedCleanBaseline: Long? = null,
    @SerializedName("deleted_by")
    var deletedBy: Int = 0,
    @SerializedName("deleted_at")
    var deletedAt: Long = 0,
    var users: @WriteWith<IntRealmListParceler> RealmList<Int> = RealmList(),
    @SerializedName("is_favorite")
    var isFavorite: Boolean = false,
    @SerializedName("sharelink")
    var shareLink: ShareLink? = null,
    @SerializedName("capabilities")
    var rights: Rights? = null,
    var categories: @WriteWith<FileCategoryRealmListParceler> RealmList<FileCategory> = RealmList(),
    var cursor: String? = null,

    /**
     * DIRECTORY ONLY
     */
    var color: String? = null,
    var dropbox: DropBox? = null,
    @SerializedName("external_import")
    var externalImport: FileExternalImport? = null,

    /**
     * FILE ONLY
     */
    var size: Long? = null,
    @SerializedName("supported_by")
    var supportedBy: @WriteWith<StringRealmListParceler> RealmList<String>? = null,
    @SerializedName("extension_type")
    var extensionType: String = "",
    var version: FileVersion? = null,
    @SerializedName("conversion_capabilities")
    var conversion: FileConversion? = null,

    /**
     * OFFLINE
     */
    @SerializedName("revised_at")
    var revisedAt: Long = 0,
    @SerializedName("updated_at")
    var updatedAt: Long = 0,

    /**
     * LOCAL
     */
    var children: @WriteWith<FileRealmListParceler> RealmList<File> = RealmList(),
    var isComplete: Boolean = false,
    var isFromUploads: Boolean = false,
    var isMarkedAsOffline: Boolean = false,
    var isOffline: Boolean = false,

    /**
     * True when this item is itself an offline folder root or is inside one (issue #1902).
     * Its offline copy is managed by the folder coverage, as opposed to [isOffline] which is an
     * explicit user pin on the file itself. Both can be true at the same time (a pinned file
     * inside an offline folder).
     */
    var isInOfflineFolder: Boolean = false,
    var responseAt: Long = 0,
    var versionCode: Int = 0,
    var lastActionAt: Long = 0,

    ) : RealmObject(), Parcelable {

    /**
     * The baseline the bytes at this row's offline path were published under, for edit
     * detection (issues: round 15 #6, round 17 #3/#21): the recorded baseline when POSITIVE —
     * a legacy required-long migration leaves a ZERO default, which is NOT publication
     * provenance and must behave exactly like an unset baseline (fall back to
     * [lastModifiedAt]) — else the accepted server metadata.
     */
    val effectivePublishedBaseline: Long
        get() = publishedCleanBaseline?.takeIf { it > 0 } ?: lastModifiedAt

    val hasThumbnail inline get() = supportedBy?.contains(SupportedByType.THUMBNAIL.apiValue) ?: false
    val hasOnlyoffice inline get() = supportedBy?.contains(SupportedByType.ONLYOFFICE.apiValue) ?: false

    @LinkingObjects("children")
    val localParent: RealmResults<File>? = null

    @Ignore
    var driveColor: String = "#5C89F7"

    @Ignore
    var currentProgress: Int = INDETERMINATE_PROGRESS

    @IgnoredOnParcel
    @Ignore
    @Transient
    var publicShareUuid: String = ""

    @IgnoredOnParcel
    @Ignore
    @Transient
    var publicShareAuthToken: String? = null

    val revisedAtInMillis: Long inline get() = revisedAt * 1000

    fun initUid() {
        this.uid = "${id}_$driveId"
    }

    fun isManagedAndValidByRealm() = isManaged && isValid

    fun isNotManagedByRealm() = !isManaged

    fun isUsable() = isManagedAndValidByRealm() || isNotManagedByRealm()

    fun isFolder(): Boolean {
        return type == "dir"
    }

    fun isOnlyOfficePreview(): Boolean {
        return hasOnlyoffice || conversion?.whenOnlyoffice == true
    }

    fun isDropBox() = hasVisibilities(VisibilityType.IS_DROPBOX)

    fun isTrashed(): Boolean {
        return status?.contains("trash") == true
    }

    fun isPublicShared() = publicShareUuid.isNotBlank()

    fun isSharedWithMe(): Boolean {
        return hasVisibilities(VisibilityType.IS_SHARED_SPACE, VisibilityType.IS_IN_SHARED_SPACE)
    }

    fun isPDF() = getFileType() == ExtensionType.PDF
    fun isVideo() = getFileType() == ExtensionType.VIDEO

    fun getFileType(): ExtensionType {
        return if (isFromUploads) getFileTypeFromExtension() else when (extensionType) {
            ExtensionType.ARCHIVE.value -> ExtensionType.ARCHIVE
            ExtensionType.AUDIO.value -> ExtensionType.AUDIO
            ExtensionType.CODE.value -> if (isBookmark()) ExtensionType.URL else ExtensionType.CODE
            ExtensionType.FONT.value -> ExtensionType.FONT
            ExtensionType.IMAGE.value -> ExtensionType.IMAGE
            ExtensionType.PDF.value -> ExtensionType.PDF
            ExtensionType.PRESENTATION.value -> ExtensionType.PRESENTATION
            ExtensionType.SPREADSHEET.value -> ExtensionType.SPREADSHEET
            ExtensionType.TEXT.value -> ExtensionType.TEXT
            ExtensionType.VIDEO.value -> ExtensionType.VIDEO
            ExtensionType.FORM.value -> ExtensionType.FORM
            else -> when {
                isFolder() -> ExtensionType.FOLDER
                isBookmark() -> ExtensionType.URL
                else -> ExtensionType.UNKNOWN
            }
        }
    }

    fun getLastModifiedAt(): Date {
        return Date(getLastModifiedInMilliSecond())
    }

    fun getLastModifiedInMilliSecond() = lastModifiedAt * 1000

    fun getAddedAt(): Date {
        return Date(addedAt * 1000)
    }

    fun getFileCreatedAt(): Date {
        return Date(createdAt * 1000)
    }

    fun getDeletedAt(): Date {
        return Date(deletedAt * 1000)
    }

    fun getFileName(): String {
        val fileExtension = getFileExtension() ?: ""
        return when {
            fileExtension.isBlank() || isFolder() -> name
            else -> name.substringBeforeLast(fileExtension)
        }
    }

    fun getRemotePath(userDrive: UserDrive = UserDrive()): String {
        return if (path.isBlank() && id != ROOT_ID) FileController.generateAndSavePath(id, userDrive) else path
    }

    fun getFileExtension(): String? {
        val extension = name.substringAfterLast('.')
        return if (extension == name) null else ".$extension"
    }

    fun isBookmark() = name.isUrlFile() || name.isWeblocFile()

    fun isPendingUploadFolder() = isFromUploads && isFolder()

    fun isObsolete(dataFile: IOFile): Boolean {
        return (dataFile.lastModified() / 1000) < lastModifiedAt
    }

    fun isIntactFile(dataFile: IOFile): Boolean {
        return dataFile.length() == size
    }

    fun isObsoleteOrNotIntact(dataFile: IOFile): Boolean {
        return isObsolete(dataFile) || !isIntactFile(dataFile)
    }

    fun getStoredFile(context: Context, userDrive: UserDrive = UserDrive()): IOFile? {
        return if (isOffline || isInOfflineFolder) getOfflineFile(context, userDrive.userId) else getCacheFile(context, userDrive)
    }

    fun canUseStoredFile(context: Context, userDrive: UserDrive = UserDrive()): Boolean {
        return getStoredFile(context, userDrive)?.let(::isObsoleteOrNotIntact) == false
    }

    fun isOfflineFile(context: Context, userId: Int = AccountUtils.currentUserId, checkLocalFile: Boolean = true): Boolean {
        return isOffline || isInOfflineFolder
                || (checkLocalFile && !isFolder() && getOfflineFile(context, userId)?.exists() == true)
    }

    /**
     * State of the retained offline copy of this row (issues: round 17 #13/#14/#15, round 18
     * #13/#14) — the ONE fresh, scoped content-state decision used by every entry and
     * completion path. Three DISTINCT answers, never conflated:
     *  - [StoredOfflineCopyState.PROVEN_CURRENT]: the bytes are PROVEN to be the accepted
     *    remote generation — the record names this owner at this location and its COMPLETE
     *    generation identity (a KNOWN row version matching the record's, modification
     *    baseline, accepted revisedAt/updatedAt) matches the row's accepted metadata. An
     *    UNKNOWN row version is never currentness: a versionless R2 listing with R1's
     *    coarse identity must not accept R1's record as proof of R2 — the copy stays
     *    readable and the authoritative version-bearing request resolves it (issue: round
     *    18 #13).
     *  - [StoredOfflineCopyState.READABLE_UNVERIFIED]: a complete copy exists and stays
     *    READABLE — judged against the generation IT was published under (its own
     *    provenance record's baseline, or the row's effective published baseline for legacy
     *    copies), so a retained, complete R1 copy remains available to reads after a newer
     *    backward-dated R2 changes the accepted metadata (issue: round 18 #14). It is never
     *    proof of current content; verification is scheduled instead.
     *  - [StoredOfflineCopyState.ABSENT_OR_STALE]: nothing usable at the path — missing,
     *    truncated, or bytes whose own generation does not cohere.
     */
    fun storedOfflineCopyState(context: Context, offlineFile: IOFile, userDrive: UserDrive? = null): StoredOfflineCopyState {
        if (!(isOffline || isInOfflineFolder)) return StoredOfflineCopyState.ABSENT_OR_STALE
        // EXISTENCE AND POSITIVE OWNERSHIP FIRST (issues: round 20 #12, round 19 #13): a
        // copy backing a pending or retained-unsent upload is the usable local content of
        // an unsent edit — recognized by its ORIGINAL URI and by DURABLE SNAPSHOT
        // ownership (a protected original whose record owns it through its snapshot uri,
        // not through this path's uri) — explicitly readable without generation proof,
        // never "absent".
        if (!offlineFile.exists()) return StoredOfflineCopyState.ABSENT_OR_STALE
        val protectedCopyUri = Uri.fromFile(offlineFile).toString()
        val snapshotOwned = userDrive?.let { scope ->
            UploadFile.hasDurableOriginalRevisionOwnership(
                fileName = name,
                remoteFolder = parentId,
                driveId = scope.driveId,
                userId = scope.userId,
                fileModifiedAt = Date(offlineFile.lastModified()),
                fileSize = offlineFile.length(),
                contentHash = UploadFile.sha256OrNull(offlineFile),
                originalFileId = id,
            )
        } == true
        if (UploadFile.hasPendingUpload(uri = protectedCopyUri) ||
            UploadFile.hasRetainedUnsentUpload(uri = protectedCopyUri) ||
            snapshotOwned
        ) {
            return StoredOfflineCopyState.READABLE_UNVERIFIED
        }
        val provenance = DownloadOfflineFileManager.readPublishedProvenance(context, offlineFile)
        // OWNER identity: a record that POSITIVELY names a different owner is a CONFLICT
        // (issues: round 19 #12, round 20 #11) — never a readable copy of this row.
        if (provenance?.positivelyForeignTo(this, userDrive) == true) return StoredOfflineCopyState.FOREIGN_CONFLICT
        // IMMUTABLE BYTE IDENTITY (issues: round 20 #3, round 20 #12): a hash-ful record
        // whose content hash does NOT match the current bytes describes DIFFERENT content
        // — the copy is truncated, edited or stale beyond its own generation claim:
        // nothing usable here. This closes the same-second same-size edit hole (the
        // seconds-resolution mtime comparison alone still matched).
        if (provenance != null && provenance.contentHash != null && !provenance.certifiesBytes(offlineFile)) {
            return StoredOfflineCopyState.ABSENT_OR_STALE
        }
        // PRESENCE: a hash-ful record that CERTIFIES the bytes is valid regardless of
        // length (a valid ZERO-BYTE generation included — issue: round 20 #12); hashless
        // legacy copies need non-empty bytes for a non-empty row (unknown size
        // conservative — it never proves the row non-empty).
        if (provenance?.contentHash == null && offlineFile.length() <= 0L && (size ?: 0L) > 0L) {
            return StoredOfflineCopyState.ABSENT_OR_STALE
        }
        // OWN-GENERATION coherence (issue: round 18 #14): the copy's usability is judged
        // against the generation it was published under — never against current remote
        // metadata alone, which a backward-dated newer revision changes without the bytes
        // becoming unusable.
        val ownGeneration = provenance?.lastModifiedAt ?: effectivePublishedBaseline
        if ((offlineFile.lastModified() / 1_000L) != ownGeneration) return StoredOfflineCopyState.ABSENT_OR_STALE
        if (provenance == null) return StoredOfflineCopyState.READABLE_UNVERIFIED
        // STRICT OWNER IDENTITY (issue: round 20 #11): CURRENTNESS requires the caller's
        // CAPTURED ACCOUNT — a null scope never grants currentness; a scopeless row match
        // stays readable-unverified at most.
        if (!provenance.identifies(this, userDrive)) return StoredOfflineCopyState.READABLE_UNVERIFIED
        val rowVersion = version?.number ?: 0
        // UNKNOWN GENERATION IS NEVER PROOF (issue: round 18 #13).
        if (rowVersion <= 0) return StoredOfflineCopyState.READABLE_UNVERIFIED
        if (provenance.version != rowVersion) return StoredOfflineCopyState.READABLE_UNVERIFIED
        // A record that cannot name the row's KNOWN revision identity does not prove
        // currentness (issue: round 18 #13) — missing/nonpositive recorded fields no longer
        // bypass the comparisons.
        if (revisedAt > 0 && provenance.revisedAt != revisedAt) return StoredOfflineCopyState.READABLE_UNVERIFIED
        if (updatedAt > 0 && provenance.updatedAt != updatedAt) return StoredOfflineCopyState.READABLE_UNVERIFIED
        if (provenance.lastModifiedAt != lastModifiedAt) return StoredOfflineCopyState.READABLE_UNVERIFIED
        // CURRENTNESS still requires the copy to be INTACT against the row's accepted size
        // (issue: round 19 #13): readability never uses the remote size, the proof does.
        if (!isIntactFile(offlineFile)) return StoredOfflineCopyState.READABLE_UNVERIFIED
        return StoredOfflineCopyState.PROVEN_CURRENT
    }

    /**
     * Whether the retained offline copy is PROVEN to be the accepted remote generation —
     * the current-content proof used by completeness decisions (walk scheduling, worker fast
     * paths, completion validation). Never confers mere readability (issue: round 17 #15).
     */
    fun isOfflineAndProvenCurrent(context: Context, offlineFile: IOFile, userDrive: UserDrive? = null): Boolean =
        storedOfflineCopyState(context, offlineFile, userDrive) == StoredOfflineCopyState.PROVEN_CURRENT

    /**
     * Whether a usable, readable offline copy exists WITHOUT proving its generation — the
     * SAME fresh content-state decision (issue: round 18 #14), so access decisions
     * (previews, playback, SAF reads) keep a retained, complete copy available against its
     * OWN published generation: legacy pre-provenance bytes stay available immediately
     * after upgrade (issue: round 17 #15), and a newer metadata change alone never renders
     * a complete retained copy unreadable. The copy's generation is unverified and
     * verification is scheduled separately.
     */
    fun isOfflineAndReadable(context: Context, offlineFile: IOFile, userDrive: UserDrive?): Boolean {
        // CAPTURED ACCOUNT SCOPE IS REQUIRED (issue: round 20 #11): the caller's scope
        // rides every readability decision — a null scope can never verify account
        // ownership, so it never grants currentness and foreignness detection weakens to
        // row/drive only. Every reader (previews, playback, SAF, worker) passes its
        // captured scope.
        val state = storedOfflineCopyState(context, offlineFile, userDrive)
        // EXPLICIT readable states (issue: round 19 #12): a positively foreign copy is a
        // conflict — display and completion never serve another identity's bytes as this
        // row's readable copy; the ownership decision precedes the presentation.
        return state == StoredOfflineCopyState.PROVEN_CURRENT || state == StoredOfflineCopyState.READABLE_UNVERIFIED
    }

    fun getConvertedPdfCache(context: Context, userDrive: UserDrive): IOFile {
        val folder = IOFile(context.cacheDir, "converted_pdf/${userDrive.userId}/${userDrive.driveId}")
        if (!folder.exists()) folder.mkdirs()
        return IOFile(folder, id.toString())
    }

    fun getOfflineFile(context: Context, userId: Int = AccountUtils.currentUserId): IOFile? {
        val userDrive = UserDrive(userId, driveId)
        val rootFolder = IOFile(getOfflineFolder(context), "${userId}/$driveId")
        val path = getRemotePath(userDrive)

        if (path.isEmpty()) return null

        val child = if (isFolder()) path else path.substringBeforeLast("/")
        val folder = IOFile(rootFolder, child)

        if (!folder.exists()) folder.mkdirs()
        return if (isFolder()) folder else IOFile(folder, name)
    }

    fun getCacheFile(context: Context, userDrive: UserDrive = UserDrive()): IOFile {
        val folder = IOFile(context.cacheDir, "cloud_storage/${userDrive.userId}/${userDrive.driveId}")
        if (!folder.exists()) folder.mkdirs()
        return IOFile(folder, id.toString())
    }

    fun deleteCaches(context: Context, userId: Int = AccountUtils.currentUserId) {
        if (isOffline || isInOfflineFolder) {
            getOfflineFile(context, userId)?.apply {
                // Never drop the bytes backing a pending upload when the cache row goes away
                // (issue #1902); the upload still reads from this file.
                if (exists() && !UploadFile.hasPendingUpload(uri = Uri.fromFile(this).toString())) {
                    // The published generation's record dies with the bytes (issue: round 18
                    // #12): every byte-deletion path clears matching provenance — an orphaned
                    // record must never lend its generation to bytes later published here.
                    DownloadOfflineFileManager.deletePublishedProvenance(context, this)
                    delete()
                }
            }
        } else {
            getCacheFile(context).apply {
                if (exists()) {
                    DownloadOfflineFileManager.deletePublishedProvenance(context, this)
                    delete()
                }
            }
        }
    }

    private fun getPublicShareCache(context: Context): IOFile {
        val folder = IOFile(context.filesDir, context.getString(R.string.EXPOSED_PUBLIC_SHARE_DIR))
        if (!folder.exists()) folder.mkdirs()
        return IOFile(folder, name)
    }

    fun isDisabled(): Boolean {
        return rights?.canRead == false && rights?.canShow == false
    }

    fun isImporting(): Boolean {
        return externalImport?.let {
            it.status == FileExternalImportStatus.IN_PROGRESS.value
                    || it.status == FileExternalImportStatus.WAITING.value
                    || isCancelingImport()
        } ?: false
    }

    fun isCancelingImport() = externalImport?.status == FileExternalImportStatus.CANCELING.value

    fun isChildOfCommonDirectory(): Boolean {
        return hasVisibilities(VisibilityType.IS_IN_TEAM_SPACE_FOLDER, VisibilityType.IS_TEAM_SPACE_FOLDER)
    }

    fun hasVisibilities(vararg types: VisibilityType) = getVisibilityType() in types

    fun getWorkerTag() = "${id}_$driveId"

    fun getMimeType(): String = name.guessMimeType()

    enum class Type(val value: String) {
        FILE("file"),
        DIRECTORY("dir"),
    }

    fun getVisibilityType(): VisibilityType {
        if (dropbox != null) return VisibilityType.IS_DROPBOX
        return when (visibility) {
            "is_root" -> VisibilityType.ROOT
            "is_team_space" -> VisibilityType.IS_TEAM_SPACE
            "is_team_space_folder" -> VisibilityType.IS_TEAM_SPACE_FOLDER
            "is_in_team_space_folder" -> VisibilityType.IS_IN_TEAM_SPACE_FOLDER
            "is_shared_space" -> VisibilityType.IS_SHARED_SPACE
            "is_in_shared_space" -> VisibilityType.IS_IN_SHARED_SPACE
            "is_in_private_space" -> VisibilityType.IS_IN_PRIVATE_SPACE
            IS_PRIVATE_SPACE -> VisibilityType.IS_PRIVATE
            else -> VisibilityType.UNKNOWN
        }
    }

    fun getDisplayName(context: Context): String {
        return when (getVisibilityType()) {
            VisibilityType.IS_PRIVATE -> context.getString(R.string.localizedFilenamePrivateTeamSpace)
            VisibilityType.IS_TEAM_SPACE -> context.getString(R.string.localizedFilenameTeamSpace)
            else -> name
        }
    }

    // TODO This function is called in the FileAdapter, for each File, and is getting the RealmInstance each time. This is not very efficient.
    fun getCategories(): List<Category> {
        val fileCategoriesIds = getSortedCategoriesIds()
        return DriveInfosController.getCategoriesFromIds(driveId, fileCategoriesIds.toTypedArray())
    }

    private fun getSortedCategoriesIds(): List<Int> {
        return if (isManaged) {
            categories.sort(FileCategory::addedAt.name).map { it.categoryId }
        } else {
            runCatching {
                // Because RealmList.sort will crash in case objects are not managed.
                categories.sortedBy { it.addedAt }.map { it.categoryId }
            }.onFailure { exception ->
                SentryLog.e("getSortedCategoriesIds", "Failed to do sortBy on unmanaged RealmList??", exception) { scope ->
                    scope.setExtra("categories", categories.joinToString { "id: ${it.categoryId} addedAt: ${it.addedAt}" })
                }
            }.getOrDefault(emptyList())
        }
    }

    fun isAllowedToBeColored() = rights?.colorable ?: false

    fun hasCreationRight() = isFolder() && rights?.canCreateFile == true

    fun createdByCurrentUser() = createdBy == AccountUtils.currentUserId

    // For applyFileActivity in FileController
    override fun equals(other: Any?): Boolean {
        if (other is File) {
            return isUsable() && other.isUsable() && other.id == id
        }
        return super.equals(other)
    }

    override fun hashCode(): Int {
        var result = id
        result = 31 * result + driveId
        return result
    }

    enum class VisibilityType {
        ROOT,
        IS_PRIVATE,
        IS_DROPBOX,
        IS_SHARED_SPACE,
        IS_TEAM_SPACE,
        IS_TEAM_SPACE_FOLDER,
        IS_IN_TEAM_SPACE_FOLDER,
        IS_IN_SHARED_SPACE,
        IS_IN_PRIVATE_SPACE,
        UNKNOWN,
    }

    enum class Office(val extensionType: ExtensionType, val extension: String) {
        DOCS(ExtensionType.TEXT, "docx"),
        POINTS(ExtensionType.PRESENTATION, "pptx"),
        GRIDS(ExtensionType.SPREADSHEET, "xlsx"),
        FORM(ExtensionType.FORM, "docxf"),
        TXT(ExtensionType.TEXT, "txt")
    }

    enum class SortType(val order: String, val orderBy: String, val translation: Int) {
        NAME_AZ("asc", "name", R.string.sortNameAZ),
        NAME_ZA("desc", "name", R.string.sortNameZA),
        OLDER("asc", "last_modified_at", R.string.sortOlder),
        RECENT("desc", "last_modified_at", R.string.sortRecent),
        OLDER_TRASHED("asc", "deleted_at", R.string.sortOlder),
        RECENT_TRASHED("desc", "deleted_at", R.string.sortRecent),
        OLDEST_ADDED("asc", "added_at", R.string.sortOldestAdded),
        MOST_RECENT_ADDED("desc", "added_at", R.string.sortMostRecentAdded),
        SMALLER("asc", "size", R.string.sortSmaller),
        BIGGER("desc", "size", R.string.sortBigger),
        LEAST_RELEVANT("asc", "relevance", R.string.sortLeastRelevant),
        MOST_RELEVANT("desc", "relevance", R.string.sortMostRelevant),
        // EXTENSION("asc", "extension", R.string.sortExtension), // TODO: Awaiting API
    }

    enum class SortTypeUsage { FILE_LIST, TRASH, SEARCH }

    @Parcelize
    enum class FolderPermission(
        override val icon: Int,
        override val translation: Int,
        override val description: Int
    ) : Permission {
        ONLY_ME(
            R.drawable.ic_account,
            R.string.createFolderMeOnly,
            R.string.createFolderMeOnly
        ),
        INHERIT(
            R.drawable.ic_users,
            R.string.createFolderKeepParentsRightTitle,
            R.string.createFolderKeepParentsRightDescription
        ),
        SPECIFIC_USERS(
            R.drawable.ic_users,
            R.string.createFolderSomeUsersTitle,
            R.string.createFolderSomeUsersDescription
        ),
        ALL_DRIVE_USERS(
            R.drawable.ic_drive,
            R.string.allAllDriveUsers,
            R.string.createCommonFolderAllUsersDescription
        )
    }

    enum class SupportedByType(val apiValue: String) {
        THUMBNAIL("thumbnail"),
        ONLYOFFICE("onlyoffice"),
        KMAIL("kmail")
    }

    suspend fun convertToIOFile(
        context: Context,
        userDrive: UserDrive = UserDrive(),
        shouldBePdf: Boolean = false,
        onProgress: (progress: Int) -> Unit,
        navigateToDownloadDialog: (suspend () -> Unit)? = null,
    ): IOFile {
        val fileNeedDownload: Boolean
        val cacheFile: IOFile
        val isPublicShared: Boolean

        withContext(Dispatchers.IO) {
            isPublicShared = isPublicShared()
            cacheFile = when {
                isPublicShared -> getPublicShareCache(context)
                isOnlyOfficePreview() -> getConvertedPdfCache(context, userDrive)
                isOffline || isInOfflineFolder -> getOfflineFile(context, userDrive.userId)!!
                else -> getCacheFile(context, userDrive)
            }

            fileNeedDownload = when {
                isOnlyOfficePreview() -> isObsolete(cacheFile)
                // UNIFIED fresh content-state decision at the conversion entry (issue: round
                // 18 #14): the previous isObsoleteOrNotIntact heuristic accepted same-size
                // old bytes whose mtime satisfied a newer BACKWARD-DATED revision (R1 at
                // 100 vs revision at 90), bypassing the provenance model entirely; and a
                // newer metadata change alone no longer forces a re-download of a complete
                // retained copy, which stays readable against its own generation.
                isOffline || isInOfflineFolder -> when (storedOfflineCopyState(context, cacheFile, userDrive)) {
                    StoredOfflineCopyState.PROVEN_CURRENT -> false
                    StoredOfflineCopyState.READABLE_UNVERIFIED -> {
                        // Usable retained copy: serve it now; its generation is verified
                        // separately (issues: round 18 #14/#15).
                        OfflineFolderController.scheduleOfflineVerification(
                            fileId = id,
                            userDrive = userDrive,
                            observedGeneration = cacheFile.lastModified() / 1_000L,
                        )
                        false
                    }
                    // POSITIVELY FOREIGN provenance (issue: round 19 #12): a fresh download
                    // is attempted; the coordinated publication reports the conflict — the
                    // foreign bytes are never served nor overwritten in place.
                    StoredOfflineCopyState.FOREIGN_CONFLICT -> true
                    StoredOfflineCopyState.ABSENT_OR_STALE -> true
                }
                else -> isObsoleteOrNotIntact(cacheFile)
            }
        }

        if (fileNeedDownload) {
            // Ownership/publication mode follows the SELECTED destination, not merely the
            // original file's flags (issue: round 9 #5): a converted PDF cache or a
            // public-share cache is not the owned offline copy — running it through the
            // offline publication would find no owner for that path and permanently skip the
            // refresh. Only an actual owned offline destination receives the offline
            // preservation checks; every cache destination uses the plain staged rename.
            val destinationIsOwnedOfflineCopy =
                (isOffline || isInOfflineFolder) && !isPublicShared && !isOnlyOfficePreview()
            if (destinationIsOwnedOfflineCopy) {
                // An offline-owned path is replaced only through the shared preservation policy
                // (issue: round 7 #3): size mismatch alone never deletes an inherited edit. A
                // copy backing an upload (or whose snapshot failed) stays the preview source; a
                // dirty edit is snapshotted before the replacement.
                if (OfflineFolderController.decisionForReplacingOwnedCopy(this, cacheFile, userDrive) ==
                    OfflineFolderController.OwnedCopyReplacementDecision.KEEP_LOCAL
                ) {
                    return cacheFile
                }
            }
            navigateToDownloadDialog?.invoke()
            val outcome = downloadFile(
                context,
                cacheFile,
                file = this,
                shouldBePdf,
                onProgress,
                isPublicShared,
                userDrive = userDrive,
                coordinated = destinationIsOwnedOfflineCopy,
            )
            // Exhaustive outcome handling in the public caller (issue: round 10 #8): a skip
            // is never silently reported as a fresh download of a valid destination.
            when (outcome) {
                PublicationOutcome.PUBLISHED, PublicationOutcome.SKIPPED_PROTECTED -> Unit
                // UNRESOLVED generation (issue: round 20 #14): bytes were published but no
                // generation proof exists — the same usable-destination validation as a
                // supersession decides whether the copy may serve.
                PublicationOutcome.SKIPPED_SUPERSEDED, PublicationOutcome.PUBLISHED_UNRESOLVED -> {
                    // Usable only when the CURRENT row still owns the named destination AND
                    // that destination holds usable content (issue: round 12 #11): metadata
                    // ownership alone reports completion over a missing or obsolete retained
                    // copy — a newer generation can supersede an in-flight preview before any
                    // usable bytes exist at the unchanged path.
                    if (destinationIsOwnedOfflineCopy) {
                        // Validation under the shared protocol (issue: round 13 #11): intent
                        // monitor → file lock → refreshed row, so the baseline the retained
                        // bytes are judged against cannot move between the read and the
                        // decision.
                        val usable = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                            OfflineFileCoordination.withFileLock(userDrive, id) {
                                val liveRow = FileController.getRealmInstance(userDrive).use { realm ->
                                    runCatching { realm.refresh() }
                                    // DEPTH-ONE detached copy (issue: round 17 #14): the version must survive — a
                                    // depth-zero copy turns a KNOWN mismatching generation
                                    // into the permissive unknown-version case.
                                    FileController.getFileById(realm, id)?.let { live -> realm.copyFromRealm(live, 1) }
                                }
                                val livePath = liveRow?.getOfflineFile(context, userDrive.userId)?.path
                                val targetUri = Uri.fromFile(cacheFile).toString()
                                liveRow != null &&
                                    (liveRow.isOffline || liveRow.isInOfflineFolder) &&
                                    livePath == cacheFile.path &&
                                    cacheFile.exists() &&
                                    // An EXACT current clean baseline (issue: round 13 #11):
                                    // "not older" accepts retained bytes whose timestamp is
                                    // NEWER than a backward-dated newer baseline (R1 at 100 vs
                                    // baseline 90) — only equality with the row's own
                                    // modification time and size, or PRE-EXISTING durable
                                    // upload ownership, makes the retained bytes usable.
                                    // READ-ONLY ownership inspection (issue: round 14 #4): a
                                    // comparison against the REPLACEMENT baseline would
                                    // classify the old generation's clean bytes as an edit
                                    // and the mutating decision would register an upload
                                    // that can restore old content over the newer
                                    // generation — no snapshot is manufactured here.
                                    (liveRow.isOfflineAndProvenCurrent(context, cacheFile, userDrive) ||
                                        UploadFile.hasPendingUpload(uri = targetUri) ||
                                        UploadFile.hasRetainedUnsentUpload(uri = targetUri) ||
                                        // Durable SNAPSHOT registrations own the original
                                        // revision under unique uris the uri queries cannot
                                        // see (issue: round 15 #7): a read-only identity,
                                        // modification-date and recorded-size match on the
                                        // scoped original revision establishes the same
                                        // ownership without creating any snapshot.
                                        UploadFile.hasDurableOriginalRevisionOwnership(
                                            fileName = liveRow.name,
                                            remoteFolder = liveRow.parentId,
                                            driveId = userDrive.driveId,
                                            userId = userDrive.userId,
                                            fileModifiedAt = Date(cacheFile.lastModified()),
                                            fileSize = cacheFile.length(),
                                            contentHash = UploadFile.sha256OrNull(cacheFile),
                                            originalFileId = liveRow.id,
                                        ))
                            }
                        }
                        if (!usable) {
                            throw java.io.IOException("Superseded preview left no usable destination for $name")
                        }
                    } else if (!cacheFile.exists()) {
                        // Id-derived cache destinations cannot be occupied by another file's
                        // row; only a vanished destination is unusable.
                        throw java.io.IOException("Superseded preview left no usable destination for $name")
                    }
                    onProgress(PROGRESS_COMPLETE)
                }
                PublicationOutcome.SKIPPED_CONFLICT ->
                    // The destination holds protected content owned by another row (issue:
                    // round 14 #13): deferred — never a success over content this preview
                    // cannot establish as F's own.
                    throw java.io.IOException("Preview destination conflict for $name")
                PublicationOutcome.SKIPPED_INVALIDATED ->
                    // The offline intent or destination disappeared mid-download (root
                    // removal): error/deferred — never a success naming a possibly deleted file.
                    throw java.io.IOException("Preview download invalidated: the offline intent for $name disappeared")
                PublicationOutcome.FAILED -> throw java.io.IOException("Preview download failed for $name")
            }
            // NO unconditional timestamp write here (issue: round 9 #1): the publication
            // timestamps the staged bytes before a successful rename, and a skipped transfer
            // (an editor's newer revision, a superseded generation) must never touch the
            // retained destination's timestamp — that write re-classified a protected edit as
            // clean against the remote baseline.
        }

        return cacheFile
    }

    companion object {
        const val IS_PRIVATE_SPACE = "is_private_space"

        /**
         * This method is here, and not directly a class method in the File class, because of a supposed Realm bug.
         * When we try to put it in the File class, the app doesn't build anymore, because of a "broken method".
         * This is not the only method in this case, search this comment in the project, and you'll see.
         * Realm's Github issue: https://github.com/realm/realm-java/issues/7637
         */
        fun File.getCloudAndFileUris(context: Context, userDrive: UserDrive = UserDrive()): Pair<Uri, Uri> {
            val cloudUri = CloudStorageProvider.createShareFileUri(context, this, userDrive)!!
            val offlineFile = getOfflineFile(context, userDrive.userId)

            return cloudUri to if ((isOffline || isInOfflineFolder) && offlineFile != null) {
                // We use the uri with scheme file because Microsoft Office don't support modifications from a content uri
                if (isOnlyOfficePreview()) {
                    offlineFile.toUri()
                } else {
                    FileProvider.getUriForFile(context, context.getString(R.string.FILE_AUTHORITY), offlineFile)
                }
            } else {
                cloudUri
            }
        }

        /**
         * This method is here, and not directly a class method in the File class, because of a supposed Realm bug.
         * When we try to put it in the File class, the app doesn't build anymore, because of a "broken method".
         * This is not the only method in this case, search this comment in the project, and you'll see.
         * Realm's Github issue: https://github.com/realm/realm-java/issues/7637
         */
        fun getOfflineFolder(context: Context): IOFile {
            val mediaFolder = context.externalMediaDirs?.firstOrNull() ?: context.filesDir
            return IOFile(mediaFolder, context.getString(R.string.EXPOSED_OFFLINE_DIR))
        }

        /**
         * This method is here, and not directly a class method in the File class, because of a supposed Realm bug.
         * When we try to put it in the File class, the app doesn't build anymore, because of a "broken method".
         * This is not the only method in this case, search this comment in the project, and you'll see.
         * Realm's Github issue: https://github.com/realm/realm-java/issues/7637
         */
        fun File.getFileTypeFromExtension(): ExtensionType {
            return when (getMimeType()) {
                in Regex("application/(zip|rar|x-tar|.*compressed|.*archive)") -> ExtensionType.ARCHIVE
                in Regex("audio/") -> ExtensionType.AUDIO
                in Regex("image/") -> ExtensionType.IMAGE
                in Regex("/pdf") -> ExtensionType.PDF
                in Regex("presentation") -> ExtensionType.PRESENTATION
                in Regex("spreadsheet|excel|comma-separated-values") -> ExtensionType.SPREADSHEET
                in Regex("document|text/plain|msword") -> ExtensionType.TEXT
                in Regex("video/") -> ExtensionType.VIDEO
                in Regex("text/|application/") -> ExtensionType.CODE
                else -> if (getFileExtension() == ".${Office.FORM.extension}") ExtensionType.FORM else ExtensionType.UNKNOWN
            }
        }
    }
}

/** State of a row's retained offline copy — see [File.storedOfflineCopyState] (round 17 #13/#14/#15). */
enum class StoredOfflineCopyState {
    PROVEN_CURRENT,
    READABLE_UNVERIFIED,

    /** The provenance record POSITIVELY names a different owner at this location (issue:
     * round 19 #12): the bytes are another identity's content — a conflict, never a
     * readable copy of this row. Display and completion must not serve them; destructive
     * operations preserve them until the conflict resolves. */
    FOREIGN_CONFLICT,
    ABSENT_OR_STALE,
}
