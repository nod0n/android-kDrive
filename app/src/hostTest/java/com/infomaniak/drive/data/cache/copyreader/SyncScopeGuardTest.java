package com.infomaniak.drive.data.cache.copyreader;

import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.io.File;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Synthetic controls compiled with the exact diagnostic guard sources on the host. */
public class SyncScopeGuardTest {
    private final SyncScopeGuard.Scope scope = new SyncScopeGuard.Scope(7, 8, 9);
    private final SyncScopeGuard.Folder folder = new SyncScopeGuard.Folder(-42, "/synthetic/");
    private SyncScopeGuard.Setting setting(SyncScopeGuard.Scope s) { return new SyncScopeGuard.Setting(s, true, false, false, true); }
    private Map<String,Object> project(List<SyncScopeGuard.Setting> settings, SyncScopeGuard.Scope expected,
                                      List<SyncScopeGuard.Folder> selected, List<SyncScopeGuard.Folder> wanted) {
        return SyncScopeGuard.project(0, settings, expected, selected, wanted);
    }
    private Map<String,Object> matching() { return project(List.of(setting(scope)), scope, List.of(folder), List.of(folder)); }
    @Test public void exactScopeAndFolder() {
        Map<String,Object> out = matching(); assertEquals(9, out.size()); assertEquals(true, out.get("expectedSyncScopeMatches"));
        assertEquals(true, out.get("exactExpectedSelectedFolderSetMatches")); assertEquals(1L, out.get("settingsRows"));
        assertEquals(false, out.get("syncVideo")); assertEquals(false, out.get("deleteAfterSync"));
        assertEquals(true, out.get("photoTrashSyncEnabled")); assertEquals(true, out.get("syncImmediately"));
    }
    @Test public void globalPendingAcrossUnrelatedScopes() {
        // Scope metadata deliberately varies but is never passed into the global predicate.
        List<boolean[]> dates = new ArrayList<>();
        for (long[] ignoredScope : List.of(new long[]{7,8,9}, new long[]{70,80,90}, new long[]{7,80,900})) {
            dates.add(new boolean[]{true,true});
        }
        dates.add(new boolean[]{false,true}); dates.add(new boolean[]{true,false}); dates.add(new boolean[]{false,false});
        assertEquals(3, SyncScopeGuard.pending(dates));
        assertEquals(3L, SyncScopeGuard.project(SyncScopeGuard.pending(dates), List.of(setting(scope)), scope, List.of(folder), List.of(folder)).get("pendingGlobal"));
    }
    @Test public void emptyQueue() { assertEquals(0, SyncScopeGuard.pending(List.of())); }
    @Test public void pendingRequiresBothNull() {
        assertEquals(1, SyncScopeGuard.pending(List.of(new boolean[]{true,true},new boolean[]{false,true},new boolean[]{true,false},new boolean[]{false,false})));
    }
    @Test public void wrongUser() { assertEquals(false, project(List.of(setting(new SyncScopeGuard.Scope(6,8,9))),scope,List.of(folder),List.of(folder)).get("expectedSyncScopeMatches")); }
    @Test public void wrongDrive() { assertEquals(false, project(List.of(setting(new SyncScopeGuard.Scope(7,6,9))),scope,List.of(folder),List.of(folder)).get("expectedSyncScopeMatches")); }
    @Test public void wrongDestination() { assertEquals(false, project(List.of(setting(new SyncScopeGuard.Scope(7,8,30))),scope,List.of(folder),List.of(folder)).get("expectedSyncScopeMatches")); }
    @Test public void missingScopeReference() { assertEquals("UNKNOWN", project(List.of(setting(scope)),null,List.of(folder),List.of(folder)).get("expectedSyncScopeMatches")); }
    @Test public void missingSettings() { Map<String,Object> out=project(List.of(),scope,List.of(folder),List.of(folder)); assertEquals(false,out.get("expectedSyncScopeMatches")); assertEquals("UNKNOWN",out.get("syncVideo")); }
    @Test public void extraSettings() { Map<String,Object> out=project(List.of(setting(scope),setting(scope)),scope,List.of(folder),List.of(folder)); assertEquals(2L,out.get("settingsRows")); assertEquals(false,out.get("expectedSyncScopeMatches")); assertEquals("UNKNOWN",out.get("photoTrashSyncEnabled")); }
    @Test public void invalidStoredScope() { assertEquals(false,project(List.of(setting(null)),scope,List.of(folder),List.of(folder)).get("expectedSyncScopeMatches")); }
    @Test public void missingFolderReference() { assertEquals("UNKNOWN",project(List.of(setting(scope)),scope,List.of(folder),null).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void missingFolder() { assertEquals(false,project(List.of(setting(scope)),scope,List.of(),List.of(folder)).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void extraFolder() { assertEquals(false,project(List.of(setting(scope)),scope,List.of(folder,new SyncScopeGuard.Folder(4,"/extra/")),List.of(folder)).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void wrongFolderId() { assertEquals(false,project(List.of(setting(scope)),scope,List.of(new SyncScopeGuard.Folder(4,"/synthetic/")),List.of(folder)).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void wrongFolderPath() { assertEquals(false,project(List.of(setting(scope)),scope,List.of(new SyncScopeGuard.Folder(-42,"/other/")),List.of(folder)).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void duplicateSelectedCannotHideMissing() { assertEquals(false,project(List.of(setting(scope)),scope,List.of(folder,folder),List.of(folder,new SyncScopeGuard.Folder(4,"/other/"))).get("exactExpectedSelectedFolderSetMatches")); }
    @Test public void duplicateExpectedRejected() { assertThrows(IllegalStateException.class,()->project(List.of(),scope,List.of(folder),List.of(folder,folder))); }
    @Test public void emptyExpectedRejected() { assertThrows(IllegalStateException.class,()->project(List.of(),scope,List.of(folder),List.of())); }
    @Test public void schemaTypeAndNullability() {
        SyncScopeGuard.field("DATE","DATE",true,true);
        assertThrows(IllegalStateException.class,()->SyncScopeGuard.field("STRING","DATE",true,true));
        assertThrows(IllegalStateException.class,()->SyncScopeGuard.field("DATE","DATE",false,true));
    }
    @Test public void exactSyncInputOnly() {
        SyncScopeGuard.input("SYNC","sync.realm","Sync.realm",13);
        for(String role:List.of("APP","DRIVES","FILES","")) assertThrows(IllegalStateException.class,()->SyncScopeGuard.input(role,"sync.realm","Sync.realm",13));
        assertThrows(IllegalStateException.class,()->SyncScopeGuard.input("SYNC","sync.realm","Sync.realm",12));
        assertThrows(IllegalStateException.class,()->SyncScopeGuard.input("SYNC","../Sync.realm","Sync.realm",13));
        assertThrows(IllegalStateException.class,()->SyncScopeGuard.input("SYNC","sync.realm","AppSettings.realm",13));
    }
    @Test public void closedOutputKeys() {
        Map<String,Object> out=matching(); out.put("id",42L); final Map<String,Object> extra=out; assertThrows(IllegalStateException.class,()->SyncScopeGuard.output(extra));
        out=matching(); out.remove("pendingGlobal"); final Map<String,Object> missing=out; assertThrows(IllegalStateException.class,()->SyncScopeGuard.output(missing));
    }
    @Test public void closedOutputScalarTypes() {
        for(Object bad:List.of("/private/path", List.of(true), Map.of("id",4), 42L)) {
            Map<String,Object> out=matching(); out.put("expectedSyncScopeMatches",bad); assertThrows(IllegalStateException.class,()->SyncScopeGuard.output(out));
        }
        Map<String,Object> out=matching();out.put("pendingGlobal",-1L); assertThrows(IllegalStateException.class,()->SyncScopeGuard.output(out));
    }
    @Test public void exactInputKeysAndEntry() {
        AggregateCopyGuard.keys(Set.of("file","protocol"),"file","protocol");
        assertThrows(IllegalStateException.class,()->AggregateCopyGuard.keys(Set.of("file","protocol","extra"),"file","protocol"));
        assertEquals(SyncScopeGuard.ENTRY,AggregateCopyGuard.entry(SyncScopeGuard.ENTRY));
        assertThrows(IllegalStateException.class,()->AggregateCopyGuard.entry("readSyncScopeProjectionOther"));
    }
    @Test public void fileAuthenticationAndNoTraversal() throws Exception {
        Path root=Files.createTempDirectory("sync-scope-host");File file=root.resolve("sync.realm").toFile();Files.write(file.toPath(),new byte[]{1,2,3});
        String sha=CopyInputGuard.sha256(file);CopyInputGuard.matches(file,3,sha);assertEquals(file,CopyInputGuard.child(root.toFile(),"sync.realm"));
        assertThrows(IllegalStateException.class,()->CopyInputGuard.matches(file,2,sha));
        assertThrows(IllegalStateException.class,()->CopyInputGuard.matches(file,3,"0".repeat(64)));
        assertThrows(IllegalStateException.class,()->CopyInputGuard.child(root.toFile(),"../sync.realm"));
        Files.createSymbolicLink(root.resolve("link.realm"),file.toPath());
        assertThrows(IllegalStateException.class,()->CopyInputGuard.child(root.toFile(),"link.realm"));
    }
    @Test public void fileTypeOwnerModeAndLinks() {
        CopyInputGuard.stat(0100600,1,23,23,false);
        for(int mode:List.of(0120600,0040600,0100644)) assertThrows(IllegalStateException.class,()->CopyInputGuard.stat(mode,1,23,23,false));
        assertThrows(IllegalStateException.class,()->CopyInputGuard.stat(0100600,2,23,23,false));
        assertThrows(IllegalStateException.class,()->CopyInputGuard.stat(0100600,1,24,23,false));
    }
}
