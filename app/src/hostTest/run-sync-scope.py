#!/usr/bin/env python3
"""Offline focused JUnit run against actual androidTest guards, without Android startup.

The host source set is deliberately independent of Gradle's production/unit-test
classpath: androidTest diagnostic classes must never become app dependencies.
Requires the already cached, aligned JUnit 6.1.3 jars and a host JDK.
"""
from pathlib import Path
import os
import subprocess
import tempfile

repo = Path(__file__).resolve().parents[3]
cache = Path.home() / ".gradle/caches/modules-2/files-2.1"
jars = []
for name in ("junit-jupiter-api", "junit-jupiter-engine", "junit-platform-engine",
             "junit-platform-launcher", "junit-platform-commons", "opentest4j", "apiguardian-api"):
    candidates = sorted(cache.glob(f"*/{name}/*/*/*.jar"))
    if name.startswith("junit-"):
        candidates = [p for p in candidates if p.parents[1].name == "6.1.3"]
    if not candidates:
        raise SystemExit(f"Required cached host dependency missing: {name}")
    jars.append(str(candidates[-1]))
classpath = os.pathsep.join(jars)
output = Path(tempfile.mkdtemp(prefix="sync-scope-host-"))
runner = output / "RunHost.java"
runner.write_text('''import org.junit.platform.launcher.core.*;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
public class RunHost {
 public static void main(String[] args) {
  var request=LauncherDiscoveryRequestBuilder.request().selectors(selectClass("com.infomaniak.drive.data.cache.copyreader.SyncScopeGuardTest")).build();
  var listener=new SummaryGeneratingListener(); var launcher=LauncherFactory.create();launcher.registerTestExecutionListeners(listener);launcher.execute(request);
  var s=listener.getSummary();s.printTo(new java.io.PrintWriter(System.out));s.printFailuresTo(new java.io.PrintWriter(System.out));
  if(s.getTestsFoundCount()!=26 || s.getTestsStartedCount()!=26 || s.getTestsSucceededCount()!=26 || s.getTestsFailedCount()!=0 || s.getTestsSkippedCount()!=0 || s.getTestsAbortedCount()!=0)System.exit(1);
 }
}''')
sources = repo / "app/src/androidTest/java/com/infomaniak/drive/data/cache/copyreader"
subprocess.run(["javac", "-cp", classpath, "-d", str(output),
                *[str(sources / n) for n in ("CopyInputGuard.java", "AggregateCopyGuard.java", "SyncScopeGuard.java")],
                str(Path(__file__).parent / "java/com/infomaniak/drive/data/cache/copyreader/SyncScopeGuardTest.java"),
                str(runner)], check=True)
subprocess.run(["java", "-cp", str(output) + os.pathsep + classpath, "RunHost"], check=True)
print(f"Host artifacts: {output}")
