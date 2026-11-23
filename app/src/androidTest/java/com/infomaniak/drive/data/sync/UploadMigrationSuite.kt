/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync

import android.app.Instrumentation
import android.content.Context
import android.os.Bundle
import androidx.test.core.app.ApplicationProvider
import androidx.test.internal.runner.TestExecutor
import androidx.test.internal.runner.listener.InstrumentationResultPrinter
import androidx.test.platform.app.InstrumentationRegistry
import io.realm.Realm
import org.junit.runner.Description
import org.junit.runner.Request
import org.junit.runner.notification.Failure
import org.junit.runner.notification.RunListener
import org.junit.runners.model.RunnerBuilder
import splitties.init.injectAsAppCtx

/** Fixed synthetic fixture only; bootstrap precedes loading UploadFile's eager Realm configuration. */
object UploadMigrationSuite {
    @JvmStatic fun run(host: Instrumentation, context: Context): Bundle {
        InstrumentationRegistry.registerInstance(host, Bundle().apply {
            putString("class", "com.infomaniak.drive.data.sync.UploadMigration7to13Test")
            putString("disableAnalytics", "true")
            putString("runnerBuilder", "de.mannodermaus.junit5.AndroidJUnit5Builder")
        })
        check(ApplicationProvider.getApplicationContext<Context>() === context)
        context.injectAsAppCtx()
        Realm.init(context)
        val type = Class.forName("com.infomaniak.drive.data.sync.UploadMigration7to13Test", true, context.classLoader)
        val builder = Class.forName("de.mannodermaus.junit5.AndroidJUnit5Builder", true, context.classLoader)
            .getDeclaredConstructor().newInstance() as RunnerBuilder
        val runner = checkNotNull(builder.runnerForClass(type))
        var started = 0; var finished = 0; var failed = 0; var skipped = 0
        val audit = object : RunListener() {
            override fun testStarted(d: Description) { started++ }
            override fun testFinished(d: Description) { finished++ }
            override fun testFailure(f: Failure) { failed++ }
            override fun testIgnored(d: Description) { skipped++ }
            override fun testAssumptionFailure(f: Failure) { skipped++ }
        }
        val result = TestExecutor.Builder(host).addRunListener(InstrumentationResultPrinter()).addRunListener(audit)
            .build().execute(Request.runner(runner))
        return result.apply {
            putInt("migrationStarted", started); putInt("migrationFinished", finished)
            putInt("migrationFailed", failed); putInt("migrationSkipped", skipped)
            putBoolean("migrationPassed", started == 1 && finished == 1 && failed == 0 && skipped == 0)
        }
    }
}
