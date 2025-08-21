-- Script to run Server and Load Generator with JFR in iTerm2

tell application "iTerm2"
    set scriptPath to POSIX path of (path to me)
    set loadBalancerDir to scriptPath & "../network-load-balancer"

    create window with default profile
    tell current session of current window
        split horizontally with default profile
        delay 0.5 -- Wait for the third session to be ready
        tell the second session
            write text "cd " & scriptPath & " && ./gradlew app:run " &  ¬
            "--jvm-args=\"-XX:StartFlightRecording=filename=recording.jfr,duration=1m\""
        end tell
        split vertically with default profile
        delay 0.5 -- Wait for the third session to be ready
        tell the third session
            write text "cd " & loadBalancerDir & " && java " &  ¬
            "-cp build/libs/network-load-generator-0.6.0.jar " &  ¬
            "com.hedera.benchmark.NftTransferLoadTest"
        end tell
    end tell
end tell