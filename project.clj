(defproject org.onyxplatform/onyx-bookkeeper "0.11.0.0"
  :description "Onyx plugin for BookKeeper"
  :url "https://github.com/onyx-platform/onyx-bookkeeper"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.apache.bookkeeper/bookkeeper-server "4.4.0" :exclusions [org.slf4j/slf4j-log4j12]]
                 ;[org.apache.bookkeeper/bookkeeper "4.4.0" :exclusions [org.slf4j/slf4j-log4j12]]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.11.0"]]
  :jvm-opts ^:replace ["-Xmx3g"]
  :profiles {:debug 
             {:jvm-opts ^:replace ["-server"
                                   "-Xmx3000M"
                                   "-XX:+UseG1GC" 
                                   "-XX:-OmitStackTraceInFastThrow" 
                                   "-XX:+UnlockCommercialFeatures"
                                   "-XX:+FlightRecorder"
                                   "-XX:StartFlightRecording=duration=1080s,filename=recording.jfr"]}
             :dev {:plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g" "-server"]}})
