(load-file "/home/craig/workspace/demand-trend-reps/core.clj")
(ns marathon.analysis.random)
;;initial lifecycle lengths for each compo
(def compo-lengths {"AC" 1095 "RC" 2190 "NG" 2190})
(def path
  "/home/craig/runs/rand-runs-demand-trends/base-testdata-v7.xlsx")
;;collect trends over 7 reps.
(rand-runs-dtrends path :reps 7 :compo-lengths compo-lengths)
