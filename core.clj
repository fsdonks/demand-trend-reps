;;This is intended to be loaded from the m4 repl.
;;We add demand trend results together for each src, demandgroup, and
;;t across multiple reps and then divide by the number of reps to get
;;the average stats per day.
(require 'marathon.analysis.random)
(ns marathon.analysis.random)
(require '[marathon.project :as project])

(defn project->demand-trends
  "Takes a project and returns demand trend records."
  [proj]
  (->> proj
       (a/load-context)
       (a/as-stream)
       (map (fn [[t ctx]]
              {:t t :ctx ctx}))
       (gen/time-weighted-samples :t)
       (mapcat (fn [[{:keys [t ctx]} t deltaT]]
                 (map (fn [r] (assoc r :deltaT deltaT :t t))
                      (util/demand-trends-exhaustive ctx))))
  ))

(defn rand-demand-trends
  "This function "
  [proj & {:keys [phases gen seed->randomizer]
           :or   {gen util/default-gen
                  seed->randomizer (fn [_] identity)}}]
   (let [project->experiments *project->experiments*]
     (->> (assoc proj :phases phases
                 :gen gen  :seed->randomizer :seed->randomizer)
          ;;paralell across SRCs
          (e/split-project)
          ;;will need to check to see if time-weighted-samples threads
          ;;share state before I go async.
          ;;(util/pmap! *threads*
          (map
                      (fn [[src proj]]
                        (let [rep-seed   (util/next-long gen)]
                          (-> proj
                              ;;these randomizer are for
                              ;;cycle time randomization
                              (assoc :rep-seed rep-seed
                                     :supply-record-randomizer
                                     (seed->randomizer rep-seed))
                              (rand-proj)
                              (project->demand-trends)
                              
                              ))))
          ;;all demand trends from all SRCs added together for this
          ;;one rep
          (apply concat)
          (vec)
          )))

(defn lerp-demand-trends
  "Given sparsely-sampled demand trend records with deltaT, we need to
  repeat each record deltaT times for easiy merging with other reps."
  [demand-trends]
  (->> (for [{:keys [deltaT t] :as r} demand-trends
             :let [new-recs (repeat deltaT (assoc r :deltaT 1))]]
         (->> (range t (+ (count new-recs) t))
         (map (fn [rec t] (assoc rec :t t)) new-recs)))
       (apply concat)
         ))
  
(defn combine-trends [reps demand-trends]
  (let [{:keys [SRC DemandGroup t]} (first demand-trends)]
     (->> (map (fn [r] (dissoc r :DemandName :Vignette
                          :Quarter :SRC :DemandGroup :t))
              demand-trends)
          (apply merge-with +)
          ;;Compute average stats for this day.
          (map (fn [[k v]] [k (float (/ v reps))]))
          (into {})
         ((fn [r] (assoc r :SRC SRC :DemandGroup DemandGroup :t t))))))

(defn group-trends [reps demand-trends]
  ;;ensure that demandtrends are lerped since we will lose delta t!
  ;;group by time and SRC
  (->> (group-by #(select-keys % [:t :SRC :DemandGroup])
                 (lerp-demand-trends demand-trends))
       (map (fn [[keys recs]] (combine-trends reps recs)) )))
	
;;for each run, add everything in the groups together (probably
;;nothing to add), and drop t, DemandName, Vignette, deltaT, Quarter. DemandGroup, SRC stay constant.
;;then merge those results with the existing set of runs with addition  Probably won?t add anything together for individual runs
;;There is also a post results transformation on every added value.

(defn rand-runs-dtrends
  "Runs replications of the rand-target-model function in order to
  return one set of demand trends from multiple initial condition
  replications.
   Caller may supply
   :reps - int, number of random replications
   :seed - integer, random seed to use for all the replications, default +default-seed+.
   :compo-lengths optional, map of {compo cyclelength} used for distribution
                  random initial cycletimes, default
  default-compo-lengths.
  :phases - optional, sequence of [phase from to] :: [string int int],
             derived from PeriodRecords if nil. Likely to punt to post processing."
  [proj-path & {:keys [reps phases seed compo-lengths seed->randomizer
                       out-file-name]
           :or   {seed +default-seed+
                  compo-lengths default-compo-lengths}}]
  (let [proj (a/load-project proj-path)
        proj-path (project/project-path proj)
        file-name (str proj-path (if out-file-name out-file-name
                                     "DemandTrends_reps.txt"))
        seed->randomizer (or seed->randomizer #(default-randomizer % compo-lengths))
        gen              (util/->gen seed)
        phases           (or phases (util/derive-phases proj))]
    ;;input validation, we probably should do more of this in general.
    (assert (s/valid? ::phases phases) (s/explain-str ::phases []))
       ;;return demand-trends
    (->> (range reps)
         (map (fn [n] (rand-demand-trends proj
                                          :phases phases
                                          :gen   gen
                                          :seed->randomizer
                                          seed->randomizer)))
         ;;all demand trends from the reps added together
         (apply concat)
         (group-trends reps)
         ((fn [recs] (tbl/records->file recs file-name))))))

