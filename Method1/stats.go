package beehive

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/state"
)

type collector interface {
	collect(bee uint64, in *msg, out []*msg)
}

type noOpStatCollector struct{}

func (c *noOpStatCollector) collect(bee uint64, in *msg, out []*msg) {}

const (
	appCollector  = "bh_collector"
	dictLocalStat = "LocalStatDict"
	dictLocalProv = "LocalProvDict"
	dictOptimizer = "OptimizerDict"

	defaultMinScore = 3
)

type collectorApp struct {
	hive Hive
}

func newAppStatCollector(h *hive) collector {
	c := &collectorApp{hive: h}
	a := h.NewApp(appCollector, NonTransactional())
	a.Handle(beeRecord{}, localCollector{})
	a.Handle(cmdMigrate{}, localCollector{})
	a.Handle(pollLocalStat{}, localStatPoller{
		thresh: uint64(h.config.OptimizeThresh),
	})

	a.Handle(beeMatrixUpdate{}, optimizerCollector{})
	a.Handle(pollOptimizer{}, optimizer{defaultMinScore})

	a.Detached(NewTimer(1*time.Second, func() {
		h.Emit(pollOptimizer{})
		h.Emit(pollLocalStat{})
	}))

	a.Handle(statRequest{}, statRequestHandler{})
	a.HandleHTTP("/stats", &statHttpHandler{hive: h})

	glog.V(1).Infof("%v installs app stat collector", h)
	return c
}

type beeRecord struct {
	Bee uint64
	In  *msg
	Out []*msg
}

func formatBeeID(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func parseBeeID(str string) uint64 {
	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		glog.Fatalf("error in parsing id: %v", err)
	}
	return id
}

func (c *collectorApp) collect(bee uint64, in *msg, out []*msg) {
	switch in.Data().(type) {
	case beeMatrixUpdate, cmdMigrate:
		return
	}

	if in.From() == Nil {
		return
	}

	// TODO(soheil): We should batch here.
	oc := make([]*msg, len(out))
	copy(oc, out)
	c.hive.Emit(beeRecord{Bee: bee, In: in, Out: oc})
}

type beeMatrix struct {
	Bee    uint64
	Matrix map[uint64]uint64
}

type localBeeMatrix struct {
	BeeMatrix    beeMatrix
	UpdateTime   time.Time
	UpdateMsgCnt uint64
}

type beeMatrixUpdate beeMatrix

type localCollector struct{}

func (c localCollector) Map(msg Msg, ctx MapContext) MappedCells {
	return ctx.LocalMappedCells()
}

func (c localCollector) Rcv(msg Msg, ctx RcvContext) error {
	switch br := msg.Data().(type) {
	case beeRecord:
		c.updateMatrix(br, ctx)
		c.updateProvenance(br, ctx)
	case cmdMigrate:
		bi, err := beeInfoFromContext(ctx, br.Bee)
		if err != nil {
			return fmt.Errorf("%v cannot find bee %v to migrate", ctx, br.Bee)
		}
		a, ok := ctx.(*bee).hive.app(bi.App)
		if !ok {
			return fmt.Errorf("%v cannot find app %v", ctx, a)
		}
		if _, err := a.qee.processCmd(br); err != nil {
			return fmt.Errorf(
				"%v cannot migrate bee %v to %v as instructed by optimizer: %v",
				ctx, br.Bee, br.To, err)
		}
	}
	return nil
}

func (c localCollector) updateMatrix(r beeRecord, ctx RcvContext) {
	d := ctx.Dict(dictLocalStat)
	k := formatBeeID(r.Bee)
	lm := localBeeMatrix{}
	if v, err := d.Get(k); err != nil {
		lm.BeeMatrix.Bee = r.Bee
		lm.BeeMatrix.Matrix = make(map[uint64]uint64)
		lm.UpdateTime = time.Now()
	} else {
		lm = v.(localBeeMatrix)
	}
	lm.BeeMatrix.Matrix[r.In.From()]++
	lm.UpdateMsgCnt++
	if err := d.Put(k, lm); err != nil {
		glog.Fatalf("cannot store matrix: %v", err)
	}
}

type provMatrix map[string]map[string]uint64

func (c localCollector) updateProvenance(r beeRecord, ctx RcvContext) {
	intype := r.In.Type()
	d := ctx.Dict(dictLocalProv)
	k := formatBeeID(r.Bee)
	var mx provMatrix
	if v, err := d.Get(k); err != nil {
		mx = make(provMatrix)
	} else {
		mx = v.(provMatrix)
	}
	stat, ok := mx[intype]
	if !ok {
		stat = make(map[string]uint64)
		mx[intype] = stat
	}
	for _, msg := range r.Out {
		stat[msg.Type()]++
	}
	if err := d.Put(k, mx); err != nil {
		glog.Fatalf("cannot store provenance data: %v", err)
	}
}

type pollLocalStat struct{}

type localStatPoller struct {
	thresh uint64
}

func (p localStatPoller) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{}
}

func (p localStatPoller) Rcv(msg Msg, ctx RcvContext) error {
	d := ctx.Dict(dictLocalStat)
	d.ForEach(func(k string, v interface{}) bool {
		lm := v.(localBeeMatrix)
		now := time.Now()
		dur := uint64(now.Sub(lm.UpdateTime) / time.Second)
		if dur == 0 {
			dur = 1
		}
		if lm.UpdateMsgCnt/dur < p.thresh {
			return true
		}

		ctx.Emit(beeMatrixUpdate(lm.BeeMatrix))
		lm.UpdateTime = now
		lm.UpdateMsgCnt = 0
		d.Put(k, lm)
		return true
	})
	return nil
}

// TODO(soheil): implement migration status: none, initiated, and done.
type optimizerStat struct {
	Bee       uint64
	Collector uint64
	Matrix    map[uint64]uint64
	Migrated  bool
	Score     int
	LastMax   uint64
}

type optimizerCollector struct{}

func (c optimizerCollector) isMigrated(b uint64, optDict state.Dict) bool {
	v, err := optDict.Get(formatBeeID(b))
	return err == nil && v.(optimizerStat).Migrated
}

func (c optimizerCollector) Rcv(msg Msg, ctx RcvContext) error {
	up := msg.Data().(beeMatrixUpdate)
	glog.V(3).Infof("optimizer receives stat update: %+v", up)
	dict := ctx.Dict(dictOptimizer)
	k := formatBeeID(up.Bee)
	os := optimizerStat{}
	if v, err := dict.Get(k); err == nil {
		os = v.(optimizerStat)
	}
	os.Bee = up.Bee
	os.Collector = msg.From()
	os.Matrix = up.Matrix
	return dict.Put(k, os)
}

var optimizerCentrlizedCells = MappedCells{{dictOptimizer, "0"}}

func (c optimizerCollector) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

type beeHiveCnt struct {
	Bee  uint64
	Hive uint64
	Cnt  uint64
}

type beeHiveStat []beeHiveCnt

func (s beeHiveStat) Len() int           { return len(s) }
func (s beeHiveStat) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s beeHiveStat) Less(i, j int) bool { return s[i].Cnt < s[j].Cnt }

type pollOptimizer struct{}

type optimizer struct {
	minScore int
}

func getOptimizerStats(dict state.Dict) (stats map[uint64]optimizerStat) {
	stats = make(map[uint64]optimizerStat)
	dict.ForEach(func(k string, v interface{}) bool {
		id := parseBeeID(k)
		stats[id] = v.(optimizerStat)
		return true
	})
	return
}

func (o optimizer) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(dictOptimizer)
	stats := getOptimizerStats(dict)

	infos := make(map[uint64]BeeInfo)
	for id, os := range stats {
		infos[id] = BeeInfo{}
		for bid := range os.Matrix {
			infos[bid] = BeeInfo{}
		}
	}

	var err error
	for id := range infos {
		infos[id], err = beeInfoFromContext(ctx, id)
		if err != nil {
			delete(infos, id)
		}
	}

	// TODO: don't hardcode this cap
	var capacity uint64
	capacity = 1

	// Count the number of bees in each hive
	// bees_per_hive is a map of hive ID to number of bees
	bees_per_hive := make(map[uint64]uint64)

	// Initialize an entry for each hive
	all_hives := ctx.Hive().(*hive).registry.hives()
	for hi := range all_hives {
		bees_per_hive[hi.ID] = 0
	}

	for b, _ := range stats {
		bi, ok := infos[b]
		if !ok {
			continue
		}

		// Create an entry in the map if there isn't one already
		_, ok = bees_per_hive[bi.Hive]
		if !ok {
			bees_per_hive[bi.Hive] = 0
		}

		// Increment the number of bees in this bee's Hive
		bees_per_hive[bi.Hive]++
	}

	// Separate out the hives that do not exceed the cap
	full_hives := make([]uint64, 0, len(bees_per_hive))
	free_hives := make([]uint64, 0, len(bees_per_hive))
	for hid, count := range bees_per_hive {
		if count > capacity {
			full_hives = append(full_hives, hid)
		} else if count < capacity {
			free_hives = append(free_hives, hid)
		}
	}

	// No hives are over capacity
	if len(full_hives) == 0 {
		return nil
	}

	// No free hives to migrate to; too bad
	if len (free_hives) == 0 {
		return nil
	}

	// For now, migrate a random bee to a random free hive
	for _, hid := range full_hives {
		bees_in_hive := ctx.Hive().(*hive).registry.beesOfHive(hid)
		for _, bee := range bees_in_hive {
			bid := bee.ID
			bi, ok := infos[bid]

			// Don't migrate certain bees
			if !ok || bi.Detached {
			continue
			}
			if app, ok := ctx.Hive().(*hive).app(bi.App); ok && app.sticky() {
				continue
			}

			os := stats[bid]
			if os.Migrated {
				continue
			}

			glog.Infof("%v initiates migration of bee %v to hive %v",
				ctx, bid, free_hives[0])
			ctx.SendToBee(cmdMigrate{Bee: bid, To: free_hives[0]}, os.Collector)
			os.Migrated = true
			k := formatBeeID(bid)
			dict.Put(k, os)

			// Remove this free hive from the list of free hives
			free_hives = free_hives[1:]

			// No free hives left; oh well
			if len(free_hives) == 0 {
				return nil
			}

			// Migration done for this hive
			break
		}
	}

	return nil
}

func (o optimizer) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

type statRequestHandler struct{}

func (h statRequestHandler) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(dictOptimizer)
	res := statResponse{
		Matrix: make(map[uint64]map[uint64]uint64),
	}
	dict.ForEach(func(k string, v interface{}) bool {
		os := v.(optimizerStat)
		res.Matrix[os.Bee] = os.Matrix
		return true
	})
	return ctx.Reply(msg, res)
}

func (h statRequestHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

func beeInfoFromContext(ctx RcvContext, bid uint64) (BeeInfo, error) {
	return ctx.Hive().(*hive).registry.bee(bid)
}

type statHttpHandler struct {
	hive Hive
}

func (h *statHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, ccl := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccl()
	res, err := h.hive.Sync(ctx, statRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonres := make(map[string]map[string]uint64)
	for to, m := range res.(statResponse).Matrix {
		jsonresto := make(map[string]uint64)
		jsonres[strconv.FormatUint(to, 10)] = jsonresto
		for from, cnt := range m {
			jsonresto[strconv.FormatUint(from, 10)] = cnt
		}
	}
	b, err := json.Marshal(jsonres)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

type statRequest struct{}

type statResponse struct {
	Matrix map[uint64]map[uint64]uint64
}

func init() {
	gob.Register(beeHiveCnt{})
	gob.Register(beeHiveStat{})
	gob.Register(beeMatrix{})
	gob.Register(beeMatrixUpdate{})
	gob.Register(beeRecord{})
	gob.Register(localBeeMatrix{})
	gob.Register(optimizerStat{})
	gob.Register(pollLocalStat{})
	gob.Register(pollOptimizer{})
	gob.Register(provMatrix{})
	gob.Register(statRequest{})
	gob.Register(statResponse{})
}
