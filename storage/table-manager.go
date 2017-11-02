package storage

import (
	"ddb/storage/config"
	"ddb/storage/table"
	"fmt"
	"sync"
	"time"
)

type TableManager struct {
	sync.Mutex
	tables map[string]*table.Table
	states map[string]string
}

func (tm *TableManager) Init() {
	tm.tables = map[string]*table.Table{}
	tm.states = map[string]string{}
	//
	//log, err := table.CreateTable(
	//	"log",
	//	config.TableConfig{
	//		Name:      "log",
	//		Storage:   "bptree",
	//		IndexType: "btree",
	//		Columns: config.ColumnsConfig{
	//			config.ColumnConfig{
	//				Name:   "time",
	//				Type:   "string",
	//				Length: 50,
	//			},
	//			config.ColumnConfig{
	//				Name:   "query",
	//				Type:   "string",
	//				Length: 1000,
	//			},
	//		},
	//	},
	//)
	//
	//if err != nil {
	//	panic(err)
	//}
	//
	//tm.tables["log"] = log
	//tm.states["log"] = "opened"
}

func (tm *TableManager) CloseTables() {
	tm.Lock()
	for i := range tm.tables {
		tm.tables[i].Close()
		tm.states[i] = "closed"
	}
	tm.Unlock()
}

func (tm *TableManager) getTable(name string) (tab *table.Table, err error) {
	var open bool

	tm.Lock()

	if _, ok := tm.states[name]; !ok {
		tm.states[name] = "closed"
	}

	if open = tm.states[name] == "closed"; open {
		tm.states[name] = "opening"
	}

	tm.Unlock()

	if !open {
		tm.waitWhileTableOpen(name)
	}

	if tab, err = tm.openTable(name); err != nil {
		tm.Lock()
		tm.states[name] = "closed"
		tm.Unlock()
	}

	return
}

func (tm *TableManager) waitWhileTableOpen(name string) {
	for {
		tm.Lock()
		state := tm.states[name]
		tm.Unlock()

		if state != "opening" {
			break
		}

		fmt.Println("Wait for table opening (", state,")")
		time.Sleep(500 * time.Millisecond)
	}
}

func (tm *TableManager) openTable(name string) (tab *table.Table, err error) {
	var ok bool

	if tab, ok = tm.tables[name]; !ok {

		cfg := config.TableConfig{}
		cfn := "/Users/andrey/Go/src/ddb/data/t" + name + ".json"

		if err = cfg.Load(cfn); err == nil {
			tab = &table.Table{}
			tab.Open(cfg)

			tm.Lock()
			tm.tables[name] = tab
			tm.states[name] = "opened"
			tm.Unlock()
		}
	}
	return
}