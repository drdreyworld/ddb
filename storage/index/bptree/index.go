package bptree

import (
	"ddb/storage/config"
	"ddb/storage/engine"
	"ddb/storage/index"
	"github.com/drdreyworld/bptree"
	"github.com/drdreyworld/smconv"
)

type Index struct {
	config   config.IndexConfig
	tree     bptree.Tree
	keySize  uint32
	dataSize uint32
}

func (idx *Index) Open(cfg config.IndexConfig) {
	idx.config = cfg
	idx.keySize = 0

	for i := 0; i < len(cfg.Cols); i++ {
		idx.keySize += uint32(cfg.Cols[i].Length)
	}

	// max 512 items uint32
	idx.dataSize = 512 * 4

	idx.tree = bptree.Tree{}
	idx.tree.Init(int(idx.keySize), int(idx.dataSize))
	idx.tree.Mid = 20
}

func (idx *Index) Close() {

}

func (idx *Index) GetColumns() config.ColumnsConfig {
	return idx.config.Cols
}

func (idx *Index) BuildIndex(storage engine.Storage) {
	rowsCount := storage.GetRowsCount()
	row := map[string][]byte{}

	for position := 0; position < rowsCount; position++ {
		for _, column := range idx.config.Cols {
			row[column.Name] = storage.GetBytes(position, column.Name)
		}
		idx.Insert(position, row)
	}
}

func (idx *Index) createKeyFromRowData(rowData map[string][]byte) []byte {
	key := []byte{}
	for i := 0; i < len(idx.config.Cols); i++ {
		key = append(key, rowData[idx.config.Cols[i].Name]...)
	}
	return key
}

func (idx *Index) Insert(rowIndex int, rowData map[string][]byte) {
	var row *bptree.Row

	key := idx.createKeyFromRowData(rowData)

	if row = idx.tree.Find(key); row == nil {
		row = bptree.CreateRow(int(idx.keySize), int(idx.dataSize), key, []byte{})
	}

	pos := idx.keySize + 4
	cnt := smconv.Uint32FromBytes((*row)[pos : pos+4])
	pos += 4

	for i := uint32(0); i < cnt; i++ {
		val := smconv.Uint32FromBytes((*row)[pos : pos+4])
		if val == uint32(rowIndex) {
			return
		}
	}

	cnt++

	pos = idx.keySize + 4
	copy((*row)[pos:pos+4], smconv.Uint32ToBytes(cnt))

	pos += cnt * 4
	copy((*row)[pos:pos+4], smconv.Uint32ToBytes(uint32(rowIndex)))

	idx.tree.Insert(row)
}

func (idx *Index) Delete(rowIndex int, rowData map[string][]byte) {

}

func (idx *Index) ScanRows(where index.ScanWhereFunc, fn index.ScanRowFunc, order map[string]string) {

	var traverse func(tree *bptree.Tree, depth int)
	var direction string
	var ok bool

	traverse = func(tree *bptree.Tree, depth int) {

		column := idx.config.Cols[depth].Name

		if direction, ok = order[column]; !ok {
			direction = "ASC"
		}

		tree.ScanRows(func(row *bptree.Row) bool {
			if where != nil {
				if !where(column, row.Key()) {
					return true
				}
			}

			// @todo multicolumn index
			//if value.Tree != nil {
			if false {
				// return traverse(value.Tree, depth+1)
			} else {
				positions := []int{}

				pos := idx.keySize + 4
				cnt := smconv.Uint32FromBytes((*row)[pos : pos+4])
				pos += 4

				for i := uint32(0); i < cnt; i++ {
					val := smconv.Uint32FromBytes((*row)[pos : pos+4])
					pos += 4
					positions = append(positions, int(val))
				}

				return fn(positions)
			}
			return true
		}, direction == "ASC")
	}

	traverse(&idx.tree, 0)
}