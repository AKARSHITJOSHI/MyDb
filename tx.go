package main

type tx struct {
	dirtyNodes    map[pgnum]*Node
	pagesToDelete []pgnum

	// new pages allocated during the transaction. They will be released if rollback is called.
	//i.e the won't be committed.
	allocatedPageNums []pgnum

	write bool

	db *DB
}

func newTx(db *DB, write bool) *tx {
	return &tx{
		map[pgnum]*Node{},
		make([]pgnum, 0),
		make([]pgnum, 0),
		write,
		db,
	}
}

func (tx *tx) Rollback() {
	if !tx.write {
		tx.db.rwlock.Unlock()
	}
	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	for _, pagenum := range tx.allocatedPageNums {
		tx.db.freelist.releasePage(pagenum)
	}
	tx.allocatedPageNums = nil
	tx.db.rwlock.Unlock()
}

func (tx *tx) Commit() error {
	if !tx.write {
		tx.db.rwlock.Unlock()
		return nil
	}
	//modified nodes are saved, deleted nodes are removed and acquired pages are marked

	for _, node := range tx.dirtyNodes {
		_, err := tx.db.writeNode(node)
		if err != nil {
			return err
		}
	}

	for _, pageNum := range tx.pagesToDelete {
		tx.db.deleteNode(pageNum)
	}
	_, err := tx.db.writeFreelist()
	if err != nil {
		return err
	}

	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	tx.allocatedPageNums = nil
	tx.db.rwlock.Unlock()
	return nil
}

func (tx *tx) newNode(items []*Item, childNodes []pgnum) *Node {
	node := NewEmptyNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = tx.db.getNextPage()
	node.tx = tx

	node.tx.allocatedPageNums = append(node.tx.allocatedPageNums, node.pageNum)
	return node
}

func (tx *tx) getNode(pageNum pgnum) (*Node, error) {
	if node, ok := tx.dirtyNodes[pageNum]; ok {
		return node, nil
	}

	node, err := tx.db.getNode(pageNum)
	if err != nil {
		return nil, err
	}
	node.tx = tx
	return node, nil
}

func (tx *tx) writeNode(node *Node) *Node {
	tx.dirtyNodes[node.pageNum] = node
	node.tx = tx
	return node
}

func (tx *tx) deleteNode(node *Node) {
	tx.pagesToDelete = append(tx.pagesToDelete, node.pageNum)
}
