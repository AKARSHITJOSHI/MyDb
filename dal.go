package main

import (
	"errors"
	"fmt"
	"os"
)

type pgnum uint64

type page struct {
	num  pgnum
	data []byte
}

type Options struct {
	pageSize       int
	MinFillPercent float32
	MaxFillPercent float32
}

var defaultOptions = &Options{
	MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type dal struct {
	file           *os.File
	pageSize       int
	minFillPercent float32
	maxFillPercent float32
	*freelist
	*meta
}

func newDal(path string, options *Options) (*dal, error) {
	dal := &dal{
		meta:           newEmptyMeta(),
		pageSize:       os.Getpagesize(),
		minFillPercent: options.MinFillPercent,
		maxFillPercent: options.MaxFillPercent,
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist
	} else if errors.Is(err, os.ErrNotExist) {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		dal.freelist = newFreelist()
		dal.freelistPage = dal.getNextPage()
		_, err := dal.writeFreelist()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMeta(dal.meta)
	} else {
		return nil, err
	}
	return dal, nil
}

func (d *dal) close() error {
	if d.file != nil {
		err := d.file.Close()

		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}
	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize, d.pageSize),
	}
}

func (d *dal) readPage(pageNum pgnum) (*page, error) {
	p := d.allocateEmptyPage()
	//use pageNum * size to get to the page address
	offset := int(pageNum) * d.pageSize

	//read the page at that offset to p and return
	_, err := d.file.ReadAt(p.data, int64(offset))

	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *dal) writePage(p *page) error {
	offest := int(d.pageSize) * int(p.num)
	_, err := d.file.WriteAt(p.data, int64(offest))
	if err != nil {
		return err
	}
	return nil
}

func (d *dal) writeMeta(meta *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	meta.serialize(p.data)
	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *dal) readMeta() (*meta, error) {
	//metaPageNum always be at pageNum 0
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}

func (d *dal) writeFreelist() (*page, error) {
	p := d.allocateEmptyPage()
	p.num = d.freelistPage
	d.freelist.serialize(p.data)
	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	d.freelistPage = p.num
	return p, nil
}

func (d *dal) readFreelist() (*freelist, error) {
	p, err := d.readPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := newFreelist()
	freelist.deserialize(p.data)
	return freelist, nil
}

//to convert page binary data stored in Disk and convert to node format
func (d *dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.readPage(pageNum)
	if err != nil {
		return nil, err
	}
	node := newEmptyNode()

	node.deserialize(p.data)
	node.pageNum = pageNum
	return node, nil
}

//to convert the node into page format and store on disk
func (d *dal) writeNode(n *Node) (*Node, error) {
	p := d.allocateEmptyPage()
	if n.pageNum == 0 {
		p.num = d.getNextPage()
		n.pageNum = p.num
	} else {
		p.num = n.pageNum
	}
	//write node's data to page data
	p.data = n.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *dal) deleteNode(pageNum pgnum) {
	d.releasePage(pageNum)
}

//pageWise size is given in order to check if node is over/under populated
func (d *dal) maxThreshold() float32 {
	return d.maxFillPercent * float32(d.pageSize)
}

func (d *dal) isOverPopulated(node *Node) bool {
	return float32(node.nodeSize()) > d.maxThreshold()
}

func (d *dal) minThreshold() float32 {
	return d.minFillPercent * float32(d.pageSize)
}

func (d *dal) isUnderPopulated(node *Node) bool {
	return float32(node.nodeSize()) < d.minThreshold()
}

// getSplitIndex should be called when performing rebalance after an item is removed.
//It checks if a node can spare an element, and if it does then it returns the index
//when there the split should happen. Otherwise -1 is returned.

func (d *dal) getSplitIndex(node *Node) int {
	size := 0
	//by default its 3
	size += nodeHeaderSize

	for i := range node.items {
		size += node.elementSize(i)

		if float32(size) > d.minThreshold() && i < len(node.items)-1 {
			return i + 1
		}
	}
	return -1
}

func (d *dal) newNode(items []*Item, childNodes []pgnum) *Node {
	node := NewEmptyNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = d.getNextPage()
	node.dal = d
	return node
}
