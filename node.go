package main

import "encoding/binary"

type Item struct {
	key   []byte
	value []byte
}

//will store dal to interact , list of items in current node , Node's self pageNum, and PageNumbers of child nodes
type Node struct {
	*dal
	items      []*Item
	pageNum    pgnum
	childNodes []pgnum
}

func newEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1
	// Add page header: isLeaf, key-value pairs count, node num
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// key-value pairs count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	// We use slotted pages for storing data in the page. It means the actual keys and values (the cells) are appended
	// to right of the page whereas offsets have a fixed size and are appended from the left.

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]

			// Write the child page as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)
		//offset is position of data in Node's 2nd partition i.e. from end.
		offset := rightPos - (klen + vlen) - 2
		//offset address of each key-value pair is stored on left side
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2
		rightPos -= vlen
		//whereas key-value data is appended from the right side
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		// Write the last child node
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		// Write the child page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}
	return buf
}

//to deserialize data that is written to a page format into Node format.
func (n *Node) deserialize(buf []byte) {
	leftPos := 0
	isLeaf := uint(buf[0])
	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))

	leftPos += 3

	for i := 0; i < itemsCount; i++ {
		//since we stored isLeaf in int format
		if isLeaf == 0 { //false
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		//read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2
		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen
		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}

}

func (n *Node) writeNode(node *Node) *Node {
	writtenNode, err := n.dal.writeNode(node)
	if err != nil {
		return nil
	}
	return writtenNode
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.dal.getNode(pageNum)
}
