package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
)

type Node interface {
	ID() rdx.ID
	String() string
	List() []string
	// returns nil if there is none
	Get(name string) Node
	Put(loc string, node Node) error
	Set(val string) error
}

var ErrNotSupported = errors.New("operation not supported")

// the root node
type ReplicaNode struct {
	repl *REPL
}

func (ren *ReplicaNode) getFieldTLV(id rdx.ID) (rdt byte, tlv []byte) {
	return chotki.GetFieldTLV(ren.repl.snap, id)
}

func (ren *ReplicaNode) ID() rdx.ID {
	return ren.repl.Host.Last()
}
func (ren *ReplicaNode) String() string {
	// todo find type, scan
	return ren.ID().String()
}
func (ren *ReplicaNode) List() []string {
	// todo find type, scan
	return nil
}
func (ren *ReplicaNode) Get(name string) Node {
	id := rdx.ParseIDString(name)
	if id == rdx.BadId {
		return nil
	}
	rdt, tlv := ren.getFieldTLV(id)
	otype := rdx.IDFromZipBytes(tlv)
	if id.Off() == 0 {
		switch rdt {
		case 'L':
			return nil // todo
		case 'O':
			return &ObjectNode{
				Id:   id,
				Type: otype,
				repl: ren.repl,
			}
		case 'T':
			return &TypeNode{
				Id:     id,
				Parent: otype,
				repl:   ren.repl,
			}
		default:
			return nil
		}
	} else {
		switch rdt {
		case 'S':
			return &SNode{Id: id, repl: ren.repl}
		default:
			fmt.Printf("unrecognized rdt %c\n", rdt)
		}
	}
	return nil
}
func (ren *ReplicaNode) Put(loc string, node Node) error {
	// todo find f in type decl
	return nil
}
func (ren *ReplicaNode) Set(val string) error {
	return chotki.ErrNotImplemented // {key="value"}
}

type AliasNode struct {
	Names map[string]rdx.ID
}

func (a *AliasNode) ID() rdx.ID {
	return rdx.ID0
}
func (a *AliasNode) String() string {
	return "-ALIASES-"
}
func (a *AliasNode) List() (ret []string) {
	for name := range a.Names {
		ret = append(ret, name)
	}
	return
}
func (a *AliasNode) Get(name string) Node {
	id, ok := a.Names[name]
	if !ok {
		return nil
	} else if id.Off() != 0 {
		// see type
		return nil // &FieldNode{Id: id}
	} else {
		return &ObjectNode{Id: id}
	}

}
func (a *AliasNode) Put(loc string, node Node) error {
	a.Names[loc] = node.ID()
	return nil
}
func (a *AliasNode) Set(val string) error {
	return ErrNotSupported
}

type TypeNode struct {
	Id     rdx.ID
	Parent rdx.ID
	repl   *REPL
}

func (tn *TypeNode) ID() rdx.ID {
	return tn.Id
}
func (tn *TypeNode) String() string {
	return tn.Parent.String()
}
func (tn *TypeNode) List() (fields []string) {
	formula, _ := tn.repl.Host.ObjectType(tn.Id)
	for _, f := range formula {
		fields = append(fields, f[1:])
	}
	return
}

func (tn *TypeNode) Get(name string) Node {
	formula, _ := tn.repl.Host.ObjectType(tn.Id)
	for n, fn := range formula {
		if fn[1:] != name {
			continue
		}
		return &ANode{
			id:    tn.Id + rdx.ID(n+1),
			value: fn[:1],
		}
	}
	return nil
}
func (tn *TypeNode) Put(loc string, node Node) error {
	// todo find f in type decl
	return nil
}
func (tn *TypeNode) Set(val string) error {
	return chotki.ErrNotImplemented // {key="value"}
}

type ObjectNode struct {
	Id   rdx.ID
	Type rdx.ID
	repl *REPL
}

func (on *ObjectNode) ID() rdx.ID {
	return on.Id
}
func (on *ObjectNode) String() string {
	return on.Type.String()
}
func (on *ObjectNode) List() []string {
	// todo find type, scan
	formula, _ := on.repl.Host.ObjectType(on.Type)
	/*it := on.repl.Host.ObjectIterator(on.Id)
	for it != nil && it.Next() {

	}*/
	return formula
}

func (on *ObjectNode) Get(name string) Node {
	// todo find f in type decl
	return nil
}
func (on *ObjectNode) Put(loc string, node Node) error {
	// todo find f in type decl
	return nil
}
func (on *ObjectNode) Set(val string) error {
	return chotki.ErrNotImplemented // {key="value"}
}

type ANode struct {
	id    rdx.ID
	value string
}

func (an *ANode) ID() rdx.ID {
	return an.id
}
func (an *ANode) String() string {
	return an.value
}
func (an *ANode) List() []string {
	return nil
}

func (an *ANode) Get(name string) Node {
	return nil
}

var ErrNotMutable = errors.New("field is not mutable")

func (an *ANode) Put(loc string, node Node) error {
	return ErrNotMutable
}
func (an *ANode) Set(val string) error {
	return ErrNotMutable
}

type FNode struct {
	Id   rdx.ID
	Host *chotki.Chotki
}

func (fn *FNode) ID() rdx.ID {
	return fn.Id
}
func (fn *FNode) String() string {
	// get
	return ""
}
func (fn *FNode) List() []string {
	return []string{}
}
func (fn *FNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *FNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *FNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type INode struct {
	Id   rdx.ID
	Host *chotki.Chotki
}

func (fn *INode) ID() rdx.ID {
	return fn.Id
}
func (fn *INode) String() string {
	// get
	return ""
}
func (fn *INode) List() []string {
	return []string{}
}
func (fn *INode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *INode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *INode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type RNode struct {
	Id   rdx.ID
	Host *chotki.Chotki
}

func (fn *RNode) ID() rdx.ID {
	return fn.Id
}
func (fn *RNode) String() string {
	// get
	return ""
}
func (fn *RNode) List() []string {
	return []string{}
}
func (fn *RNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *RNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *RNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type SNode struct {
	Id   rdx.ID
	repl *REPL
}

func (fn *SNode) ID() rdx.ID {
	return fn.Id
}
func (fn *SNode) String() string {
	// get
	return ""
}
func (fn *SNode) List() []string {
	return []string{}
}
func (fn *SNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *SNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *SNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type TNode struct {
	Id   rdx.ID
	Host *chotki.Chotki
}

func (fn *TNode) ID() rdx.ID {
	return fn.Id
}
func (fn *TNode) String() string {
	// get
	return ""
}
func (fn *TNode) List() []string {
	return []string{}
}
func (fn *TNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *TNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *TNode) Set(val string) error {
	return chotki.ErrNotImplemented
}
