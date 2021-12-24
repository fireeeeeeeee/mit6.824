package linkedmap

import "container/list"

type LinkMap struct {
	l  *list.List
	ma map[string]*list.Element
}

func New() *LinkMap {
	return &LinkMap{list.New(), make(map[string]*list.Element)}
}

func (lm *LinkMap) Len() int {
	return lm.l.Len()
}

func (lm *LinkMap) Insert(key string, v interface{}) *list.Element {
	element := lm.l.PushBack(v)
	lm.ma[key] = element
	return element
}

func (lm *LinkMap) Front() *list.Element {
	return lm.l.Front()
}

func (lm *LinkMap) Remove(e *list.Element) {
	lm.l.Remove(e)
}

func (lm *LinkMap) RemoveByKey(key string) {
	lm.l.Remove(lm.GetItem(key))
}

func (lm *LinkMap) GetItem(key string) *list.Element {
	return lm.ma[key]
}
