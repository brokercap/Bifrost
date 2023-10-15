package src

func (This *Conn) InitSupportBatchCommit() {
	This.SetChildInsertFunc(This.Insert)
	This.SetChildUpdateFunc(This.Update)
	This.SetChildQueryFunc(This.Del)
	This.SetChildQueryFunc(This.Query)
	This.SetChildCommitFunc(This.Commit)
}
