package inner

type Visitor interface {
	VisitData(message *DataMessage) error
	VisitEndOfRecords(message *EndOfRecordsMessage) error
	VisitEmitTotals(message *EmitTotalsMessage) error
}
