package metrics

// MetricsRow holds one second's worth of resource usage for a container.
// Column names match the original dstat CSV schema for backward compatibility.
type MetricsRow struct {
	Epoch    int64   `csv:"epoch"`
	CpuUsr   float64 `csv:"cpu_usr"`
	CpuSys   float64 `csv:"cpu_sys"`
	CpuIdl   float64 `csv:"cpu_idl"`
	CpuWai   float64 `csv:"cpu_wai"`
	CpuHiq   float64 `csv:"cpu_hiq"`
	CpuSiq   float64 `csv:"cpu_siq"`
	MemUsed  int64   `csv:"mem_used"`
	MemBuff  int64   `csv:"mem_buff"`
	MemCach  int64   `csv:"mem_cach"`
	MemFree  int64   `csv:"mem_free"`
	NetRecv  int64   `csv:"net_recv"`
	NetSend  int64   `csv:"net_send"`
	DskRead  int64   `csv:"dsk_read"`
	DskWrit  int64   `csv:"dsk_writ"`
	Load1    float64 `csv:"load_avg1"`
	Load5    float64 `csv:"load_avg5"`
	Load15   float64 `csv:"load_avg15"`
	ProcsRun int     `csv:"procs_run"`
	ProcsBlk int     `csv:"procs_blk"`
	TcpLis   int     `csv:"tcp_lis"`
	TcpAct   int     `csv:"tcp_act"`
	TcpSyn   int     `csv:"tcp_syn"`
	TcpTim   int     `csv:"tcp_tim"`
	TcpClo   int     `csv:"tcp_clo"`
}
