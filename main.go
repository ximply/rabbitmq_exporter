package main

import (
	"io"
	"os"
	"net"
	"fmt"
	"os/exec"
	"strings"
	"net/http"
	"strconv"
	"github.com/robfig/cron"
	"sync"
	"flag"
)

var (
	Name           = "rabbitmq_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/rabbitmq_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	users          = flag.String("users", "", "Rabbitmq users list, multi split with ,.")
	vhosts         = flag.String("vhosts", "", "Rabbitmq vhost list, multi split with ,.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
)

var g_lock sync.RWMutex
var g_ret string
var g_users []string
var g_usersMap map[string]string
var g_vhosts []string
var g_vhostsMap map[string]string
var doing bool

type userConnInfo struct {
	User string
	Blocked float64
	Running float64
	Closed float64
}

type vhostQueueInfo struct {
	Vhost string
	UnAck float64
	Ready float64
}

func execImpl(cmdStr string) string {
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	cmd.Wait()
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func up() string {
	return execImpl("ss -ntpl | grep beam.smp | grep -v grep | wc -l")
}

func status() string {
	return execImpl("/usr/local/rabbitmq/sbin/rabbitmqctl status")
}

func listConnections() string {
	return execImpl("/usr/local/rabbitmq/sbin/rabbitmqctl list_connections user state | grep -v Listing | grep -v done | awk -v OFS=',' '{print $1,$2}'")
}

// messages_unacknowledged,messages_ready
func listQueues(msg string, vhost string) string {
	if len(msg) == 0 || len(vhost) == 0 {
		return ""
	}

	return execImpl("/usr/local/rabbitmq/sbin/rabbitmqctl list_queues -p %s %s | grep -v Listing | grep -v done | awk '{print $NF}'")
}

func doWork() {
	if doing {
		return
	}
	doing = true

	alive := up()
	alive = strings.TrimRight(alive, "\n")
	if !strings.EqualFold(alive, "4") {
		g_lock.Lock()
		g_ret = "rabbitmq_up 0"
		g_lock.Unlock()
		doing = false
		return
	}

	status := status()
	statusList := strings.Split(status, "\n")
	/*if len(statusList) < 45 {
		g_lock.Lock()
		g_ret = "rabbitmq_up 0"
		g_lock.Unlock()
		doing = false
		return
	}*/

	ret := "rabbitmq_up 1\n"
	nameSpace := "rabbitmq"
	total_memory_used_v := 0.0
	vm_memory_limit_v := 0.0
	processes_limit_v := 0.0
	processes_used_v := 0.0

	for _, s := range statusList {
		if strings.Contains(s,"[{total,") {
			//     [{total,448090960},
			tmp := strings.TrimRight(s, "},")
			l := strings.Split(tmp, ",")
			total_memory_used, _ := strconv.ParseFloat(l[1], 64)
			total_memory_used_v = total_memory_used
			ret += fmt.Sprintf("%s_total_memory_used %g\n", nameSpace, total_memory_used)
		} else if strings.Contains(s, "{vm_memory_high_watermark,") {
			// {vm_memory_high_watermark,0.8},
			tmp := strings.TrimRight(s, "},")
			l := strings.Split(tmp, ",")
			vm_memory_high_watermark, _ := strconv.ParseFloat(l[1],64)
			ret += fmt.Sprintf("%s_vm_memory_high_watermark %g\n", nameSpace, vm_memory_high_watermark)

		} else if strings.Contains(s, "{vm_memory_limit,") {
			// {vm_memory_limit,6576822681},
			tmp := strings.TrimRight(s, "},")
			l := strings.Split(tmp, ",")
			vm_memory_limit, _ := strconv.ParseFloat(l[1],64)
			vm_memory_limit_v = vm_memory_limit
			ret += fmt.Sprintf("%s_vm_memory_limit %g\n", nameSpace, vm_memory_limit)

		} else if strings.Contains(s,"{processes,") {
			// {processes,[{limit,1048576},{used,12596}]},
			tmp := strings.TrimRight(s, "}]},")
			// {processes,[{limit,1048576},{used,12596
			tmp = strings.Replace(tmp, " ", "", -1)
			//{processes,[{limit,1048576},{used,12596
			tmp = strings.Replace(tmp, "{processes,[{limit,", "", -1)
			//1048576},{used,12596
			tmp = strings.Replace(tmp, "},{used", "", -1)
			//1048576,12596
			l := strings.Split(tmp, ",")
			processes_limit, _ := strconv.ParseFloat(l[0], 64)
			processes_limit_v = processes_limit
			ret += fmt.Sprintf("%s_processes_limit %g\n", nameSpace, processes_limit)
			processes_used, _ := strconv.ParseFloat(l[1], 64)
			processes_used_v = processes_used
			ret += fmt.Sprintf("%s_processes_used %g\n", nameSpace, processes_used)
		}
	}

	if vm_memory_limit_v > 1 {
		ret += fmt.Sprintf("%s_total_memory_used_pct %g\n", nameSpace,
			(total_memory_used_v / vm_memory_limit_v) * 100)
	} else {
		ret += fmt.Sprintf("%s_total_memory_used_pct %g\n", nameSpace, 0.0)
	}
	if processes_limit_v > 1 {
		ret += fmt.Sprintf("%s_processes_limit_pct %g\n", nameSpace,
			(processes_used_v / processes_limit_v) * 100)
	} else {
		ret += fmt.Sprintf("%s_processes_limit_pct %g\n", nameSpace, 0.0)
	}

	connections := listConnections()
	connections = strings.TrimRight(connections, "\n")
	connectionsList := strings.Split(connections,"\n")
	var cm map[string]*userConnInfo
	cm = make(map[string]*userConnInfo)
	for _, u := range g_users {
		uci := &userConnInfo{
			User:u,
			Blocked:0.0,
			Running:0.0,
			Closed:0.0,
		}
		cm[u] = uci
	}
	for _, c := range connectionsList {
		l := strings.Split(c, ",")
		if len(l) != 2 {
			continue
		}
		if _, ok := g_usersMap[l[0]]; ok {
			if strings.HasPrefix(l[1], "blocked") {
				cm[l[0]].Blocked += 1
			} else if strings.HasPrefix(l[1], "running") {
				cm[l[0]].Running += 1
			} else if strings.HasPrefix(l[1], "closed") {
				cm[l[0]].Closed += 1
			} else {

			}
		}
	}

	for _, val := range cm {
		ret += fmt.Sprintf("%s_connections{user=\"%s\",type=\"blocked\"} %g\n",
			nameSpace, val.User, val.Blocked)
		ret += fmt.Sprintf("%s_connections{user=\"%s\",type=\"running\"} %g\n",
			nameSpace, val.User, val.Running)
		ret += fmt.Sprintf("%s_connections{user=\"%s\",type=\"closed\"} %g\n",
			nameSpace, val.User, val.Closed)
	}

	var vm map[string]*vhostQueueInfo
	vm = make(map[string]*vhostQueueInfo)
	for _, v := range g_vhostsMap {
		vqi := &vhostQueueInfo{
			Vhost:v,
			UnAck:0.0,
			Ready:0.0,
		}
		vm[v] = vqi
	}
	for _, vhost := range g_vhosts {
		queues := listQueues("messages_ready", vhost)
		queues = strings.TrimRight(queues, "\n")
		queuesList := strings.Split(queues,"\n")
		for _, q := range queuesList {
			if strings.HasPrefix(q, "0") {
				continue
			}
			val, _ := strconv.ParseFloat(q, 64)
			vm[vhost].Ready += val
		}

		queues2 := listQueues("messages_unacknowledged", vhost)
		queues2 = strings.TrimRight(queues2, "\n")
		queues2List := strings.Split(queues2,"\n")
		for _, q := range queues2List {
			if strings.HasPrefix(q, "0") {
				continue
			}
			val, _ := strconv.ParseFloat(q, 64)
			vm[vhost].UnAck += val
		}
	}

	for _, val := range vm {
		ret += fmt.Sprintf("%s_queues_messages{vhost=\"%s\",type=\"ready\"} %g\n",
			nameSpace, val.Vhost, val.Ready)
		ret += fmt.Sprintf("%s_queues_messages{vhost=\"%s\",type=\"unacknowledged\"} %g\n",
			nameSpace, val.Vhost, val.UnAck)
	}

	g_lock.Lock()
	g_ret = ret
	g_lock.Unlock()
	doing = false
}

func metrics(w http.ResponseWriter, req *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func main() {
	flag.Parse()
	addr := ""
	if listenAddress != nil {
		addr = *listenAddress
	} else {
		addr = "/dev/shm/rabbitmq_exporter.sock"
	}

	if users == nil {
		panic("error users")
	}
	g_users = strings.Split(*users, ",")
    if len(g_users) == 0 {
    	panic("no users")
	}
	g_usersMap = make(map[string]string)
	for _, u := range g_users {
		g_usersMap[u] = u
	}

	if vhosts == nil {
		panic("error vhosts")
	}
	g_vhosts = strings.Split(*vhosts, ",")
    if len(g_vhosts) == 0 {
    	panic("no vhosts")
	}
	g_vhostsMap = make(map[string]string)
	for _, v := range g_vhosts {
		g_vhostsMap[v] = v
	}

	doing = false
	doWork()
	c := cron.New()
	c.AddFunc("0 */2 * * * ?", doWork)
	c.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Rabbitmq Exporter</title></head>
             <body>
             <h1>Rabbitmq Exporter</h1>
             <p><a href='` + "/metrics" + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	server := http.Server{
		Handler: mux, // http.DefaultServeMux,
	}
	os.Remove(addr)

	listener, err := net.Listen("unix", addr)
	if err != nil {
		panic(err)
	}
	server.Serve(listener)
}