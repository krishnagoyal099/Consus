package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ── ANSI Color Codes ──────────────────────────────────────────────
const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Italic    = "\033[3m"
	Underline = "\033[4m"

	Red     = "\033[38;5;203m"
	Green   = "\033[38;5;114m"
	Yellow  = "\033[38;5;221m"
	Blue    = "\033[38;5;111m"
	Magenta = "\033[38;5;176m"
	Cyan    = "\033[38;5;117m"
	White   = "\033[38;5;255m"
	Gray    = "\033[38;5;245m"
	Orange  = "\033[38;5;215m"

	BgDark = "\033[48;5;236m"
)

const version = "1.0.0"

func main() {
	addr := "http://localhost:8080"

	args := os.Args[1:]
	var filtered []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--addr" && i+1 < len(args) {
			addr = args[i+1]
			i++
		} else if strings.HasPrefix(args[i], "--addr=") {
			addr = strings.TrimPrefix(args[i], "--addr=")
		} else {
			filtered = append(filtered, args[i])
		}
	}
	args = filtered

	if len(args) == 0 {
		runREPL(addr)
		return
	}
	runCommand(addr, args)
}

// ── Interactive REPL ──────────────────────────────────────────────

func runREPL(addr string) {
	printBanner()

	// Quick connectivity check
	resp, err := http.Get(fmt.Sprintf("%s/api/ping", addr))
	if err != nil {
		fmt.Printf("  %s⚠  Cannot reach %s%s\n", Yellow, addr, Reset)
		fmt.Printf("  %sStart server first, then retry%s\n\n", Dim, Reset)
	} else {
		resp.Body.Close()
		fmt.Printf("  %s●%s Connected to %s%s%s\n\n", Green, Reset, Bold, addr, Reset)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%s%sconsus%s%s›%s ", Bold, Cyan, Reset, Gray, Reset)
		if !scanner.Scan() {
			fmt.Println()
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := tokenize(line)
		if len(parts) == 0 {
			continue
		}

		start := time.Now()
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "QUIT", "EXIT":
			fmt.Printf("\n  %s👋 Goodbye!%s\n\n", Dim, Reset)
			return
		case "HELP":
			printREPLHelp()
		case "CLEAR":
			fmt.Print("\033[2J\033[H")
			printBanner()
		case "PING":
			doPing(addr, start)
		case "SET":
			if len(parts) < 3 {
				printErr("wrong number of arguments for 'SET'")
				continue
			}
			doSet(addr, parts[1], parts[2], start)
		case "GET":
			if len(parts) < 2 {
				printErr("wrong number of arguments for 'GET'")
				continue
			}
			doGet(addr, parts[1], start)
		case "DEL", "DELETE":
			if len(parts) < 2 {
				printErr("wrong number of arguments for 'DEL'")
				continue
			}
			doDel(addr, parts[1], start)
		case "KEYS":
			pattern := "*"
			if len(parts) >= 2 {
				pattern = parts[1]
			}
			doKeys(addr, pattern, start)
		case "EXISTS":
			if len(parts) < 2 {
				printErr("wrong number of arguments for 'EXISTS'")
				continue
			}
			doExists(addr, parts[1], start)
		case "DBSIZE":
			doDBSize(addr, start)
		case "INFO":
			doInfo(addr)
		case "SETEX":
			if len(parts) < 4 {
				printErr("Usage: SETEX key seconds value")
				continue
			}
			doSetex(addr, parts[1], parts[2], parts[3], start)
		case "TTL":
			if len(parts) < 2 {
				printErr("wrong number of arguments for 'TTL'")
				continue
			}
			doTTL(addr, parts[1], start)
		case "CLUSTER":
			if len(parts) >= 2 && strings.ToUpper(parts[1]) == "STATUS" {
				doClusterStatus(addr)
			} else {
				printErr("Usage: CLUSTER STATUS")
			}
		default:
			printErr(fmt.Sprintf("unknown command '%s' — type HELP for commands", cmd))
		}
	}
}

// ── One-shot Command Mode ─────────────────────────────────────────

func runCommand(addr string, args []string) {
	start := time.Now()
	switch strings.ToLower(args[0]) {
	case "put", "set":
		if len(args) < 3 {
			fatalErr("Usage: consus set <key> <value>")
		}
		doSet(addr, args[1], args[2], start)
	case "get":
		if len(args) < 2 {
			fatalErr("Usage: consus get <key>")
		}
		doGet(addr, args[1], start)
	case "del", "delete":
		if len(args) < 2 {
			fatalErr("Usage: consus del <key>")
		}
		doDel(addr, args[1], start)
	case "keys":
		pattern := "*"
		if len(args) >= 2 {
			pattern = args[1]
		}
		doKeys(addr, pattern, start)
	case "exists":
		if len(args) < 2 {
			fatalErr("Usage: consus exists <key>")
		}
		doExists(addr, args[1], start)
	case "dbsize":
		doDBSize(addr, start)
	case "ping":
		doPing(addr, start)
	case "info":
		doInfo(addr)
	case "setex":
		if len(args) < 4 {
			fatalErr("Usage: consus setex <key> <seconds> <value>")
		}
		doSetex(addr, args[1], args[2], args[3], start)
	case "ttl":
		if len(args) < 2 {
			fatalErr("Usage: consus ttl <key>")
		}
		doTTL(addr, args[1], start)
	case "cluster":
		if len(args) >= 2 && args[1] == "status" {
			doClusterStatus(addr)
		} else {
			fatalErr("Usage: consus cluster status")
		}
	case "version", "--version", "-v":
		fmt.Printf("%sConsus%s v%s %s(Parallel Raft KV Store)%s\n", Bold, Reset, version, Dim, Reset)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "%s✗ Unknown command: %s%s\n\n", Red, args[0], Reset)
		printUsage()
		os.Exit(1)
	}
}

// ── Command Implementations ───────────────────────────────────────

func doPing(addr string, start time.Time) {
	resp, err := http.Get(fmt.Sprintf("%s/api/ping", addr))
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	lat := latency(start)
	fmt.Printf("%s%sPONG%s %s%s%s\n", Bold, Green, Reset, Dim, lat, Reset)
}

func doSet(addr, key, value string, start time.Time) {
	u := fmt.Sprintf("%s/api/put?key=%s&value=%s", addr,
		url.QueryEscape(key), url.QueryEscape(value))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		printErr(strings.TrimSpace(string(body)))
		return
	}
	lat := latency(start)
	fmt.Printf("%sOK%s %s%s%s\n", Green, Reset, Dim, lat, Reset)
}

func doGet(addr, key string, start time.Time) {
	u := fmt.Sprintf("%s/api/get?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	lat := latency(start)

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("%s(nil)%s %s%s%s\n", Dim, Reset, Dim, lat, Reset)
		return
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err == nil && result.Value != "" {
		fmt.Printf("%s\"%s\"%s %s%s%s\n", Yellow, result.Value, Reset, Dim, lat, Reset)
	} else {
		fmt.Printf("%s(nil)%s %s%s%s\n", Dim, Reset, Dim, lat, Reset)
	}
}

func doDel(addr, key string, start time.Time) {
	u := fmt.Sprintf("%s/api/delete?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	lat := latency(start)
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("%s(integer) 0%s %s%s%s\n", Red, Reset, Dim, lat, Reset)
		return
	}
	fmt.Printf("%s(integer) 1%s %s%s%s\n", Green, Reset, Dim, lat, Reset)
}

func doKeys(addr, pattern string, start time.Time) {
	u := fmt.Sprintf("%s/api/keys?pattern=%s", addr, url.QueryEscape(pattern))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	lat := latency(start)

	var result struct {
		Keys  []string `json:"keys"`
		Count int      `json:"count"`
	}
	json.Unmarshal(body, &result)

	if len(result.Keys) == 0 {
		fmt.Printf("%s(empty list)%s %s%s%s\n", Dim, Reset, Dim, lat, Reset)
		return
	}
	for i, k := range result.Keys {
		fmt.Printf("%s%d)%s %s\"%s\"%s\n", Dim, i+1, Reset, Yellow, k, Reset)
	}
	fmt.Printf("%s─── %d keys %s%s\n", Dim, len(result.Keys), lat, Reset)
}

func doExists(addr, key string, start time.Time) {
	u := fmt.Sprintf("%s/api/exists?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	lat := latency(start)

	var result struct {
		Exists int `json:"exists"`
	}
	json.Unmarshal(body, &result)
	color := Red
	if result.Exists == 1 {
		color = Green
	}
	fmt.Printf("%s(integer) %d%s %s%s%s\n", color, result.Exists, Reset, Dim, lat, Reset)
}

func doDBSize(addr string, start time.Time) {
	u := fmt.Sprintf("%s/api/dbsize", addr)
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	lat := latency(start)

	var result struct {
		Keys int `json:"keys"`
	}
	json.Unmarshal(body, &result)
	fmt.Printf("%s(integer) %d%s %s%s%s\n", Cyan, result.Keys, Reset, Dim, lat, Reset)
}

func doInfo(addr string) {
	u := fmt.Sprintf("%s/api/info", addr)
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var info map[string]interface{}
	json.Unmarshal(body, &info)

	sectionOrder := []string{"server", "memory", "keyspace", "cluster"}
	for _, section := range sectionOrder {
		data, ok := info[section]
		if !ok {
			continue
		}
		fmt.Printf("\n  %s%s# %s%s\n", Bold, Magenta, capitalize(section), Reset)
		if m, ok := data.(map[string]interface{}); ok {
			for k, v := range m {
				fmt.Printf("  %s%-14s%s %s%v%s\n", Gray, k+":", Reset, White, v, Reset)
			}
		}
	}
	fmt.Println()
}

func doSetex(addr, key, ttl, value string, start time.Time) {
	u := fmt.Sprintf("%s/api/setex?key=%s&value=%s&ttl=%s", addr,
		url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(ttl))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	lat := latency(start)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		printErr(strings.TrimSpace(string(body)))
		return
	}
	fmt.Printf("%sOK%s %s(expires in %ss) %s%s\n", Green, Reset, Dim, ttl, lat, Reset)
}

func doTTL(addr, key string, start time.Time) {
	u := fmt.Sprintf("%s/api/ttl?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		printErr(err.Error())
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	lat := latency(start)

	var result struct {
		TTL int64 `json:"ttl"`
	}
	json.Unmarshal(body, &result)

	switch {
	case result.TTL == -2:
		fmt.Printf("%s(integer) -2%s %s— key not found %s%s\n", Red, Reset, Dim, lat, Reset)
	case result.TTL == -1:
		fmt.Printf("%s(integer) -1%s %s— no expiry %s%s\n", Green, Reset, Dim, lat, Reset)
	default:
		fmt.Printf("%s(integer) %d%s %s— %ds remaining %s%s\n", Yellow, result.TTL, Reset, Dim, result.TTL, lat, Reset)
	}
}

func doClusterStatus(addr string) {
	u := fmt.Sprintf("%s/api/state", addr)
	resp, err := http.Get(u)
	if err != nil {
		printErr(fmt.Sprintf("cannot connect to %s: %v", addr, err))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var state struct {
		NodeID string `json:"nodeID"`
		Term   uint64 `json:"term"`
		Status string `json:"status"`
		Uptime string `json:"uptime"`
		Nodes  []struct {
			ID      string `json:"id"`
			State   string `json:"state"`
			IsSelf  bool   `json:"isSelf"`
			Address string `json:"address"`
			Shards  int    `json:"shards"`
		} `json:"nodes"`
		Shards []struct {
			ID uint64 `json:"id"`
		} `json:"shards"`
	}

	if err := json.Unmarshal(body, &state); err != nil {
		printErr(fmt.Sprintf("parse error: %v", err))
		return
	}

	fmt.Println()
	fmt.Printf("  %s%s╔══════════════════════════════════╗%s\n", Bold, Blue, Reset)
	fmt.Printf("  %s%s║%s  %sCluster Status%s                  %s%s║%s\n", Bold, Blue, Reset, Bold, Reset, Bold, Blue, Reset)
	fmt.Printf("  %s%s╚══════════════════════════════════╝%s\n", Bold, Blue, Reset)
	fmt.Println()

	statusColor := Green
	if strings.ToLower(state.Status) != "leader" {
		statusColor = Yellow
	}
	fmt.Printf("  %sRole%s      %s%s%s\n", Gray, Reset, statusColor, state.Status, Reset)
	fmt.Printf("  %sTerm%s      %s%d%s\n", Gray, Reset, White, state.Term, Reset)
	fmt.Printf("  %sUptime%s    %s%s%s\n", Gray, Reset, White, state.Uptime, Reset)
	fmt.Println()

	fmt.Printf("  %s%sNodes%s\n", Bold, Cyan, Reset)
	fmt.Printf("  %s─────────────────────────────────%s\n", Dim, Reset)
	for _, n := range state.Nodes {
		icon := "●"
		color := Green
		selfTag := ""
		if n.IsSelf {
			selfTag = fmt.Sprintf(" %s(you)%s", Dim, Reset)
		}
		addr := n.Address
		if addr == "" {
			addr = "unknown"
		}
		stateStr := strings.ToUpper(n.State)
		if stateStr == "LEADER" {
			icon = "★"
			color = Yellow
		}
		fmt.Printf("  %s%s%s %s%-8s%s %s%-10s%s %s%s%s%s\n",
			color, icon, Reset,
			White, n.ID, Reset,
			color, stateStr, Reset,
			Dim, addr, Reset,
			selfTag)
	}

	if len(state.Shards) > 0 {
		fmt.Printf("\n  %sShards%s    %s%d%s\n", Gray, Reset, Cyan, len(state.Shards), Reset)
	}
	fmt.Println()
}

// ── Visual Helpers ────────────────────────────────────────────────

func printBanner() {
	fmt.Println()
	fmt.Printf("  %s%s ██████╗ ██████╗ ███╗   ██╗███████╗██╗   ██╗███████╗%s\n", Bold, Cyan, Reset)
	fmt.Printf("  %s%s██╔════╝██╔═══██╗████╗  ██║██╔════╝██║   ██║██╔════╝%s\n", Bold, Cyan, Reset)
	fmt.Printf("  %s%s██║     ██║   ██║██╔██╗ ██║███████╗██║   ██║███████╗%s\n", Bold, Blue, Reset)
	fmt.Printf("  %s%s██║     ██║   ██║██║╚██╗██║╚════██║██║   ██║╚════██║%s\n", Bold, Blue, Reset)
	fmt.Printf("  %s%s╚██████╗╚██████╔╝██║ ╚████║███████║╚██████╔╝███████║%s\n", Bold, Magenta, Reset)
	fmt.Printf("  %s%s ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝ ╚═════╝ ╚══════╝%s\n", Bold, Magenta, Reset)
	fmt.Println()
	fmt.Printf("  %sParallel Raft KV Store%s  %sv%s%s\n", Dim, Reset, Dim, version, Reset)
	fmt.Printf("  %sType %sHELP%s%s for commands • %sQUIT%s%s to exit%s\n\n", Dim, White, Reset, Dim, White, Reset, Dim, Reset)
}

func printErr(msg string) {
	fmt.Printf("%s✗ %s%s\n", Red, msg, Reset)
}

func fatalErr(msg string) {
	fmt.Fprintf(os.Stderr, "%s✗ %s%s\n", Red, msg, Reset)
	os.Exit(1)
}

func latency(start time.Time) string {
	d := time.Since(start)
	if d < time.Millisecond {
		return fmt.Sprintf("(%dμs)", d.Microseconds())
	}
	return fmt.Sprintf("(%.1fms)", float64(d.Microseconds())/1000.0)
}

func printUsage() {
	fmt.Printf("\n  %s%sConsus CLI%s — Distributed KV Store\n\n", Bold, Cyan, Reset)
	fmt.Printf("  %sUsage:%s  consus [--addr=URL] [command] [args]\n\n", Bold, Reset)
	fmt.Printf("  %sNo command = interactive REPL mode%s\n\n", Dim, Reset)
	fmt.Printf("  %s%sCommands:%s\n", Bold, White, Reset)
	printCmdHelp("set", "<key> <value>", "Store a key-value pair")
	printCmdHelp("get", "<key>", "Retrieve a value")
	printCmdHelp("del", "<key>", "Delete a key")
	printCmdHelp("keys", "[pattern]", "List keys (default: *)")
	printCmdHelp("exists", "<key>", "Check if key exists")
	printCmdHelp("setex", "<key> <sec> <val>", "Set with TTL (seconds)")
	printCmdHelp("ttl", "<key>", "Get remaining TTL")
	printCmdHelp("dbsize", "", "Count keys")
	printCmdHelp("ping", "", "Health check")
	printCmdHelp("info", "", "Server info")
	printCmdHelp("cluster", "status", "Cluster health")
	printCmdHelp("version", "", "Show version")
	printCmdHelp("help", "", "This help")
	fmt.Printf("\n  %s%sOptions:%s\n", Bold, White, Reset)
	fmt.Printf("    %s--addr%s=URL    Server address %s(default: http://localhost:8080)%s\n\n", Green, Reset, Dim, Reset)
}

func printCmdHelp(cmd, args, desc string) {
	if args != "" {
		fmt.Printf("    %s%-8s%s %s%-20s%s %s%s%s\n", Green, cmd, Reset, White, args, Reset, Dim, desc, Reset)
	} else {
		fmt.Printf("    %s%-8s%s %s%-20s%s %s%s%s\n", Green, cmd, Reset, White, "", Reset, Dim, desc, Reset)
	}
}

func printREPLHelp() {
	fmt.Println()
	fmt.Printf("  %s%sCommands%s\n", Bold, Cyan, Reset)
	fmt.Printf("  %s─────────────────────────────────────────%s\n", Dim, Reset)
	printCmdHelp("SET", "key value", "store a value")
	printCmdHelp("GET", "key", "retrieve a value")
	printCmdHelp("DEL", "key", "delete a key")
	printCmdHelp("KEYS", "[pattern]", "list keys (* for all)")
	printCmdHelp("EXISTS", "key", "check existence (0/1)")
	printCmdHelp("SETEX", "key sec value", "set with expiry")
	printCmdHelp("TTL", "key", "remaining TTL (seconds)")
	printCmdHelp("DBSIZE", "", "count keys")
	printCmdHelp("PING", "", "health check")
	printCmdHelp("INFO", "", "server info")
	printCmdHelp("CLUSTER", "STATUS", "cluster health")
	printCmdHelp("CLEAR", "", "clear screen")
	printCmdHelp("QUIT", "", "exit")
	fmt.Println()
}

func tokenize(line string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(line); i++ {
		ch := line[i]
		if inQuote {
			if ch == quoteChar {
				inQuote = false
			} else {
				current.WriteByte(ch)
			}
		} else if ch == '"' || ch == '\'' {
			inQuote = true
			quoteChar = ch
		} else if ch == ' ' || ch == '\t' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
