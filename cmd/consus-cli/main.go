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

const version = "1.0.0"

func main() {
	addr := "http://localhost:8080"

	// Parse --addr flag from anywhere in args
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
		// Interactive REPL mode
		runREPL(addr)
		return
	}

	// One-shot command mode
	runCommand(addr, args)
}

func runREPL(addr string) {
	fmt.Println("Consus CLI v" + version + " — Parallel Raft KV Store")
	fmt.Println("Type 'help' for commands, 'quit' to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("consus> ")
		if !scanner.Scan() {
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

		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "QUIT", "EXIT":
			fmt.Println("Bye!")
			return
		case "HELP":
			printREPLHelp()
		case "PING":
			replPing(addr)
		case "SET":
			if len(parts) < 3 {
				fmt.Println("(error) wrong number of arguments for 'SET'")
				continue
			}
			replSet(addr, parts[1], parts[2])
		case "GET":
			if len(parts) < 2 {
				fmt.Println("(error) wrong number of arguments for 'GET'")
				continue
			}
			replGet(addr, parts[1])
		case "DEL", "DELETE":
			if len(parts) < 2 {
				fmt.Println("(error) wrong number of arguments for 'DEL'")
				continue
			}
			replDel(addr, parts[1])
		case "KEYS":
			pattern := "*"
			if len(parts) >= 2 {
				pattern = parts[1]
			}
			replKeys(addr, pattern)
		case "EXISTS":
			if len(parts) < 2 {
				fmt.Println("(error) wrong number of arguments for 'EXISTS'")
				continue
			}
			replExists(addr, parts[1])
		case "DBSIZE":
			replDBSize(addr)
		case "INFO":
			replInfo(addr)
		case "SETEX":
			if len(parts) < 4 {
				fmt.Println("(error) Usage: SETEX key seconds value")
				continue
			}
			replSetex(addr, parts[1], parts[2], parts[3])
		case "TTL":
			if len(parts) < 2 {
				fmt.Println("(error) wrong number of arguments for 'TTL'")
				continue
			}
			replTTL(addr, parts[1])
		case "CLUSTER":
			if len(parts) >= 2 && strings.ToUpper(parts[1]) == "STATUS" {
				cmdClusterStatus(addr)
			} else {
				fmt.Println("(error) Usage: CLUSTER STATUS")
			}
		default:
			fmt.Printf("(error) unknown command '%s'. Type 'help' for commands.\n", cmd)
		}
	}
}

func runCommand(addr string, args []string) {
	switch strings.ToLower(args[0]) {
	case "put", "set":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli set <key> <value>\n")
			os.Exit(1)
		}
		cmdPut(addr, args[1], args[2])
	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli get <key>\n")
			os.Exit(1)
		}
		cmdGet(addr, args[1])
	case "del", "delete":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli del <key>\n")
			os.Exit(1)
		}
		cmdDelete(addr, args[1])
	case "keys":
		pattern := "*"
		if len(args) >= 2 {
			pattern = args[1]
		}
		replKeys(addr, pattern)
	case "exists":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli exists <key>\n")
			os.Exit(1)
		}
		replExists(addr, args[1])
	case "dbsize":
		replDBSize(addr)
	case "ping":
		replPing(addr)
	case "info":
		replInfo(addr)
	case "setex":
		if len(args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli setex <key> <seconds> <value>\n")
			os.Exit(1)
		}
		replSetex(addr, args[1], args[2], args[3])
	case "ttl":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli ttl <key>\n")
			os.Exit(1)
		}
		replTTL(addr, args[1])
	case "cluster":
		if len(args) >= 2 && args[1] == "status" {
			cmdClusterStatus(addr)
		} else {
			fmt.Fprintf(os.Stderr, "Usage: consus-cli cluster status\n")
			os.Exit(1)
		}
	case "version":
		fmt.Printf("Consus v%s (Parallel Raft KV Store)\n", version)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

// --- REPL command implementations ---

func replPing(addr string) {
	resp, err := http.Get(fmt.Sprintf("%s/api/ping", addr))
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Println("PONG")
}

func replSet(addr, key, value string) {
	start := time.Now()
	u := fmt.Sprintf("%s/api/put?key=%s&value=%s", addr,
		url.QueryEscape(key), url.QueryEscape(value))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("(error) %s\n", strings.TrimSpace(string(body)))
		return
	}
	fmt.Printf("OK (%s)\n", time.Since(start).Round(100*time.Microsecond))
}

func replGet(addr, key string) {
	u := fmt.Sprintf("%s/api/get?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		fmt.Println("(nil)")
		return
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err == nil && result.Value != "" {
		fmt.Printf("\"%s\"\n", result.Value)
	} else {
		fmt.Println("(nil)")
	}
}

func replDel(addr, key string) {
	u := fmt.Sprintf("%s/api/delete?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Println("(integer) 0")
		return
	}
	fmt.Println("(integer) 1")
}

func replKeys(addr, pattern string) {
	u := fmt.Sprintf("%s/api/keys?pattern=%s", addr, url.QueryEscape(pattern))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Keys  []string `json:"keys"`
		Count int      `json:"count"`
	}
	json.Unmarshal(body, &result)

	if len(result.Keys) == 0 {
		fmt.Println("(empty list)")
		return
	}
	for i, k := range result.Keys {
		fmt.Printf("%d) \"%s\"\n", i+1, k)
	}
}

func replExists(addr, key string) {
	u := fmt.Sprintf("%s/api/exists?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Exists int `json:"exists"`
	}
	json.Unmarshal(body, &result)
	fmt.Printf("(integer) %d\n", result.Exists)
}

func replDBSize(addr string) {
	u := fmt.Sprintf("%s/api/dbsize", addr)
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Keys int `json:"keys"`
	}
	json.Unmarshal(body, &result)
	fmt.Printf("(integer) %d\n", result.Keys)
}

func replInfo(addr string) {
	u := fmt.Sprintf("%s/api/info", addr)
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var info map[string]interface{}
	json.Unmarshal(body, &info)

	// Print in Redis-style sections
	for section, data := range info {
		fmt.Printf("# %s\n", capitalize(section))
		if m, ok := data.(map[string]interface{}); ok {
			for k, v := range m {
				fmt.Printf("%s:%v\n", k, v)
			}
		}
		fmt.Println()
	}
}

func replSetex(addr, key, ttl, value string) {
	start := time.Now()
	u := fmt.Sprintf("%s/api/setex?key=%s&value=%s&ttl=%s", addr,
		url.QueryEscape(key), url.QueryEscape(value), url.QueryEscape(ttl))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("(error) %s\n", strings.TrimSpace(string(body)))
		return
	}
	fmt.Printf("OK (%s)\n", time.Since(start).Round(100*time.Microsecond))
}

func replTTL(addr, key string) {
	u := fmt.Sprintf("%s/api/ttl?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		TTL int64 `json:"ttl"`
	}
	json.Unmarshal(body, &result)
	fmt.Printf("(integer) %d\n", result.TTL)
}

// --- One-shot command implementations ---

func cmdPut(addr, key, value string) {
	start := time.Now()
	u := fmt.Sprintf("%s/api/put?key=%s&value=%s", addr,
		url.QueryEscape(key), url.QueryEscape(value))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Printf("\u2713 OK (latency: %s)\n", time.Since(start).Round(100*time.Microsecond))
}

func cmdGet(addr, key string) {
	u := fmt.Sprintf("%s/api/get?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err == nil && result.Value != "" {
		fmt.Println(result.Value)
	} else {
		fmt.Println(strings.TrimSpace(string(body)))
	}
}

func cmdDelete(addr, key string) {
	u := fmt.Sprintf("%s/api/delete?key=%s", addr, url.QueryEscape(key))
	resp, err := http.Get(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Println("\u2713 Deleted")
}

func cmdClusterStatus(addr string) {
	u := fmt.Sprintf("%s/api/state", addr)
	resp, err := http.Get(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: could not connect to %s: %v\n", addr, err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cluster: %s\n", strings.ToLower(state.Status))
	fmt.Printf("Leader:  %s (term %d)\n", state.NodeID, state.Term)
	fmt.Printf("Uptime:  %s\n", state.Uptime)
	fmt.Println()

	fmt.Println("Nodes:")
	for _, n := range state.Nodes {
		selfMark := ""
		if n.IsSelf {
			selfMark = " (you)"
		}
		a := n.Address
		if a == "" {
			a = "unknown"
		}
		fmt.Printf("  %-8s %-10s (%s) [UP]%s\n", n.ID+":", strings.ToUpper(n.State), a, selfMark)
	}

	if len(state.Shards) > 0 {
		fmt.Printf("\nShards: %d total\n", len(state.Shards))
	}
}

// --- Helpers ---

func printUsage() {
	fmt.Println("Consus CLI — Distributed KV Store Client")
	fmt.Println()
	fmt.Println("Usage: consus-cli [--addr=http://host:port] [command] [args]")
	fmt.Println()
	fmt.Println("No command = interactive REPL mode")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  set <key> <value>         Store a key-value pair")
	fmt.Println("  get <key>                 Retrieve a value")
	fmt.Println("  del <key>                 Delete a key")
	fmt.Println("  keys [pattern]            List keys (default: *)")
	fmt.Println("  exists <key>              Check if key exists")
	fmt.Println("  setex <key> <sec> <val>   Set with TTL")
	fmt.Println("  ttl <key>                 Get remaining TTL")
	fmt.Println("  dbsize                    Count keys")
	fmt.Println("  ping                      Health check")
	fmt.Println("  info                      Server info")
	fmt.Println("  cluster status            Cluster health")
	fmt.Println("  version                   Show version")
	fmt.Println("  help                      This help")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --addr=URL    Server address (default: http://localhost:8080)")
}

func printREPLHelp() {
	fmt.Println("Commands:")
	fmt.Println("  SET key value        — store a value")
	fmt.Println("  GET key              — retrieve a value")
	fmt.Println("  DEL key              — delete a key")
	fmt.Println("  KEYS [pattern]       — list keys (use * for all)")
	fmt.Println("  EXISTS key           — check if key exists (0/1)")
	fmt.Println("  SETEX key sec value  — set with expiry")
	fmt.Println("  TTL key              — remaining TTL in seconds")
	fmt.Println("  DBSIZE               — count keys")
	fmt.Println("  PING                 — health check")
	fmt.Println("  INFO                 — server info")
	fmt.Println("  CLUSTER STATUS       — cluster health")
	fmt.Println("  QUIT                 — exit")
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
